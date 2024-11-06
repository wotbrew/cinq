(ns com.wotbrew.cinq.lmdb-kv
  (:require [com.wotbrew.cinq.nio-codec :as codec])
  (:import (com.wotbrew.cinq CinqUtil)
           (java.nio ByteBuffer)
           (java.util ArrayList)
           (org.lmdbjava Cursor GetOp PutFlags)))

;; 1 dbi
;; 'index' prefix on keys
;; index is a keyword or other interned object (symbol-table) guaranteed to fit in the key.
;; no DUPSORT otherwise vals have to have a max-key size
;; on collision we write the whole val by comparing as we go and inserting once we are binary < or = to the existing key.
;; layout will be optimised for a simple linear loop rather than binary search (small number of keys under collision, usually 1).
;; this is because long key collisions are gonna be, you know "a bad thing".

(def ^:private ^"[Lorg.lmdbjava.PutFlags;" insert-flags
  (into-array PutFlags []))

(defn- throw-on-err [err]
  (when err (throw (ex-info "Not enough space in write-buffer during insert" {:cinq/error err}))))

(defn- encode-key [^ByteBuffer key-buffer ^ByteBuffer val-buffer symbol-table index key]
  (.clear val-buffer)
  (.clear key-buffer)
  (codec/encode-object index val-buffer symbol-table true)  ; has to be symbolic, guaranteed fit
  (throw-on-err (codec/encode-object key val-buffer nil false))
  (.flip val-buffer)
  (.limit val-buffer (min (.limit val-buffer) (.capacity key-buffer)))
  (.put key-buffer val-buffer)
  (.clear val-buffer)
  (codec/mark-if-key-truncated key-buffer))

(defn- ensure-remaining [^ByteBuffer buffer ^long n]
  (when (< (.remaining buffer) n) (throw-on-err ::codec/not-enough-space)))

(defn- ensure-collision [^ByteBuffer buffer symbol-list]
  (let [collision (codec/decode-object buffer symbol-list)]
    (when-not (identical? :cinq/collision collision) (throw (ex-info "Unexpected collision prefix" {:prefix collision})))))

(defn- collision-new
  [^ByteBuffer val-buffer
   symbol-table
   key
   val]
  (.clear val-buffer)
  ;; version prefix
  (codec/encode-object :cinq/collision val-buffer symbol-table true)

  (let [min-len (+ 4 8 32)]
    (ensure-remaining val-buffer min-len))

  (.putInt val-buffer 1)

  (let [len-pos (.position val-buffer)
        ;; skip to key/val
        _ (.position val-buffer (+ 8 len-pos))
        pos (.position val-buffer)
        _ (throw-on-err (codec/encode-object key val-buffer nil false))
        key-len (- (.position val-buffer) pos)
        _ (throw-on-err (codec/encode-object val val-buffer symbol-table true))
        val-len (- (.position val-buffer) pos key-len)]
    (.putInt val-buffer len-pos key-len)
    (.putInt val-buffer (+ 4 len-pos) val-len))
  (.flip val-buffer))

(defn- collision-append
  [^ByteBuffer cursor-val-buffer
   ^ByteBuffer val-buffer
   symbol-table
   key
   val]
  (.clear val-buffer)
  (let [symbol-list (codec/symbol-list symbol-table)
        _ (ensure-collision cursor-val-buffer symbol-list)
        n-pairs (.getInt cursor-val-buffer)

        _ (.clear val-buffer)
        _ (codec/encode-object key val-buffer symbol-table true)
        swap-buffer (.flip (.duplicate val-buffer))

        _ (throw-on-err (codec/encode-object :cinq/collision val-buffer symbol-table true))
        _ (ensure-remaining val-buffer (+ 32 12))
        _ (.putInt val-buffer (inc n-pairs))

        olimit (.limit cursor-val-buffer)

        written (boolean-array 1)
        write-new (fn []
                    (let [vpos (.position val-buffer)
                          _ (ensure-remaining val-buffer (+ 32 8))
                          _ (.position val-buffer (+ vpos 8))
                          start-pos (+ vpos 8)
                          _ (throw-on-err (codec/encode-object key val-buffer nil false))
                          new-key-len (- (.position val-buffer) start-pos)
                          _ (throw-on-err (codec/encode-object val val-buffer symbol-table true))
                          new-val-len (- (.position val-buffer) start-pos new-key-len)
                          _ (.putInt val-buffer vpos new-key-len)
                          _ (.putInt val-buffer (+ 4 vpos) new-val-len)]
                      (aset written 0 true)))
        _
        (dotimes [_ n-pairs]
          (let [key-len (.getInt cursor-val-buffer)
                val-len (.getInt cursor-val-buffer)
                pos (.position cursor-val-buffer)]
            (.limit cursor-val-buffer (+ pos key-len))

            (let [collided (if (aget written 0)
                             false
                             (let [cmp (.compareTo swap-buffer cursor-val-buffer)]
                               (cond
                                 (< cmp 0)
                                 (do (write-new) false)

                                 (= cmp 0)
                                 (do (write-new) true))))]
              (.limit cursor-val-buffer olimit)
              (if collided
                (do (.position cursor-val-buffer (+ pos key-len val-len)))
                (do (ensure-remaining val-buffer (* 8 32))
                    (.putInt val-buffer key-len)
                    (.putInt val-buffer val-len)
                    (.limit cursor-val-buffer (+ pos key-len val-len))
                    (.put val-buffer cursor-val-buffer)
                    (.limit cursor-val-buffer olimit))))))]

    (when-not (aget written 0)
      (write-new))

    (.flip val-buffer)
    (.position val-buffer (.limit swap-buffer))))

(defn- collision-delete [^ByteBuffer cursor-buffer ^ByteBuffer val-buffer symbol-table key]
  (let [symbol-list (codec/symbol-list symbol-table)
        _ (ensure-collision cursor-buffer symbol-list)
        olimit (.limit cursor-buffer)
        n-pairs (.getInt cursor-buffer)
        _ (.clear val-buffer)
        _ (throw-on-err (codec/encode-object key val-buffer nil false))
        swap-buffer (.flip (.duplicate val-buffer))
        _ (throw-on-err (codec/encode-object :cinq/collision val-buffer symbol-table true))
        new-n-pairs-pos (.position val-buffer)
        _ (.position val-buffer (+ 4 (.position val-buffer)))]
    (loop [i 0]
      (if (= i n-pairs)
        (do
          (.flip val-buffer)
          (.position val-buffer (.limit swap-buffer))
          ::not-found)
        (let [key-len (.getInt cursor-buffer)
              val-len (.getInt cursor-buffer)
              _ (.limit cursor-buffer (+ (.position cursor-buffer) key-len))
              eq (= cursor-buffer swap-buffer)
              _ (.limit cursor-buffer olimit)]
          (if eq
            (if (= 1 n-pairs)
              ::found-clear
              (do (.position cursor-buffer (+ (.position cursor-buffer) key-len val-len))
                  (ensure-remaining val-buffer (.remaining cursor-buffer))
                  (.put val-buffer cursor-buffer)
                  (.putInt val-buffer new-n-pairs-pos (dec n-pairs))
                  (.flip val-buffer)
                  (.position val-buffer (.limit swap-buffer))
                  ::found))
            (do (ensure-remaining val-buffer 32)
                (.putInt val-buffer key-len)
                (.putInt val-buffer val-len)
                (.limit cursor-buffer (+ (.position cursor-buffer) key-len val-len))
                (.put val-buffer cursor-buffer)
                (.limit cursor-buffer olimit)
                (recur (unchecked-inc i)))))))))

(defn put [^Cursor cursor
           ^ByteBuffer key-buffer
           ^ByteBuffer val-buffer
           symbol-table
           index
           key
           val]
  (if (encode-key key-buffer val-buffer symbol-table index key)
    (if (.get cursor (.flip key-buffer) GetOp/MDB_SET)
      (collision-append (.val cursor) val-buffer symbol-table key val)
      (collision-new val-buffer symbol-table key val))
    (do
      (.flip key-buffer)
      (throw-on-err (codec/encode-object val val-buffer symbol-table true))
      (.flip val-buffer)))
  (.put cursor key-buffer val-buffer insert-flags))

(defn del [^Cursor cursor
           ^ByteBuffer key-buffer
           ^ByteBuffer val-buffer
           symbol-table
           index
           key]
  (let [truncated (encode-key key-buffer val-buffer symbol-table index key)]

    (cond
      (not (.get cursor (.flip key-buffer) GetOp/MDB_SET_KEY)) ::no-record

      truncated
      (case (collision-delete (.val cursor) val-buffer symbol-table key)
        ::not-found ::no-record
        ::found (.put cursor (.key cursor) val-buffer insert-flags)
        ::found-clear (.delete cursor insert-flags))

      :else
      (do
        (.delete cursor insert-flags)
        nil))))

(defn- walk-collision [^ByteBuffer val-buf symbol-list n-pairs index f init fwd]
  (if fwd
    (loop [acc init
           i 0]
      (if (= i n-pairs)
        acc
        (let [_ (.position val-buf (+ 8 (.position val-buf)))
              key (codec/decode-object val-buf symbol-list)
              val (codec/decode-object val-buf symbol-list)
              ret (f acc index key val)]
          (if (reduced? ret)
            ret
            (recur ret (unchecked-inc i))))))
    ;; todo, tune this if it matters - slow path is probably ok
    (let [d (walk-collision val-buf symbol-list n-pairs index (fn [acc i k v] (conj acc [i k v])) [] true)]
      (loop [acc init
             i (count d)]
        (if (= i 0)
          acc
          (let [[index key val] (nth d i)
                ret (f acc index key val)]
            (if (reduced? ret)
              ret
              (recur ret (unchecked-dec i)))))))))


(defn native-pred [filter symbol-table]
  (let [[op & args] filter
        symbol-list (codec/symbol-list symbol-table)]
    (case (nth filter 0)
      :=
      (do (assert (= 1 (count args)))
          (let [[x] args
                buf (codec/encode-heap x symbol-table false)]
            #(= 0 (codec/compare-bufs buf % symbol-list))))
      (:< :<= :> :>=)
      (do (assert (= 1 (count args)))
          (let [[x] args
                buf (codec/encode-heap x symbol-table false)]
            (case op
              :< #(CinqUtil/lt (codec/compare-bufs buf ^ByteBuffer % symbol-list) (long 0))
              :<= #(CinqUtil/lte (codec/compare-bufs buf ^ByteBuffer % symbol-list) (long 0))
              :> #(CinqUtil/gt (codec/compare-bufs buf ^ByteBuffer % symbol-list) (long 0))
              :>= #(CinqUtil/gte (codec/compare-bufs buf ^ByteBuffer % symbol-list) (long 0)))))
      :and (apply every-pred (map #(native-pred % symbol-table) args))
      :or (apply some-fn (map #(native-pred % symbol-table) args)))))

;; each of these can take key theta or val theta
;; lookup e.g a = 42
;; prefix (composite) e.g a = 42, free b
;; prefix-range (composite) a = 42, b < 32
;; range e.g a = 42

;; [:= 42]
;; [:in [42, 43, 44]]

;; key filtering based on buf-cmp-ksv
;; can support any native filter by instead of comparing, return a bounded buffer for the val at key.
;; [:get :a [:= 42]]
;; [:get :a [:< 43]]
;; [:get :a [:> 42]]

;; buffer filter vtables (hit over and over during scan, but then, so is (f))

(defn scan-all [^Cursor cursor symbol-table f init fwd]
  (let [symbol-list (codec/symbol-list symbol-table)]
    (loop [acc init
           hit (if fwd (.first cursor) (.last cursor))]
      (if-not hit
        acc
        (if (codec/truncated? (.key cursor))
          (let [index (codec/decode-object (.key cursor) symbol-list)
                ^ByteBuffer val-buf (.val cursor)
                _ (ensure-collision val-buf symbol-list)
                n-pairs (.getInt val-buf)
                ret (walk-collision val-buf symbol-list n-pairs index f init fwd)]
            (if (reduced? ret)
              @ret
              (recur ret (.next cursor))))
          (let [index (codec/decode-object (.key cursor) symbol-list)
                key (codec/decode-object (.key cursor) symbol-list)
                val (codec/decode-object (.val cursor) symbol-list)
                ret (f acc index key val)]
            (if (reduced? ret)
              ret
              (recur ret (if fwd (.next cursor) (.prev cursor))))))))))

(defrecord Entry [index key val])

(defn materialize-all [^Cursor cursor symbol-table]
  (persistent!
    (scan-all cursor
              symbol-table
              (fn [acc index key val] (conj! acc (->Entry index key val)))
              (transient [])
              true)))

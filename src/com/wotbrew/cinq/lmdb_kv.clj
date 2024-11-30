(ns com.wotbrew.cinq.lmdb-kv
  (:require [com.wotbrew.cinq.nio-codec :as codec])
  (:import (java.nio ByteBuffer)
           (org.lmdbjava Cursor GetOp PutFlags)))

(def ^:private ^"[Lorg.lmdbjava.PutFlags;" put-flags
  (into-array PutFlags []))

(defn find-collision-pos ^long [^ByteBuffer collision-node ^ByteBuffer full-key]
  (let [opos (.position collision-node)
        olimit (.limit collision-node)
        size (.getInt collision-node)
        ret
        (loop [i 0]
          (if (= i size)
            -1
            (let [key-pos (.position collision-node)
                  key-size (codec/next-object-size collision-node)
                  _ (.limit collision-node (+ key-pos key-size))
                  cmp-res (codec/compare-lex-bufs full-key collision-node)]
              (cond
                (= 0 cmp-res)
                key-pos

                (> 0 cmp-res)
                -1

                :else
                (do (.limit collision-node olimit)
                    (codec/skip-object collision-node)
                    (codec/skip-object collision-node)
                    (recur (unchecked-inc i)))))))]
    (.limit collision-node olimit)
    (.position collision-node opos)
    ret))

(defn- copy [^ByteBuffer buf]
  (let [ret (ByteBuffer/allocate (.remaining buf))
        _ (.put ret buf)]
    (.flip ret)))

(defn- put-raw-on-collision [^Cursor cursor ^ByteBuffer lmdb-key ^ByteBuffer full-key ^ByteBuffer val return-existing]
  (let [^ByteBuffer existing-val (.val cursor)
        ;; collision append
        opos (.position existing-val)
        collision-pos (find-collision-pos existing-val full-key)
        collision-size (if (= -1 collision-pos)
                         0
                         (do (.position existing-val collision-pos)
                             (let [s (codec/next-pair-size existing-val)]
                               (.position existing-val opos)
                               s)))
        new-size (+ (.remaining existing-val)
                    (- collision-size)
                    (.remaining full-key)
                    (.remaining val))
        existing-count (.getInt existing-val)
        ;; have to consider passing buffer to avoid alloc
        ;; heap allocation though, and only matters for long-keys. Do we care?
        tmp-buffer (ByteBuffer/allocate (.remaining existing-val))
        _ (.put tmp-buffer (.duplicate existing-val))
        existing-val (.flip tmp-buffer)
        ;; this reuses the same memory as the existing key
        ^ByteBuffer new-buf (.reserve cursor lmdb-key new-size put-flags)]
    (.putInt new-buf (if (= -1 collision-pos) (unchecked-inc existing-count) existing-count))
    ;; idea: could not loop if collision-pos != nil and instead write part-a new-pair part-b using pos/size.
    ;; collision only though, do we care?
    (loop [i 0]
      (if (= i existing-count)
        (do
          (.put new-buf full-key)
          (.put new-buf val)
          nil)
        (let [opos (.position existing-val)
              olimit (.limit existing-val)
              key-size (codec/next-object-size existing-val)
              _ (.limit existing-val (+ opos key-size))
              cmp-ret (codec/compare-lex-bufs full-key existing-val)
              _ (.limit existing-val olimit)]
          (if (and cmp-ret (<= cmp-ret 0))
            (do (.put new-buf full-key)
                (.put new-buf val)

                (.limit existing-val olimit)
                (.position existing-val opos)

                (if (= 0 cmp-ret)
                  (do (codec/skip-object existing-val)
                      (let [next-size (codec/next-object-size existing-val)
                            olimit (.limit existing-val)
                            _ (.limit existing-val (+ (.position existing-val) next-size))
                            ret (if return-existing (copy existing-val) (.position existing-val (+ (.position existing-val) next-size)))
                            _ (.limit existing-val olimit)]
                        (.put new-buf existing-val)
                        ret))
                  (do (.put new-buf existing-val)
                      nil)))
            (do (.position existing-val opos)
                (.limit existing-val (+ opos (codec/next-pair-size existing-val)))
                (.put new-buf existing-val)
                (.limit existing-val olimit)
                (recur (unchecked-inc i)))))))))

;; todo allow overriding collision behaviour (e.g throw)
(defn put-raw ^ByteBuffer [^Cursor cursor ^ByteBuffer lmdb-key ^ByteBuffer full-key ^ByteBuffer val return-existing]
  (if-not full-key
    (if (.get cursor lmdb-key GetOp/MDB_SET)                ; exists
      (let [^ByteBuffer val-buf (.val cursor)
            ret (when return-existing (copy val-buf))]
        (.put cursor lmdb-key val put-flags)
        ret)
      (do
        (.put cursor lmdb-key val put-flags)
        nil))
    (if (.get cursor lmdb-key GetOp/MDB_SET)
      (put-raw-on-collision cursor lmdb-key full-key val return-existing)
      (do
        (let [new-size (+ 4
                          (.remaining full-key)
                          (.remaining val))
              ^ByteBuffer new-buf (.reserve cursor lmdb-key new-size put-flags)]
          (.putInt new-buf 1)
          (.put new-buf full-key)
          (.put new-buf val)
          nil)))))

(defn del-raw [^Cursor cursor ^ByteBuffer lmdb-key ^ByteBuffer full-key return-existing]
  (cond
    (not (.get cursor lmdb-key GetOp/MDB_SET)) nil

    (not full-key)
    (let [ret (copy (.val cursor))]
      (.delete cursor put-flags) ret)

    :else
    (let [^ByteBuffer existing-val (.val cursor)
          ;; heap allocation, but already on a slow path. See note in put-raw
          tmp (ByteBuffer/allocate (.remaining existing-val))
          _ (.put tmp existing-val)
          existing-val (.flip tmp)
          collision-pos (find-collision-pos existing-val full-key)]
      (when-not (= -1 collision-pos)
        (let [n (.getInt existing-val)]
          (if (= 1 n)
            (if return-existing
              (let [_ (codec/skip-object existing-val)
                    ret (copy existing-val)]
                (.delete cursor put-flags)
                ret)
              (do (.delete cursor put-flags) nil))
            (let [olimit (.limit existing-val)
                  opos (.position existing-val)
                  _ (.position existing-val collision-pos)
                  hit-size (codec/next-pair-size existing-val)
                  _ (.limit existing-val (+ collision-pos hit-size))
                  ret (when return-existing (codec/skip-object existing-val) (copy existing-val))
                  _ (.limit existing-val olimit)
                  _ (.position existing-val opos)
                  existing-val (.duplicate existing-val)
                  ^ByteBuffer new-buf (.reserve cursor lmdb-key (+ 4 (- (.remaining existing-val) hit-size)) put-flags)
                  _ (.putInt new-buf (unchecked-dec-int n))
                  _ (.limit existing-val collision-pos)
                  _ (.put new-buf existing-val)
                  _ (.position existing-val collision-pos)
                  _ (.limit existing-val olimit)
                  _ (codec/skip-object existing-val)
                  _ (codec/skip-object existing-val)
                  _ (.put new-buf existing-val)]
              ret)))))))

(defn- throw-on-err [err]
  (when err (throw (ex-info "Not enough space in write-buffer during insert" {:cinq/error err}))))

(defn- encode-key [^ByteBuffer lmdb-key-buffer ^ByteBuffer write-buffer symbol-table index key]
  (.clear write-buffer)
  (.clear lmdb-key-buffer)
  (codec/encode-interned index write-buffer symbol-table true) ; has to be symbolic, guaranteed fit
  (throw-on-err (codec/encode-object key write-buffer nil false))
  (let [opos (.position write-buffer)]
    (.flip write-buffer)
    (.limit write-buffer (min (.limit write-buffer) (.remaining lmdb-key-buffer)))
    (.put lmdb-key-buffer write-buffer)
    (.limit write-buffer (.capacity write-buffer))
    (.position write-buffer opos)
    (codec/mark-if-key-truncated lmdb-key-buffer)))

(defn put [^Cursor cursor
           ^ByteBuffer lmdb-key-buffer
           ^ByteBuffer write-buffer
           symbol-table
           index
           key
           val
           return-existing]
  (when (nil? key) (throw (ex-info "Key cannot be nil" {:val val})))
  (if (encode-key lmdb-key-buffer write-buffer symbol-table index key)
    (let [key-buffer (.flip (.duplicate write-buffer))
          _ (codec/skip-object key-buffer)                  ; skip index
          key-end (.position write-buffer)
          _ (when-not (codec/buffer-min-remaining? write-buffer) (throw-on-err ::codec/not-enough-space))
          _ (throw-on-err (codec/encode-object val write-buffer symbol-table true))
          _ (.flip write-buffer)
          _ (.position write-buffer key-end)
          val-buffer write-buffer]
      (put-raw cursor (.flip lmdb-key-buffer) key-buffer val-buffer return-existing))
    (do (.clear write-buffer)
        (throw-on-err (codec/encode-object val write-buffer symbol-table true))
        (put-raw cursor (.flip lmdb-key-buffer) nil (.flip write-buffer) return-existing))))

(defn del [^Cursor cursor
           ^ByteBuffer lmdb-key-buffer
           ^ByteBuffer write-buffer
           symbol-table
           index
           key
           return-existing]
  (when (nil? key) (throw (ex-info "Key cannot be nil" {:val val})))
  (if (encode-key lmdb-key-buffer write-buffer symbol-table index key)
    (do
      (.flip write-buffer)
      (codec/skip-object write-buffer)
      (del-raw cursor (.flip lmdb-key-buffer) write-buffer return-existing))
    (del-raw cursor (.flip lmdb-key-buffer) nil return-existing)))

(defn- walk-collision [^ByteBuffer val-buf symbol-list n-pairs index f init fwd]
  (if fwd
    (loop [acc init
           i 0]
      (if (= i n-pairs)
        acc
        (let [kb (.duplicate val-buf)
              _ (codec/skip-object val-buf)
              vb (.duplicate val-buf)
              _ (codec/skip-object val-buf)
              ret (f acc index kb vb)]
          (if (reduced? ret)
            ret
            (recur ret (unchecked-inc i))))))
    ;; todo, tune this if it matters - slow path is probably ok
    (let [d (walk-collision val-buf symbol-list n-pairs index (fn [acc _i k v] (conj acc [k v])) [] true)]
      (loop [acc init
             i (count d)]
        (if (= i 0)
          acc
          (let [[key val] (nth d i)
                ret (f acc index key val)]
            (if (reduced? ret)
              ret
              (recur ret (unchecked-dec i)))))))))

(defn scan [^Cursor cursor symbol-table f init ^ByteBuffer start fwd]
  (let [symbol-list (codec/symbol-list symbol-table)]
    (loop [acc init
           hit (if start (.get cursor start GetOp/MDB_SET_RANGE) (if fwd (.first cursor) (.last cursor)))]
      ;; todo interrupt
      (if-not hit
        acc
        (if (codec/truncated? (.key cursor))
          (let [index (codec/decode-object (.key cursor) symbol-list)
                ^ByteBuffer val-buf (.val cursor)
                n-pairs (.getInt val-buf)
                ;; copy is required to ensure memory remains valid for the entire walk
                ;; (e.g you might delete or replace the entry in (f))
                ret (walk-collision (copy val-buf) symbol-list n-pairs index f init fwd)]
            (if (reduced? ret)
              @ret
              (recur ret (if fwd (.next cursor) (.prev cursor)))))
          (let [index (codec/decode-object (.key cursor) symbol-list)
                ret (f acc index (.key cursor) (.val cursor))]
            (if (reduced? ret)
              ret
              (recur ret (if fwd (.next cursor) (.prev cursor))))))))))

(defn drop-index [^Cursor cursor ^ByteBuffer lmdb-key-buffer symbol-table index]
  (let [_ (.clear lmdb-key-buffer)
        _ (codec/encode-interned index lmdb-key-buffer symbol-table false)
        sl (codec/symbol-list symbol-table)]
    (when (.get cursor lmdb-key-buffer GetOp/MDB_SET_RANGE)
      (loop []
        ;; todo interrupt
        (when (identical? index (codec/decode-object (.key cursor) sl))
          (.delete cursor put-flags)
          (when (.next cursor)
            (recur)))))))

(defrecord Entry [index key val])

(defn materialize-all [^Cursor cursor symbol-table]
  (let [symbol-list (codec/symbol-list symbol-table)]
    (persistent!
      (scan cursor
            symbol-table
            (fn [acc index key val] (conj! acc (->Entry index (codec/decode-object key symbol-list) (codec/decode-object val symbol-list))))
            (transient [])
            nil
            true))))

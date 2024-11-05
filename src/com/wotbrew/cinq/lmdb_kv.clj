(ns com.wotbrew.cinq.lmdb-kv
  (:require [com.wotbrew.cinq.nio-codec :as codec])
  (:import (java.nio ByteBuffer)
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
  (throw-on-err (codec/encode-object index val-buffer symbol-table true))
  (throw-on-err (codec/encode-object key val-buffer nil false))
  (.flip val-buffer)
  (.limit val-buffer (min (.limit val-buffer) (.capacity key-buffer)))
  (.put key-buffer val-buffer)
  (.clear val-buffer)
  (codec/mark-if-key-truncated key-buffer))

(defn- ensure-remaining [^ByteBuffer buffer ^long n]
  (when (< (.remaining buffer) n) (throw-on-err ::codec/not-enough-space)))

(defn- collision-new
  [^ByteBuffer val-buffer
   symbol-table
   key
   val]
  (.clear val-buffer)
  ;; version prefix
  (throw-on-err (codec/encode-object ::collision val-buffer symbol-table true))

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
        collision (codec/decode-object cursor-val-buffer symbol-list)
        _ (when-not (identical? ::collision collision) (throw (ex-info "Unexpected collision prefix" {:prefix collision})))
        n-pairs (.getInt cursor-val-buffer)

        _ (.clear val-buffer)
        _ (codec/encode-object key val-buffer symbol-table true)
        swap-buffer (.flip (.duplicate val-buffer))

        _ (throw-on-err (codec/encode-object ::collision val-buffer symbol-table true))
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

(defn- collision-delete []

  ;; return number of remaining entries
  )

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
      ;; todo delete behaviour
      (do
        (loop []
          (let [^ByteBuffer v (.val cursor)
                k (codec/decode-object v (codec/symbol-list symbol-table))]
            (if (= k key)
              (.delete cursor insert-flags)
              (when (and (.next cursor) (= key-buffer (.key cursor)))
                (recur)))))
        nil)

      :else
      (do
        (.delete cursor insert-flags)
        nil))))

(defrecord Entry [index key val])

(defn- materialize-entries [^Cursor cursor symbol-list]
  (if (codec/truncated? (.key cursor))
    (let [index (codec/decode-object (.key cursor) symbol-list)
          ^ByteBuffer val-buf (.val cursor)
          collision (codec/decode-object val-buf symbol-list)
          _ (when-not (identical? ::collision collision) (throw (ex-info "Unexpected collision prefix" {:prefix collision})))
          n-pairs (.getInt val-buf)
          al (ArrayList.)]
      (dotimes [_ n-pairs]
        (let [_ (.position val-buf (+ 8 (.position val-buf)))
              key (codec/decode-object val-buf symbol-list)
              val (codec/decode-object val-buf symbol-list)]
          (.add al (->Entry index key val))))
      al)
    (let [index (codec/decode-object (.key cursor) symbol-list)
          key (codec/decode-object (.key cursor) symbol-list)
          val (codec/decode-object (.val cursor) symbol-list)]
      [(->Entry index key val)])))

(defn materialize-all [^Cursor cursor symbol-table]
  (let [sl (codec/symbol-list symbol-table)]
    (if (.first cursor)
      (let [al (ArrayList.)]
        (.addAll al (materialize-entries cursor sl))
        (while (.next cursor)
          (.addAll al (materialize-entries cursor sl)))
        (vec al))
      [])))

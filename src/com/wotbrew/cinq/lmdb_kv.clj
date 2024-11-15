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

;; secondary vs primary
;; a secondary index includes the val (primary) with the key. This is because a value can map to multiple primary keys
;; we can then use the same scan algs

;; TODO refactor the walks, put/del in terms of buffer inputs.
;; avoid code/decode here other than checking for truncation
;; scan works in terms of raw bufs:
;; (scan-forward index lmdb-key f init)
;; (scan-backward index lmdb-key f init)

;; (put index lmdb-key full-key value truncated) (on collision, reserve the new buffer (space requirements can be calculated) and sort)
;; (del index lmdb-key full-key)

;; RANDOM: map-buf sort will be needed for start-key lookups to work
;; RANDOM: should we change our mind on nil? how does nil behave in scans, can it be different
;; from top-level cinq, or should a different poison value be preferred?!,

;; (= 42 a) (clojure semantics)

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

(defn put-raw [^Cursor cursor ^ByteBuffer lmdb-key ^ByteBuffer full-key ^ByteBuffer val]
  (if-not full-key
    (.put cursor lmdb-key val put-flags)
    (if (.get cursor lmdb-key GetOp/MDB_SET)
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
              (.put new-buf val))
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

                    (when (= 0 cmp-ret)
                      (codec/skip-object existing-val)
                      (codec/skip-object existing-val))

                    (.put new-buf existing-val))
                (do (.position existing-val opos)
                    (.limit existing-val (+ opos (codec/next-pair-size existing-val)))
                    (.put new-buf existing-val)
                    (.limit existing-val olimit)
                    (recur (unchecked-inc i))))))))
      (do
        (let [new-size (+ 4
                          (.remaining full-key)
                          (.remaining val))
              ^ByteBuffer new-buf (.reserve cursor lmdb-key new-size put-flags)]
          (.putInt new-buf 1)
          (.put new-buf full-key)
          (.put new-buf val)))))
  nil)

(defn del-raw [^Cursor cursor ^ByteBuffer lmdb-key ^ByteBuffer full-key]
  (cond
    (not (.get cursor lmdb-key GetOp/MDB_SET)) false
    (not full-key) (do (.delete cursor put-flags) true)
    :else (let [^ByteBuffer existing-val (.val cursor)
                ;; heap allocation, but already on a slow path. See note in put-raw
                tmp (ByteBuffer/allocate (.remaining existing-val))
                _ (.put tmp existing-val)
                existing-val (.flip tmp)
                collision-pos (find-collision-pos existing-val full-key)]
            (if (= -1 collision-pos)
              false
              (let [n (.getInt existing-val)]
                (if (= 1 n)
                  (do (.delete cursor put-flags) true)
                  (let [olimit (.limit existing-val)
                        opos (.position existing-val)
                        _ (.position existing-val collision-pos)
                        hit-size (codec/next-pair-size existing-val)
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
                    true)))))))

(defn- throw-on-err [err]
  (when err (throw (ex-info "Not enough space in write-buffer during insert" {:cinq/error err}))))

(defn- encode-key [^ByteBuffer lmdb-key-buffer ^ByteBuffer write-buffer symbol-table index key]
  (.clear write-buffer)
  (.clear lmdb-key-buffer)
  (codec/encode-object index write-buffer symbol-table true) ; has to be symbolic, guaranteed fit
  (throw-on-err (codec/encode-object key write-buffer nil false))
  (let [opos (.position write-buffer)]
    (.flip write-buffer)
    (.limit write-buffer (min (.limit write-buffer) (.remaining lmdb-key-buffer)))
    (.put lmdb-key-buffer write-buffer)
    (.limit write-buffer (.capacity write-buffer))
    (.position write-buffer opos)
    (codec/mark-if-key-truncated lmdb-key-buffer)))

(defn put-clustered [^Cursor cursor
                     ^ByteBuffer lmdb-key-buffer
                     ^ByteBuffer write-buffer
                     symbol-table
                     index
                     key
                     val]
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
      (put-raw cursor (.flip lmdb-key-buffer) key-buffer val-buffer))
    (do (.clear write-buffer)
        (throw-on-err (codec/encode-object val write-buffer symbol-table true))
        (put-raw cursor (.flip lmdb-key-buffer) nil (.flip write-buffer)))))

(defn del-clustered [^Cursor cursor
                     ^ByteBuffer lmdb-key-buffer
                     ^ByteBuffer write-buffer
                     symbol-table
                     index
                     key]
  (when (nil? key) (throw (ex-info "Key cannot be nil" {:val val})))
  (if (encode-key lmdb-key-buffer write-buffer symbol-table index key)
    (do
      (.flip write-buffer)
      (codec/skip-object write-buffer)
      (del-raw cursor (.flip lmdb-key-buffer) write-buffer))
    (del-raw cursor (.flip lmdb-key-buffer) nil)))

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
      (if-not hit
        acc
        (if (codec/truncated? (.key cursor))
          (let [index (codec/decode-object (.key cursor) symbol-list)
                ^ByteBuffer val-buf (.val cursor)
                n-pairs (.getInt val-buf)
                ret (walk-collision val-buf symbol-list n-pairs index f init fwd)]
            (if (reduced? ret)
              @ret
              (recur ret (if fwd (.next cursor) (.prev cursor)))))
          (let [index (codec/decode-object (.key cursor) symbol-list)
                ret (f acc index (.key cursor) (.val cursor))]
            (if (reduced? ret)
              ret
              (recur ret (if fwd (.next cursor) (.prev cursor))))))))))

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

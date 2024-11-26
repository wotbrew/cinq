(ns com.wotbrew.cinq.nio-codec
  "Encoding/decoding supported data to nio ByteBuffer's."
  (:import (clojure.lang Keyword Named Symbol)
           (com.wotbrew.cinq CinqDynamicArrayMap CinqUnsafeDynamicMap)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.util ArrayList Arrays Comparator Date HashMap List Map Set UUID)
           (java.util.concurrent ConcurrentHashMap)))

(set! *warn-on-reflection* true)

(defmacro case2 [expr & cases]
  (if (even? (count cases))
    `(case ~expr
       ~@(for [[test expr] (partition 2 cases)
               test (if (seq? test) test [test])
               form [(if (symbol? test) (eval test) test) expr]]
           form))
    `(case ~expr
       ~@(for [[test expr] (partition 2 cases)
               test (if (seq? test) test [test])
               form [(if (symbol? test) (eval test) test) expr]]
           form)
       ~(last cases))))

(defprotocol Encode
  (encode-object [o buffer symbol-table intern-flag]
    "Return nil if object was placed in the buffer. Return an error keyword (e.g ::not-enough-space) if not.

    Buffer is guaranteed to have at least 16 bytes remaining.

    Implementations do not need to reset the buffer position if a partial write happens."))

(def ^:const t-max 2147483647)

;; requires the following
;; map is only read/written under lock (write transaction)
;; added is only read/written under lock (write transaction)

;; list array can be read on any thread up to len (always swap the array ptr before the list-len as each operation)
;; list array, len is only modified under lock
;; at start of transaction, - len is captured, so prior state can be cleared in list-array, .adds is captured.
;; on error all added symbols (in adds) are removed from the table.

(defprotocol ISymbolTable
  (symbol-list [st])
  (intern-symbol [st symbol intern-flag])
  (rollback-adds [st])
  (clear-adds [st]))

(deftype SymbolTable [^Map map ^:volatile-mutable ^objects list-array ^ArrayList adds]
  ISymbolTable
  (intern-symbol [_ symbol allow-intern]
    (or (.get map symbol)
        (when allow-intern
          (let [n (.size map)
                tid (unchecked-add t-max (long n))]
            (.put map symbol tid)
            (.add adds symbol)
            (if (< n (alength list-array))
              (aset list-array n symbol)
              (let [new-list-array (object-array (* 2 (alength list-array)))]
                (System/arraycopy list-array 0 new-list-array 0 n)
                (aset new-list-array n symbol)
                (set! list-array new-list-array)))
            tid))))
  (rollback-adds [_]
    (dotimes [i (.size adds)]
      (let [k (.get adds i)
            n (unchecked-subtract (long (.get map k)) t-max)]
        (aset list-array n nil)
        (.remove map k)))
    (.clear adds))
  (clear-adds [_]
    (.clear adds))
  (symbol-list [_] list-array))

(def ^:const t-nil 0)
(def ^:const t-true 1)
(def ^:const t-false 2)
(def ^:const t-long-neg 3)
(def ^:const t-long 4)
(def ^:const t-double 5)
(def ^:const t-nan 6)
(def ^:const t-string 7)
(def ^:const t-small-map 8)
(def ^:const t-list 9)
(def ^:const t-set 10)
(def ^:const t-instant 11)

;; normally in symbol table - but not always
(def ^:const t-symbol 12)
(def ^:const t-keyword 13)

(def ^:const t-uuid 14)
(def ^:const t-big-map 15)

(def ^:const t-interned 16)

(def ^:const t-single-key-index 17)
(def ^:const t-multi-key-index 18)

(defn buffer-min-remaining? [^ByteBuffer buffer]
  ;; 16 min for type + payload, 4 min for byte count dynamic types
  (<= 24 (.remaining buffer)))

(defn- encode-double-lex ^long [^double d]
  (let [d (if (= d -0.0) 0.0 d)]
    (if (neg? d)
      (bit-not (Double/doubleToLongBits d))
      (bit-flip (Double/doubleToLongBits d) 63))))

(defn- decode-double-lex ^double [^long n]
  (if (bit-test n 63)
    (Double/longBitsToDouble (bit-flip n 63))
    (Double/longBitsToDouble (bit-not n))))

(defmacro record-byte-size-after-write [buffer-sym & body]
  (assert (symbol? buffer-sym))
  `(let [ipos# (.position ~buffer-sym)
         start-pos# (unchecked-add-int ipos# 4)]
     (.position ~buffer-sym start-pos#)
     (let [ret# (do ~@body)
           size# (unchecked-subtract-int (.position ~buffer-sym) start-pos#)]
       (.putInt ~buffer-sym ipos# size#)
       ret#)))

(declare encode-heap lex-comparator)

(defn- sort-lex-keys [kvs]
  ;; when key-encoding unsorted structures, subcomponents could in theory still use the symbol table
  ;; and save space but there is no way to convey this right now
  ;; examples would be e.g maps as keys in an index {:foo 42, :bar 43} :foo and :bar can use integers.
  (if (every? keyword? kvs)
    (sort-by key kvs)
    (sort-by (memoize #(encode-heap (key %) nil false)) lex-comparator (seq kvs))))

(defn encode-big-map [^Map m ^ByteBuffer buffer symbol-table intern-flag]
  (let [len (.size m)]
    (.putLong buffer t-big-map)
    (record-byte-size-after-write buffer
      (.putInt buffer len)
      (reduce
        (fn [_ [k v]]
          (some-> (or (when-not (buffer-min-remaining? buffer) ::not-enough-space)
                      (encode-object k buffer symbol-table intern-flag)
                      (when-not (buffer-min-remaining? buffer) ::not-enough-space)
                      (encode-object v buffer symbol-table intern-flag))
                  reduced))
        nil
        (if symbol-table m (sort-lex-keys m))))))

(defn encode-small-map [^Map m ^ByteBuffer buffer symbol-table intern-flag]
  (let [len (.size m)]
    ;; todo change type depending on whether all keys symbolic
    (.putLong buffer t-small-map)
    (record-byte-size-after-write buffer
      (.putInt buffer len)
      (if-not (buffer-min-remaining? buffer)
        ::not-enough-space
        (let [keys-size-pos (.position buffer)
              vals-size-pos (unchecked-add-int keys-size-pos 4)
              offset-table-pos (unchecked-add-int vals-size-pos 4)
              _ (.position buffer offset-table-pos)
              offset-table-size (* len 4)
              ctr (int-array 1)]
          (if-not (< offset-table-size (.remaining buffer))
            ::not-enough-space
            (let [kvs (if symbol-table m (sort-lex-keys m))] ; tune this later if it matters
              (.position buffer (unchecked-add-int offset-table-pos offset-table-size))
              (or
                ;; two loops provide the best encoding
                (reduce
                  (fn [_ [k]]
                    (let [i (aget ctr 0)]
                      (if-not (< i len)
                        (throw (ex-info "Possible map mutation during encode, exception thrown to avoid corruption" {}))
                        (do (aset ctr 0 (unchecked-inc i))
                            ;; put key
                            (if-not (buffer-min-remaining? buffer)
                              ::not-enough-space
                              (some-> (encode-object k buffer symbol-table intern-flag) reduced))))))
                  nil
                  kvs)
                (let [begin-vals-pos (.position buffer)
                      keys-size (unchecked-subtract-int begin-vals-pos (unchecked-add-int offset-table-pos offset-table-size))]
                  (.putInt buffer keys-size-pos keys-size)
                  (or
                    (let [start-pos (.position buffer)]
                      (aset ctr 0 0)
                      (reduce
                        (fn [_ [_ v]]
                          (let [i (aget ctr 0)]
                            (if-not (< i len)
                              (throw (ex-info "Possible map mutation during encode, exception thrown to avoid corruption" {}))
                              (do (aset ctr 0 (unchecked-inc i))
                                  ;; put offset from start
                                  (.putInt buffer
                                           (unchecked-add-int offset-table-pos (unchecked-multiply-int i 4))
                                           (unchecked-subtract (.position buffer) start-pos))
                                  ;; put val
                                  (if-not (buffer-min-remaining? buffer)
                                    ::not-enough-space
                                    (some-> (encode-object v buffer symbol-table intern-flag) reduced))))))
                        nil
                        kvs))
                    (let [end-vals-pos (.position buffer)
                          vals-size (unchecked-subtract-int end-vals-pos begin-vals-pos)]
                      (.putInt buffer vals-size-pos vals-size)
                      nil)))))))))))

(defn- maybe-encode-symbol [x ^ByteBuffer buffer symbol-table intern-flag]
  (let [n (intern-symbol symbol-table x intern-flag)]
    (if n
      (do
        (.putLong buffer n)
        nil)
      (encode-object x buffer nil intern-flag))))

(defn encode-interned
  [o buffer symbol-table intern-flag]
  (if symbol-table
    (maybe-encode-symbol o buffer symbol-table intern-flag)
    (do
      (.putLong ^ByteBuffer buffer t-interned)
      (if-not (buffer-min-remaining? buffer)
        ::not-enough-space
        (encode-object o buffer symbol-table intern-flag)))))

(defprotocol Index
  (get-indexed-key [index record])
  (get-indexed-val [index primary-key record]))

(defrecord SingleKeyIndex [table-id kind key]
  Index
  (get-indexed-key [index record] (get record key))
  (get-indexed-val [index primary-key record] (if (identical? :secondary kind) primary-key record)))

(defrecord MultikeyIndex [table-id kind keys]
  Index
  (get-indexed-key [index record] (mapv #(get record %) keys))
  (get-indexed-val [index primary-key record] (if (identical? :secondary kind) primary-key record)))

(extend-protocol Encode
  nil
  (encode-object [_ buffer symbol-table intern-flag]
    (.putLong ^ByteBuffer buffer t-nil)
    nil)
  Boolean
  (encode-object [b buffer symbol-table intern-flag]
    (if b
      (.putLong ^ByteBuffer buffer t-true)
      (.putLong ^ByteBuffer buffer t-false))
    nil)
  Long
  (encode-object [n buffer symbol-table intern-flag]
    (let [^ByteBuffer buffer buffer]
      (.putLong buffer (if (neg? n) t-long-neg t-long))
      (.putLong buffer n)
      nil))
  Integer
  (encode-object [n buffer symbol-table intern-flag]
    (encode-object (long n) buffer symbol-table intern-flag))
  Short
  (encode-object [n buffer symbol-table intern-flag]
    (encode-object (long n) buffer symbol-table intern-flag))
  Byte
  (encode-object [n buffer symbol-table intern-flag]
    (encode-object (long n) buffer symbol-table intern-flag))
  Double
  (encode-object [n buffer symbol-table intern-flag]
    (let [^ByteBuffer buffer buffer]
      (if (Double/isNaN n)
        (.putLong buffer t-nan)
        (do (.putLong buffer t-double)
            (.putLong buffer (encode-double-lex n)))))
    nil)
  Float
  (encode-object [n buffer symbol-table intern-flag]
    (encode-object (double n) buffer symbol-table intern-flag)
    nil)
  String
  (encode-object [s buffer symbol-table intern-flag]
    (let [^ByteBuffer buffer buffer
          barr (.getBytes ^String s StandardCharsets/UTF_8)]
      (do
        (.putLong buffer t-string)
        (.putInt buffer (alength barr))
        (if (< (.remaining buffer) (alength barr))
          (do
            (.put buffer barr 0 (min (alength barr) (.remaining buffer)))
            ::not-enough-space)
          (do (.put buffer barr)
              nil)))))
  Keyword
  (encode-object [s buffer symbol-table intern-flag]
    (if symbol-table
      (maybe-encode-symbol s buffer symbol-table intern-flag)
      (let [^ByteBuffer buffer buffer]
        (if (< (.remaining buffer) (+ 32 8))
          ::not-enough-space
          (do (.putLong buffer t-keyword)
              (or (encode-object (namespace s) buffer nil intern-flag)
                  (encode-object (name s) buffer nil intern-flag)))))))
  Symbol
  (encode-object [s buffer symbol-table intern-flag]
    (if symbol-table
      (maybe-encode-symbol s buffer symbol-table intern-flag)
      (let [^ByteBuffer buffer buffer]
        (if (< (.remaining buffer) (+ 32 8))
          ::not-enough-space
          (do (.putLong buffer t-symbol)
              (or (encode-object (namespace s) buffer nil intern-flag)
                  (encode-object (name s) buffer nil intern-flag)))))))
  Map
  (encode-object [m buffer symbol-table intern-flag]
    (if (< 32 (.size ^Map m))
      (encode-big-map m buffer symbol-table intern-flag)
      (encode-small-map m buffer symbol-table intern-flag)))
  List
  (encode-object [l buffer symbol-table intern-flag]
    (let [^List l l
          ^ByteBuffer buffer buffer]
      (.putLong buffer t-list)
      (record-byte-size-after-write buffer
        (.putInt buffer (.size l))
        (reduce
          (fn [_ x]
            (if-not (buffer-min-remaining? buffer)
              (reduced ::not-enough-space)
              (some-> (encode-object x buffer symbol-table intern-flag) reduced)))
          nil
          l))))
  Set
  (encode-object [s buffer symbol-table intern-flag]
    (let [^Set s s
          ^ByteBuffer buffer buffer]
      (.putLong buffer t-set)
      (.putInt buffer (.size s))
      (reduce
        (fn [_ x]
          (if-not (buffer-min-remaining? buffer)
            (reduced ::not-enough-space)
            (some-> (encode-object x buffer symbol-table intern-flag) reduced)))
        nil
        (if symbol-table s (sort-by #(encode-heap % nil false) s)))))
  Date
  (encode-object [d buffer _symbol-table intern-flag]
    (let [^ByteBuffer buffer buffer]
      (.putLong buffer t-instant)
      (.putLong buffer (inst-ms d))
      nil))
  UUID
  (encode-object [uuid buffer _symbol-table _intern-flag]
    (let [^UUID uuid uuid
          ^ByteBuffer buffer buffer]
      (.putLong buffer t-uuid)
      (.putLong buffer (.getMostSignificantBits uuid))
      (.putLong buffer (.getLeastSignificantBits uuid))
      nil))
  SingleKeyIndex
  (encode-object [i buffer symbol-table intern-flag]
    (.putLong buffer t-multi-key-index)
    (if-not (buffer-min-remaining? buffer)
      ::not-enough-space
      (encode-small-map i buffer symbol-table intern-flag)))
  MultikeyIndex
  (encode-object [i buffer symbol-table intern-flag]
    (.putLong buffer t-multi-key-index)
    (if-not (buffer-min-remaining? buffer)
      ::not-enough-space
      (encode-small-map i buffer symbol-table intern-flag))))

(declare decode-object)

(defn decode-string [^ByteBuffer buffer]
  (let [len (.getInt buffer)
        arr (byte-array len)]
    (.get buffer arr)
    (String. arr StandardCharsets/UTF_8)))

(defmacro skip-len [buffer]
  (assert (symbol? buffer))
  `(.position ~buffer (unchecked-add-int (.position ~buffer) 4)))

(defn decode-object-array [^ByteBuffer buffer symbol-list]
  (skip-len buffer)
  (let [len (.getInt buffer)
        items (object-array len)]
    (dotimes [i len]
      (aset items i (decode-object buffer symbol-list)))
    items))

(defn decode-list [^ByteBuffer buffer symbol-list]
  (vec (decode-object-array buffer symbol-list)))

(defn decode-small-map [^ByteBuffer buffer symbol-list]
  (skip-len buffer)
  (CinqDynamicArrayMap/read buffer symbol-list))

(defn decode-big-map [^ByteBuffer buffer symbol-list]
  (skip-len buffer)
  (let [len (.getInt buffer)]
    (loop [tm (transient (hash-map))
           i 0]
      (if (< i len)
        (recur (assoc! tm (decode-object buffer symbol-list) (decode-object buffer symbol-list))
               (unchecked-inc i))
        (persistent! tm)))))

(defn decode-set [^ByteBuffer buffer symbol-list]
  (let [len (.getInt buffer)
        m (volatile! (transient #{}))]
    (dotimes [_ len]
      (vswap! m conj! (decode-object buffer symbol-list)))
    (persistent! @m)))

(defn decode-date [^ByteBuffer buffer]
  (let [epoch (.getLong buffer)]
    (Date. epoch)))

(defn decode-symbol [^ByteBuffer buffer]
  (symbol (decode-object buffer nil) (decode-object buffer nil)))

(defn decode-keyword [^ByteBuffer buffer]
  (keyword (decode-object buffer nil) (decode-object buffer nil)))

(defn decode-uuid [^ByteBuffer buffer]
  (let [most (.getLong buffer)
        least (.getLong buffer)]
    (UUID. most least)))

(defn decode-object [^ByteBuffer buffer ^objects symbol-list]
  (let [tid (.getLong buffer)]
    (case2 tid
      t-nil nil
      t-true true
      t-false false
      t-long-neg (.getLong buffer)
      t-long (.getLong buffer)
      t-double (decode-double-lex (.getLong buffer))
      t-nan Double/NaN
      t-string (decode-string buffer)
      t-list (decode-list buffer symbol-list)
      t-small-map (decode-small-map buffer symbol-list)
      t-set (decode-set buffer symbol-list)
      t-instant (decode-date buffer)
      t-symbol (decode-symbol buffer)
      t-keyword (decode-keyword buffer)
      t-uuid (decode-uuid buffer)
      t-big-map (decode-big-map buffer symbol-list)
      t-interned (decode-object buffer symbol-list)
      t-single-key-index (map->SingleKeyIndex (decode-small-map buffer symbol-list))
      t-multi-key-index (map->MultikeyIndex (decode-small-map buffer symbol-list))
      (aget symbol-list (unchecked-subtract tid t-max)))))

(defn decode-root-unsafe [^CinqUnsafeDynamicMap mut-record ^ByteBuffer buffer ^objects symbol-list]
  (let [tid (.getLong buffer)]
    (case2 tid
      t-nil nil
      t-true true
      t-false false
      t-long (.getLong buffer)
      t-double (.getDouble buffer)
      t-string (decode-string buffer)
      t-list (decode-list buffer symbol-list)
      t-small-map (do (.read mut-record buffer) mut-record)
      t-set (decode-set buffer symbol-list)
      t-instant (decode-date buffer)
      t-symbol (decode-symbol buffer)
      t-keyword (decode-keyword buffer)
      t-uuid (decode-uuid buffer)
      t-big-map (decode-big-map buffer symbol-list)
      (aget symbol-list (unchecked-subtract tid t-max)))))

(defn empty-symbol-table []
  (->SymbolTable (ConcurrentHashMap.) (object-array 32) (ArrayList.)))

(defn list-adds [^SymbolTable symbol-table]
  (.-adds symbol-table))

(def ^:const max-key-size 511)

(defn mark-if-key-truncated [^ByteBuffer buf]
  (let [pos (.position buf)
        at-limit (<= max-key-size pos)]
    (when at-limit
      (.position buf (unchecked-dec max-key-size))
      (.put buf (unchecked-byte 0xFF))
      (.limit buf (.position buf)))
    at-limit))

(defn truncated? [^ByteBuffer buf]
  (and (= max-key-size (.limit buf))
       (= (unchecked-byte 0xFF) (.get buf (int (unchecked-dec max-key-size))))))

(defn encode-key [o ^ByteBuffer buf]
  ;; TODO discard result, do we need to handle any errors here?
  (encode-object o buf nil false)
  (mark-if-key-truncated buf))

(defn encode-heap ^ByteBuffer
  [o symbol-table intern-flag]
  (loop [buf (ByteBuffer/allocate 64)]
    (if-some [err (encode-object o buf symbol-table intern-flag)]
      (case err
        ::not-enough-space
        (recur (ByteBuffer/allocate (* 2 (.capacity buf)))))
      (.flip buf))))

(defn encode-interned-heap ^ByteBuffer
  [o symbol-table intern-flag]
  (loop [buf (ByteBuffer/allocate 16)]
    (if-some [err (encode-interned o buf symbol-table intern-flag)]
      (case err
        ::not-enough-space
        (recur (ByteBuffer/allocate (* 2 (.capacity buf)))))
      (.flip buf))))

(defn- compare-mixed-type-nums [^ByteBuffer buf ^ByteBuffer valbuf symbol-list]
  (let [a-pos (.position buf)
        a (decode-object buf symbol-list)
        b-pos (.position valbuf)
        b (decode-object valbuf symbol-list)]
    (.position buf a-pos)
    (.position valbuf b-pos)
    (compare a b)))

(defn compare-lex-bufs ^long [^ByteBuffer buf1 ^ByteBuffer buf2]
  (let [mis (.mismatch buf1 buf2)]
    (if (= -1 mis)
      (compare (.remaining buf1) (.remaining buf2))
      (let [pos0 (unchecked-add (.position buf1) mis)
            pos1 (unchecked-add (.position buf2) mis)]
        (Byte/compareUnsigned (.get buf1 pos0) (.get buf2 pos1))))))

(def ^Comparator lex-comparator
  (reify Comparator
    (compare [_ a b] (compare-lex-bufs a b))))

(defn- compare-bufs* [^long tid ^ByteBuffer buf ^ByteBuffer valbuf symbol-list]
  (let [val-tid (.getLong valbuf (.position valbuf))]
    (when-not (= t-nil val-tid)
      (let [bin-ret (compare-lex-bufs buf valbuf)]
        (if (= 0 bin-ret)
          0
          (case2 tid
            (t-long-neg t-long t-double)
            (if (= val-tid tid)
              bin-ret
              (compare-mixed-type-nums buf valbuf symbol-list))
            (if (= tid val-tid)
              bin-ret
              (Long/compare tid val-tid))))))))

(defn compare-bufs [^ByteBuffer buf ^ByteBuffer valbuf symbol-list]
  (compare-bufs* (.getLong buf (.position buf)) buf valbuf symbol-list))

(defn next-object-size
  "Return the size of the next object in the buffer, does not advance the position."
  ^long [^ByteBuffer buf]
  (case2 (.getLong buf (.position buf))
    t-nil 8
    t-true 8
    t-false 8
    t-long-neg 16
    t-long 16
    t-double 16
    t-nan 8
    t-string (+ 8 4 (.getInt buf (+ (.position buf) 8)))

    (t-keyword t-symbol)
    (let [opos (.position buf)
          _ (.position buf (+ opos 8))
          ns-size (next-object-size buf)
          _ (.position buf (+ opos 8 ns-size))
          name-size (next-object-size buf)]
      (.position buf opos)
      (+ 8 ns-size name-size))

    (t-list t-small-map t-big-map)
    (let [content-size (.getInt buf (+ (.position buf) 8))]
      (+ 8 4 content-size))

    t-instant 16
    t-uuid 24
    t-interned
    (let [opos (.position buf)
          _ (.position buf (+ opos 8))
          m-size (next-object-size buf)]
      (.position buf opos)
      (+ 8 m-size))

    ;; assume symbolic
    8
    ))

(defn next-pair-size
  "Returns the size of the next 2 objects (for pairs)"
  ^long [^ByteBuffer buf]
  (let [pos (.position buf)
        size1 (next-object-size buf)
        _ (.position buf (+ pos size1))
        size2 (next-object-size buf)
        _ (.position buf pos)]
    (+ size1 size2)))

(defn skip-object
  "Positions the buffer immediately after the next object, assumes such a position is valid."
  [^ByteBuffer buf]
  (.position buf (+ (.position buf) (next-object-size buf))))

(defn focus
  "Position the buffer .position, .limit at the value object for the given key. Return true if found, false if not."
  [^ByteBuffer buf ^ByteBuffer key symbol-list]
  (let [tid (.getLong buf)]
    (cond
      (= t-small-map tid)
      (let [_ (skip-len buf)
            len (.getInt buf)
            key-size (.getInt buf)
            _val-size (.getInt buf)
            offset-pos (.position buf)
            olimit (.limit buf)
            offset-table-size (unchecked-multiply-int len 4)
            key-start-pos (unchecked-add-int offset-pos offset-table-size)
            val-start-pos (unchecked-add-int key-start-pos key-size)]
        (if (= 0 len)
          false
          (do
            (.position buf (+ (.position buf) (* len 4)))
            (loop [i 0]
              (if (= i len)
                false
                (let [next-size (next-object-size buf)
                      _ (.limit buf (+ (.position buf) next-size))
                      comp-res (compare-bufs key buf symbol-list)
                      _ (.limit buf olimit)]
                  (if (= 0 comp-res)
                    ;;hit
                    (let [start (unchecked-add-int val-start-pos (.getInt buf (+ offset-pos (* i 4))))
                          end (if (< (inc i) len)
                                (unchecked-add-int val-start-pos (.getInt buf (+ offset-pos (* (inc i) 4))))
                                (.limit buf))]
                      (.position buf start)
                      (.limit buf end)
                      true)
                    (do
                      (skip-object buf)
                      (recur (unchecked-inc i))))))))))
      :else false)))

(defn bufcmp-ksv [^ByteBuffer buf ^ByteBuffer valbuf ^long keysym symbol-list]
  (let [tid (.getLong valbuf)
        buf-tid (.getLong buf (.position buf))]
    (cond
      (= buf-tid t-nil) nil
      (= t-small-map tid)
      (let [_ (skip-len valbuf)
            len (.getInt valbuf)
            key-size (.getInt valbuf)
            _val-size (.getInt valbuf)
            offset-pos (.position valbuf)
            offset-table-size (unchecked-multiply-int len 4)
            key-start-pos (unchecked-add-int offset-pos offset-table-size)
            val-start-pos (unchecked-add-int key-start-pos key-size)]
        (if (= 0 len)
          nil
          (do
            (.position valbuf (+ (.position valbuf) (* len 4)))
            (loop [i 0]
              (if (= i len)
                nil
                (let [k (.getLong valbuf)]
                  ;; is a kw
                  (if (<= t-max k)
                    ;; is a kw, equal to sym int
                    (if (= keysym k)
                      ;; hit
                      (let [start (unchecked-add-int val-start-pos (.getInt valbuf (+ offset-pos (* i 4))))
                            end (if (< (inc i) len)
                                  (unchecked-add-int val-start-pos (.getInt valbuf (+ offset-pos (* (inc i) 4))))
                                  (.limit valbuf))]
                        (.position valbuf start)
                        (.limit valbuf end)
                        (compare-bufs* buf-tid buf valbuf symbol-list))
                      ;; miss, kw is already read - move to next key
                      (recur (inc i)))
                    ;; not a kw, move backwards, skip
                    (do (.position valbuf (dec (.position valbuf)))
                        ;; skip key
                        (skip-object valbuf)
                        (recur (inc i))))))))))
      :else
      nil)))

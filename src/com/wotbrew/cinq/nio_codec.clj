(ns com.wotbrew.cinq.nio-codec
  "Encoding/decoding supported data to nio ByteBuffer's."
  (:import (clojure.lang Keyword Symbol)
           (com.wotbrew.cinq CinqDynamicArrayRecord CinqLongBox)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.util Date List Map Set)))

(defprotocol Encode
  (encode-object [o buffer symbol-table]
    "Return true if object was placed in the buffer.

    Buffer is guaranteed to have at least 16 bytes remaining.

    Implementations do not need to reset the buffer position if a partial write happens.

    False if not."))

(def ^:const t-max 2147483647)

(defrecord SymbolTableValue [map vec added])

(defn intern-symbol [symbol-table symbol]
  (-> ^SymbolTableValue (swap! symbol-table (fn [^SymbolTableValue r]
                            (let [m (.-map r)]
                              (if (contains? m symbol)
                                r
                                (->SymbolTableValue
                                  (assoc m symbol (unchecked-add t-max (long (count m))))
                                  (conj (.-vec r) symbol)
                                  (conj (.-added r) symbol))))))
      .-map
      (get symbol)))

(def ^:const t-nil 0)
(def ^:const t-true 1)
(def ^:const t-false 2)
(def ^:const t-long 3)
(def ^:const t-double 4)
(def ^:const t-string 5)
(def ^:const t-map 6)
(def ^:const t-list 7)
(def ^:const t-set 8)
(def ^:const t-date 9)

;; normally in symbol table - but not always
(def ^:const t-symbol 10)
(def ^:const t-keyword 11)

(defn buffer-min-remaining? [^ByteBuffer buffer]
  (<= 16 (.remaining buffer)))

(extend-protocol Encode
  nil
  (encode-object [_ buffer symbol-table]
    (.putLong ^ByteBuffer buffer t-nil)
    true)
  Boolean
  (encode-object [b buffer symbol-table]
    (if b
      (.putLong ^ByteBuffer buffer t-true)
      (.putLong ^ByteBuffer buffer t-false))
    true)
  Long
  (encode-object [n buffer symbol-table]
    (let [^ByteBuffer buffer buffer]
      (.putLong buffer t-long)
      (.putLong buffer n)
      true))
  Integer
  (encode-object [n buffer symbol-table]
    (let [^ByteBuffer buffer buffer]
      (.putLong buffer t-long)
      (.putLong buffer n)
      true))
  Short
  (encode-object [n buffer symbol-table]
    (let [^ByteBuffer buffer buffer]
      (.putLong buffer t-long)
      (.putLong buffer n)
      true))
  Byte
  (encode-object [n buffer symbol-table]
    (let [^ByteBuffer buffer buffer]
      (.putLong buffer t-long)
      (.putLong buffer n)
      true))
  Double
  (encode-object [n buffer symbol-table]
    (let [^ByteBuffer buffer buffer]
      (.putLong buffer t-double)
      (.putDouble buffer n))
    true)
  Float
  (encode-object [n buffer symbol-table]
    (let [^ByteBuffer buffer buffer]
      (.putLong buffer t-double)
      (.putDouble buffer n))
    true)
  String
  (encode-object [s buffer symbol-table]
    (let [^ByteBuffer buffer buffer
          barr (.getBytes ^String s StandardCharsets/UTF_8)]
      (if (< (.remaining buffer) (+ 8 4 (alength barr)))
        false
        (do
          (.putLong buffer t-string)
          (.putInt buffer (alength barr))
          (.put buffer barr)
          true))))
  Keyword
  (encode-object [s buffer symbol-table]
    (if symbol-table
      (let [^ByteBuffer buffer buffer
            n (intern-symbol symbol-table s)]
        (.putLong buffer n)
        true)
      (let [^ByteBuffer buffer buffer]
        (if (< (.remaining buffer) (+ 16 8))
          false
          (do (.putLong buffer t-keyword)
              (and (encode-object (namespace s) buffer nil)
                   (encode-object (name s) buffer nil)))))))
  Symbol
  (encode-object [s buffer symbol-table]
    (if symbol-table
      (let [^ByteBuffer buffer buffer
            n (intern-symbol symbol-table s)]
        (.putLong buffer n)
        true)
      (let [^ByteBuffer buffer buffer]
        (if (< (.remaining buffer) (+ 16 8))
          false
          (do (.putLong buffer t-symbol)
              (and (encode-object (namespace s) buffer nil)
                   (encode-object (name s) buffer nil)))))))
  Map
  (encode-object [m buffer symbol-table]
    (let [^Map m m
          ^ByteBuffer buffer buffer
          len (.size m)]
      (.putLong buffer t-map)
      (.putInt buffer len)
      (let [offset-table-size (* len 8)
            offset-table-pos (.position buffer)
            ctr (CinqLongBox. 0)]
        (and (< offset-table-size (.remaining buffer))
             (do (dotimes [_ len]
                   (.putInt buffer 0)
                   (.putInt buffer 0))
                 true)
             (reduce-kv
               (fn [_ k v]
                 (let [i (.-val ctr)]
                   (if-not (< i len)
                     (throw (ex-info "Possible map mutation during encode, exception thrown to avoid corruption" {}))
                     (let [key-pos (.position buffer)]
                       (set! (.-val ctr) (unchecked-inc i))
                       (.putInt buffer
                                (unchecked-add offset-table-pos (unchecked-multiply i 8))
                                (unchecked-subtract key-pos offset-table-pos))
                       (if (and (buffer-min-remaining? buffer)
                                (encode-object k buffer symbol-table))
                         (let [val-pos (.position buffer)]
                           (.putInt buffer
                                    (unchecked-add offset-table-pos (unchecked-add 4 (unchecked-multiply i 8)))
                                    (unchecked-subtract val-pos offset-table-pos))
                           (if (and (buffer-min-remaining? buffer)
                                    (encode-object v buffer symbol-table))
                             true
                             (reduced false)))
                         (reduced false))))))
               true
               m)))))
  List
  (encode-object [l buffer symbol-table]
    (let [^List l l
          ^ByteBuffer buffer buffer]
      (.putLong buffer t-list)
      (.putInt buffer (.size l))
      (reduce
        (fn [_ x]
          (if (and (buffer-min-remaining? buffer)
                   (encode-object x buffer symbol-table))
            true
            (reduced false)))
        true
        l)))
  Set
  (encode-object [s buffer symbol-table]
    (let [^Set s s
          ^ByteBuffer buffer buffer]
      (.putLong buffer t-set)
      (.putInt buffer (.size s))
      (reduce
        (fn [_ x]
          (if (and (buffer-min-remaining? buffer)
                   (encode-object x buffer symbol-table))
            true
            (reduced false)))
        true
        s)))
  Date
  (encode-object [d buffer _symbol-table]
    (let [^ByteBuffer buffer buffer]
      (.putLong buffer t-date)
      (.putLong buffer (inst-ms d))
      true)))

(defmacro case2 [expr & cases]
  (if (even? (count cases))
    `(case ~expr
       ~@(for [[test expr] (partition 2 cases)
               form [(if (symbol? test) (eval test) test) expr]]
           form))
    `(case ~expr
       ~@(for [[test expr] (partition 2 cases)
               form [(if (symbol? test) (eval test) test) expr]]
           form)
       ~(last cases))))

(declare decode-object)

(defn decode-string [^ByteBuffer buffer]
  (let [len (.getInt buffer)
        arr (byte-array len)]
    (.get buffer arr)
    (String. arr StandardCharsets/UTF_8)))

(defn decode-object-array [^ByteBuffer buffer symbol-list]
  (let [len (.getInt buffer)
        items (object-array len)]
    (dotimes [i len]
      (aset items i (decode-object buffer symbol-list)))
    items))

(defn decode-list [^ByteBuffer buffer symbol-list]
  (vec (decode-object-array buffer symbol-list)))

(defn buffer-copy [^ByteBuffer buffer]
  (let [buf (ByteBuffer/allocate (.remaining buffer))]
    (.put buf buffer)
    (.flip buf)))

(defn decode-map [^ByteBuffer buffer symbol-list]
  #_(let [len (.getInt buffer)
        ;; skip offset map
        _ (.position buffer (+ (.position buffer) (* len 8)))
        m (volatile! (transient {}))]
    (dotimes [_ len]
      (vswap! m assoc! (decode-object buffer symbol-list) (decode-object buffer symbol-list)))
    (persistent! @m))

  (let [len (.getInt buffer)
        start-pos (.position buffer)
        offsets (int-array len)
        keys (object-array len)
        vals (object-array len)
        table-size (unchecked-multiply-int len 8)]
    (dotimes [i len]
      (let [k-offset (.getInt buffer)
            v-offset (.getInt buffer)]
        (.mark buffer)
        (.position buffer (unchecked-add-int start-pos k-offset))
        (aset keys (int i) (decode-object buffer symbol-list))
        (aset offsets (int i) (unchecked-subtract-int v-offset table-size))
        (.reset buffer)))
    (CinqDynamicArrayRecord. keys vals offsets (buffer-copy buffer) symbol-list)))

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

(defn decode-object [^ByteBuffer buffer symbol-list]
  (let [tid (.getLong buffer)]
    (case2 tid
      t-nil nil
      t-true true
      t-false false
      t-long (.getLong buffer)
      t-double (.getDouble buffer)
      t-string (decode-string buffer)
      t-list (decode-list buffer symbol-list)
      t-map (decode-map buffer symbol-list)
      t-set (decode-set buffer symbol-list)
      t-date (decode-date buffer)
      t-symbol (decode-symbol buffer)
      t-keyword (decode-keyword buffer)
      (nth symbol-list (unchecked-subtract tid t-max)))))

(defn symbol-list [symbol-table]
  (.-vec ^SymbolTableValue (deref symbol-table)))

(defn empty-symbol-table []
  (atom (->SymbolTableValue {} [] [])))

(defn encode
  ^ByteBuffer [o & {:keys [direct]}]
  (let [symbol-table (empty-symbol-table)]
    (loop [content-buf (ByteBuffer/allocate 16)]
      (if (encode-object o content-buf symbol-table)
        (loop [symbol-buf (ByteBuffer/allocate 16)]
          (if (encode-object (symbol-list symbol-table) symbol-buf nil)
            (let [_ (do (.flip content-buf)
                        (.flip symbol-buf))
                  cap (+ (.remaining content-buf) (.remaining symbol-buf))
                  buf (if direct (ByteBuffer/allocateDirect cap) (ByteBuffer/allocate cap))]
              (.put buf symbol-buf)
              (.put buf content-buf)
              (.flip buf))
            (recur (ByteBuffer/allocate (* 2 (.capacity symbol-buf))))))
        (recur (ByteBuffer/allocate (* 2 (.capacity content-buf))))))))

(defn decode [^ByteBuffer buf]
  (decode-object buf (decode-object buf [])))

(defn clear-symbol-adds [symbol-table]
  (swap! symbol-table assoc :added []))

(comment

  (-> (encode {:foo 42})
      (decode))

  (-> (encode {:foo [true nil false 42 42.3 "hello, world"]})
      (decode))

  )

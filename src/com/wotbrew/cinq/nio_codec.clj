(ns com.wotbrew.cinq.nio-codec
  "Encoding/decoding supported data to nio ByteBuffer's."
  (:import (clojure.lang Keyword Symbol)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.util ArrayList Date HashMap HashSet List Map Set)
           (java.util.function Function)))

(defprotocol Encode
  (encode-object [o buffer symbol-table]
    "Return true if object was placed in the buffer.

    Buffer is guaranteed to have at least 16 bytes remaining.

    Implementations do not need to reset the buffer position if a partial write happens.

    False if not."))

(def ^:const t-max 2147483647)

(defn intern-symbol [symbol-table symbol]
  (.computeIfAbsent ^Map symbol-table symbol (reify Function (apply [_ _] (unchecked-add t-max (long (.size ^Map symbol-table)))))))

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
  Double
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
          ^ByteBuffer buffer buffer]
      (.putLong buffer t-map)
      (.putInt buffer (.size m))
      (reduce-kv
        (fn [_ k v]
          (if (and (buffer-min-remaining? buffer)
                   (encode-object k buffer symbol-table)
                   (buffer-min-remaining? buffer)
                   (encode-object v buffer symbol-table))
            true
            (reduced false)))
        true
        m)))
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

(defn decode-object [^ByteBuffer buffer symbol-list]
  (let [tid (.getLong buffer)]
    (condp = tid
      t-nil nil
      t-true true
      t-false false
      t-long (.getLong buffer)
      t-double (.getDouble buffer)
      t-string
      (let [len (.getInt buffer)
            arr (byte-array len)]
        (.get buffer arr)
        (String. arr StandardCharsets/UTF_8))

      t-list
      (let [len (.getInt buffer)
            items (object-array len)]
        (dotimes [i len]
          (aset items i (decode-object buffer symbol-list)))
        (vec items))

      t-map
      (let [len (.getInt buffer)
            m (volatile! (transient {}))]
        (dotimes [_ len]
          (let [k (decode-object buffer symbol-list)
                v (decode-object buffer symbol-list)]
            (vswap! m assoc! k v)))
        (persistent! @m))

      t-set
      (let [len (.getInt buffer)
            m (volatile! (transient #{}))]
        (dotimes [_ len]
          (vswap! m conj! (decode-object buffer symbol-list)))
        (persistent! @m))

      t-date
      (let [epoch (.getLong buffer)]
        (Date. epoch))

      t-symbol
      (symbol (decode-object buffer symbol-list) (decode-object buffer symbol-list))

      t-keyword
      (keyword (decode-object buffer symbol-list) (decode-object buffer symbol-list))

      (nth symbol-list (unchecked-subtract tid t-max)))))

(defn symbol-list [^Map symbol-table]
  (let [l (object-array (.size symbol-table))]
    (doseq [[k v] symbol-table]
      (aset l (unchecked-subtract (long v) t-max) k))
    (vec l)))

(defn encode
  ^ByteBuffer [o & {:keys [direct]}]
  (let [symbol-table (HashMap.)]
    (loop [content-buf (ByteBuffer/allocate 16)]
      (if (encode-object o content-buf symbol-table)
        (loop [symbol-buf (ByteBuffer/allocate 16)]
          (if (encode-object symbol-table symbol-buf nil)
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
  (let [symbol-table (decode-object buf [])]
    (decode-object buf (symbol-list symbol-table))))

(comment

  (-> (encode {:foo [true nil false 42 42.3 "hello, world"]})
      (decode))

  )

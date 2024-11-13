(ns com.wotbrew.cinq.lmdb-kv-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.test.check.generators :as gen]
            [com.gfredericks.test.chuck :as chuck]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [com.wotbrew.cinq.lmdb-kv :as kv]
            [com.wotbrew.cinq.nio-codec :as codec])
  (:import (clojure.lang IDeref)
           (java.io Closeable File)
           (java.nio ByteBuffer ByteOrder)
           (java.util Comparator)
           (org.lmdbjava DbiFlags Env EnvFlags)))

(defn closeable [x close-fn]
  (reify IDeref
    (deref [_] x)
    Closeable
    (close [_] (close-fn x))))

(defn- open-env [file]
  (-> (Env/create)
      (.setMapSize (* 8 1024 1024))
      (.setMaxDbs 1)
      (.open
        (io/file file)
        ^"[Lorg.lmdbjava.EnvFlags;" (into-array EnvFlags [EnvFlags/MDB_NOSUBDIR
                                                          EnvFlags/MDB_NOTLS]))))

(defn with-tmp-cursor [f]
  (with-open [tmp-file (closeable (File/createTempFile "test" ".cinq") io/delete-file)
              env (open-env @tmp-file)]
    (let [dbi (.openDbi env "a" (into-array DbiFlags [DbiFlags/MDB_CREATE #_DbiFlags/MDB_DUPSORT]))
          symbol-table (codec/empty-symbol-table)
          key-buffer (ByteBuffer/allocateDirect 511)
          val-buffer (ByteBuffer/allocateDirect (* 32 1024))]
      (with-open [txn (.txnWrite env)
                  cursor (.openCursor dbi txn)]
        (let [mat (fn [] (vec (keep (fn [{:keys [index key val]}] (when (= :default index) [key val])) (kv/materialize-all cursor symbol-table))))
              put (fn [k v] (kv/put-clustered cursor key-buffer val-buffer symbol-table :default k v))
              del (fn [k] (kv/del-clustered cursor key-buffer val-buffer symbol-table :default k))]
          (f {:cursor cursor
              :symbol-table symbol-table
              :key-buffer key-buffer
              :val-buffer val-buffer
              :mat mat
              :put put
              :del del}))))))

(deftest simple-rt-test
  (with-tmp-cursor
    (fn [{:keys [mat put del]}]
      (is (= [] (mat)))
      (is (nil? (put :greeting "Hello, world")))
      (is (= [[:greeting "Hello, world"]] (mat)))
      (is (nil? (put :greeting2 "Bonjour!")))
      (is (= [[:greeting "Hello, world"] [:greeting2 "Bonjour!"]] (mat)))
      (is (true? (del :greeting)))
      (is (false? (del :greeting)))
      (is (= [[:greeting2 "Bonjour!"]] (mat)))
      (is (true? (del :greeting2)))
      (is (= [] (mat))))))

(deftest long-string-test
  (with-tmp-cursor
    (fn [{:keys [mat put del]}]
      (let [prefix (apply str (repeat 512 "a"))
            s #(str prefix %)]
        (put (s "a") 0)
        (is (= [[(s "a") 0]] (mat)))
        (put (s "a") 0)
        (is (= [[(s "a") 0]] (mat)))
        (put (s "c") 2)
        (is (= [[(s "a") 0] [(s "c") 2]] (mat)))
        (put (s "b") 1)
        (is (= [[(s "a") 0] [(s "b") 1] [(s "c") 2]] (mat)))
        (put "" -1)
        (is (= [["" -1] [(s "a") 0] [(s "b") 1] [(s "c") 2]]))
        (del (s "a"))
        (is (= [["" -1] [(s "b") 1] [(s "c") 2]] (mat)))))))

(deftest numeric-test
  (with-tmp-cursor
    (fn [{:keys [mat put]}]
      (put 0 0)
      (put -1 0)
      (is (= [-1 0] (map first (mat))))))

  (checking "all indexes are lex sorted" (chuck/times 100)
    [x (gen/vector (gen/one-of [gen/large-integer
                                (gen/double* {:NaN? false})]))]
    (with-tmp-cursor
      (fn [{:keys [mat put]}]
        (run! #(put % %) x)
        (is (= (sort-by #(codec/encode-heap % nil false) codec/lex-comparator (distinct x))
               (map first (mat))))))))

(def primitive-gen
  (gen/one-of [gen/small-integer
               ;; TODO re-enable NaN when we scan binary instead
               (gen/double* {:NaN? false})
               gen/string
               gen/keyword
               gen/symbol]))

(def val-gen
  (gen/recursive-gen
    (fn [inner]
      (gen/one-of [(gen/vector inner)
                   (gen/map inner inner)]))
    primitive-gen))

(deftest all-indexes-are-lex-sorted-test
  (checking "all indexes are lex sorted" (chuck/times 100)
    [x (gen/vector val-gen)]
    (with-tmp-cursor
      (fn [{:keys [mat put]}]
        (run! #(put % %) x)
        (is (= (sort-by (memoize #(codec/encode-heap % nil false)) codec/lex-comparator (distinct x))
               (map first (mat))))))))

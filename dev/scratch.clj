(ns scratch
  (:import (java.nio ByteBuffer)))

(set! *warn-on-reflection* true)

(require 'criterium.core)
(require 'clj-async-profiler.core)
(require 'com.wotbrew.cinq.tpch-test)

(import 'com.wotbrew.cinq.tpch_test.Lineitem)
(import 'java.util.Date)

(comment

  (def dataset @com.wotbrew.cinq.tpch-test/sf-005)
  (def dataset @com.wotbrew.cinq.tpch-test/sf-01)
  (def dataset @com.wotbrew.cinq.tpch-test/sf-1)

  ;; unwrapped Date, theoretical fastest
  ;; 409 micros
  (let [a (long-array (map (fn [{:keys [^Date shipdate]}] (.getTime shipdate)) (:lineitem dataset)))
        sd2 (.getTime #inst "1998-09-02")]
    (criterium.core/quick-bench
      (let [^longs a a
            sd2 (long sd2)]
        (loop [n 0
               i 0]
          (if (< i (alength a))
            (let [o (aget a i)]
              (if (<= o sd2)
                (recur (unchecked-inc n) (unchecked-inc i))
                (recur n (unchecked-inc i))))
            n)))))

  ;; adding a box (and compare), no clojure compare
  ;; 2.162507 ms
  (let [a (object-array (:lineitem dataset))]
    (criterium.core/quick-bench
      (let [^objects a a]
        (loop [n 0
               i 0]
          (if (< i (alength a))
            (let [^Lineitem o (aget a i)
                  ^Date shipdate (.-shipdate o)]
              (if (<= (compare shipdate #inst "1998-09-02") 0)
                (recur (unchecked-inc n) (unchecked-inc i))
                (recur n (unchecked-inc i))))
            n)))))

  ;; adding a box, iterator (and compare), no clojure compare
  ;; 2.162507 ms
  (let [a (object-array (:lineitem dataset))]
    (criterium.core/quick-bench
      (let [a (clojure.lang.ArrayIter/create a)]
        (loop [n 0
               i 0]
          (if (.hasNext a)
            (let [^Lineitem o (.next a)
                  ^Date shipdate (.-shipdate o)]
              (if (<= (compare shipdate #inst "1998-09-02") 0)
                (recur (unchecked-inc n) (unchecked-inc i))
                (recur n (unchecked-inc i))))
            n)))))

  (defn rmq1 [^objects o]
    (com.wotbrew.cinq.tpch-test/iter-count
      (com.wotbrew.cinq.row-major/q
        [^Lineitem l o
         :when (<= l:shipdate #inst "1998-09-02")
         :group []]
        %count)))

  ;; ArrayIter 4.342572 ms
  (let [o (object-array (:lineitem dataset))]
    (criterium.core/quick-bench (rmq1 o)))

  ;; reducing over vector 5.038593 ms, no count specialisation
  ;; graal 2.946812 ms
  (criterium.core/quick-bench
    (com.wotbrew.cinq.tpch-test/iter-count
      (com.wotbrew.cinq.row-major/q
        [^Lineitem l (:lineitem dataset)
         :when (<= l:shipdate #inst "1998-09-02")]
        l)))

  ;; clojure version
  ;; graal 8.611135 ms
  (criterium.core/quick-bench
    (->> (:lineitem dataset)
         (filter #(<= (compare (:shipdate %) #inst "1998-09-02") 0))
         count))
  ;; clojure version transducer
  ;; graal 11.017295 ms
  (criterium.core/quick-bench
    (->> (:lineitem dataset)
         (eduction (filter #(<= (compare (:shipdate %) #inst "1998-09-02") 0)))
         com.wotbrew.cinq.tpch-test/iter-count))

  ;; using a vector, %count Iter 5.810982 ms
  (criterium.core/quick-bench
    (com.wotbrew.cinq.tpch-test/iter-count
      (com.wotbrew.cinq.row-major/q
        [^Lineitem l (:lineitem dataset)
         :when (<= l:shipdate #inst "1998-09-02")
         :group []]
        %count)))

  ;; using a vector-seq iter 6.133335 ms
  ;; graal 5.667854 ms
  (criterium.core/quick-bench
    (com.wotbrew.cinq.tpch-test/iter-count
      (com.wotbrew.cinq.array-seq/q*
        [l (:lineitem dataset)
         :when (<= l:shipdate #inst "1998-09-02")
         :group []]
        %count)))


  ;; sf1 261.506492 ms
  ;; graal 1, 139.939131 ms, 0.05 4.8ms
  (criterium.core/quick-bench
    (first
      (com.wotbrew.cinq.row-major/q
        [^Lineitem l (:lineitem dataset)
         :when (<= l:shipdate #inst "1998-09-02")
         ;; could use optimising the array list way when we do not need it
         :group []]
        ($select :sum_qty ($sum l:quantity)
                 :sum_base_price ($sum l:extendedprice)
                 :sum_disc_price ($sum (* l:extendedprice (- 1.0 l:discount)))))))

  ;; 720.501554 ms
  (criterium.core/quick-bench
    (first
      (com.wotbrew.cinq.array-seq/q
        [l (:lineitem dataset)
         :when (<= l:shipdate #inst "1998-09-02")
         ;; could use optimising the array list way when we do not need it
         :group []]
        ($select :sum_qty ($sum l:quantity)
                 :sum_base_price ($sum l:extendedprice)
                 :sum_disc_price ($sum (* l:extendedprice (- 1.0 l:discount)))))))

  ;; graal sf1 158ms
  (criterium.core/quick-bench
    (loop [sum-qty 0.0
           sum-base-price 0.0
           sum-disc-price 0.0
           xs (seq (:lineitem dataset))]
      (if-some [h (first xs)]
        (if (<= (compare (:shipdate h) #inst "1998-09-02") 0)
          (recur (+ sum-qty (:quantity h))
                 (+ sum-base-price (:extendedprice h))
                 (+ sum-disc-price (* (:extendedprice h) (- 1.0 (:discount h))))
                 (rest xs))
          (recur sum-qty sum-base-price sum-disc-price (rest xs)))
        [sum-qty sum-base-price sum-disc-price])))

  ;; graal sf1 39.945584 ms
  (criterium.core/quick-bench
    (let [iter (.iterator ^Iterable (:lineitem dataset))]
      (loop [sum-qty 0.0
             sum-base-price 0.0
             sum-disc-price 0.0]
        (if (.hasNext iter)
          (let [^Lineitem h (.next iter)
                shipdate (.-shipdate h)]
            (if (<= (compare shipdate #inst "1998-09-02") 0)
              (let [q (.-quantity h)
                    ep (.-extendedprice h)
                    d (.-discount h)]
                (recur (+ sum-qty q)
                       (+ sum-base-price ep)
                       (+ sum-disc-price (* ep (- 1.0 d)))))
              (recur sum-qty sum-base-price sum-disc-price)))
          [sum-qty sum-base-price sum-disc-price]))))

  (def ^longs shipdate (long-array (map (comp inst-ms :shipdate) (:lineitem dataset))))
  (def ^doubles quantity (double-array (map :quantity (:lineitem dataset))))
  (def ^doubles extendedprice (double-array (map :extendedprice (:lineitem dataset))))
  (def ^doubles discount (double-array (map :discount (:lineitem dataset))))



  ;; sf1 graal 7.740897 ms
  (defn columnar-loop [^longs shipdate
                       ^doubles quantity
                       ^doubles extendedprice
                       ^doubles discount]
    (let [sd (long (inst-ms #inst "1998-09-02"))
          n (alength discount)]
      (loop [i 0
             sum-qty 0.0
             sum-base-price 0.0
             sum-disc-price 0.0]
        (if (< i n)
          (let [s (aget shipdate i)]
            (if (<= s sd)
              (let [q (aget quantity i)
                    ep (aget extendedprice i)
                    d (aget discount i)]
                (recur (unchecked-inc i)
                       (+ sum-qty q)
                       (+ sum-base-price ep)
                       (+ sum-disc-price (* ep (- 1.0 d)))))
              (recur (unchecked-inc i) sum-qty sum-base-price sum-disc-price)))
          [sum-qty sum-base-price sum-disc-price]))))

  (criterium.core/quick-bench (columnar-loop shipdate quantity extendedprice discount))

  (do
    (set! *warn-on-reflection* true)
    (defn stride-cl []
      (let [n (count (:lineitem dataset))
            shipdate (long-array n)
            quantity (double-array n)
            extendedprice (double-array n)
            discount (double-array n)
            iter (.iterator ^Iterable (:lineitem dataset))]
        (loop [i 0]
          (when (.hasNext iter)
            (let [^Lineitem h (.next iter)
                  ^Date s (.-shipdate h)
                  q (.-quantity h)
                  ep (.-extendedprice h)
                  d (.-discount h)]
              (aset shipdate i (.getTime s))
              (aset quantity i q)
              (aset extendedprice i ep)
              (aset discount i d)
              (recur (unchecked-inc i)))))
        (columnar-loop shipdate quantity extendedprice discount))))

  ;; 50ms (43ms in striding)
  (criterium.core/quick-bench (stride-cl))

  (defn pack-dynamic-buffer [t coll]
    (let [ts 8
          buf (ByteBuffer/allocateDirect (* ts (count coll)))]
      (case t
        :long (doseq [e coll] (.putLong buf (long e)))
        :double (doseq [e coll] (.putDouble buf (long e))))
      (.flip buf)))

  (def dshipdate (pack-dynamic-buffer :long shipdate))
  (def dquantity (pack-dynamic-buffer :double quantity))
  (def dextendedprice (pack-dynamic-buffer :double extendedprice))
  (def ddiscount (pack-dynamic-buffer :double discount))

  (defn buf-columnar-loop
    [^ByteBuffer shipdate
     ^ByteBuffer quantity
     ^ByteBuffer extendedprice
     ^ByteBuffer discount
     n]
    (let [sd (long (inst-ms #inst "1998-09-02"))
          n (int n)]
      (loop [i 0
             sum-qty 0.0
             sum-base-price 0.0
             sum-disc-price 0.0]
        (if (< i n)
          (let [offset (unchecked-multiply 8 i)
                s (.getLong shipdate offset)]
            (if (<= s sd)
              (let [q (.getDouble quantity offset)
                    ep (.getDouble extendedprice offset)
                    d (.getDouble discount offset)]
                (recur (unchecked-inc i)
                       (+ sum-qty q)
                       (+ sum-base-price ep)
                       (+ sum-disc-price (* ep (- 1.0 d)))))
              (recur (unchecked-inc i) sum-qty sum-base-price sum-disc-price)))
          [sum-qty sum-base-price sum-disc-price]))))

  ;; 10.591815 ms, 11ms on heap
  (criterium.core/quick-bench
    (buf-columnar-loop
      (.duplicate dshipdate)
      (.duplicate dquantity)
      (.duplicate dextendedprice)
      (.duplicate ddiscount)
      (count (:lineitem dataset))))

  (def oshipdate (object-array (seq shipdate)))
  (def oshipdate2 (object-array (map :shipdate (:lineitem dataset))))
  (def oquantity (object-array (seq quantity)))
  (def oextendedprice (object-array (seq extendedprice)))
  (def odiscount (object-array (seq discount)))

  (defn boxed-columnar-loop
    [^objects shipdate
     ^objects quantity
     ^objects extendedprice
     ^objects discount
     n]
    (let [sd (long (inst-ms #inst "1998-09-02"))
          n (int n)]
      (loop [i 0
             sum-qty (Long/valueOf 0)
             sum-base-price (Long/valueOf 0)
             sum-disc-price (Long/valueOf 0)]
        (if (< i n)
          (let [s (aget shipdate i)]
            (if (s sd)
              (let [q (aget quantity i)
                    ep (aget extendedprice i)
                    d (aget discount i)]
                (recur (unchecked-inc i)
                       (+ sum-qty q)
                       (+ sum-base-price ep)
                       (+ sum-disc-price (* ep (- 1.0 d)))))
              (recur (unchecked-inc i) sum-qty sum-base-price sum-disc-price)))
          [sum-qty sum-base-price sum-disc-price]))))

  ;; 60.060846 ms
  (criterium.core/quick-bench
    (boxed-columnar-loop
      oshipdate
      oquantity
      oextendedprice
      odiscount
      (count (:lineitem dataset))))

  (defn boxed-columnar-loop2
    [^objects shipdate
     ^doubles quantity
     ^doubles extendedprice
     ^doubles discount
     n]
    (let [sd (inst-ms #inst "1998-09-02")
          n (int n)]
      (loop [i 0
             sum-qty 0.0
             sum-base-price 0.0
             sum-disc-price 0.0]
        (if (< i n)
          (let [s (aget shipdate i)]
            (if (<= (.getTime ^java.util.Date s) 904694400000)
              (let [q (aget quantity i)
                    ep (aget extendedprice i)
                    d (aget discount i)]
                (recur (unchecked-inc i)
                       (+ sum-qty q)
                       (+ sum-base-price ep)
                       (+ sum-disc-price (* ep (- 1.0 d)))))
              (recur (unchecked-inc i) sum-qty sum-base-price sum-disc-price)))
          [sum-qty sum-base-price sum-disc-price]))))

  ;; 208.009596 ms (all boxed)
  ;; 79.153263 ms (just shipdate as a Date, clj compare)
  ;; 63.627457 ms (unboxing date on the fly)
  (criterium.core/quick-bench
    (boxed-columnar-loop2
      oshipdate2
      quantity
      extendedprice
      discount
      (count (:lineitem dataset))))

  (defrecord DoublePair [^double a ^double b])
  (def double-pairs
    (let [n 10000000]
      (->> (map ->DoublePair (repeatedly n #(rand)) (repeatedly n #(rand)))
           object-array)))

  (def ^java.nio.ByteBuffer doubles-packed
    (let [buf (java.nio.ByteBuffer/allocateDirect (* 10000000 16))]
      (run! (fn [^DoublePair p] (.putDouble buf (.-a p)) (.putDouble buf (.-b p))) double-pairs)
      (.flip buf)))

  (def ^longs doubles-packed-longs
    (let [buf (long-array (* 10000000 2))]
      (dotimes [i (alength double-pairs)]
        (let [^DoublePair p (aget double-pairs i)]
          (aset buf (* 2 i) (Double/doubleToRawLongBits (.-a p)))
          (aset buf (inc (* 2 i)) (Double/doubleToRawLongBits (.-b p)))))
      buf))

  ;; 10 mil, java8 (parallel gc) 52.520798 ms
  ;; graal 227.738447 ms
  ;; jdk17 154.200645 ms
  (clj-async-profiler.core/serve-ui 5000)
  (clojure.java.browse/browse-url "http://localhost:5000")

  (defn rmqdoubs []
    (first
      (com.wotbrew.cinq.row-major/q
        [^DoublePair l double-pairs
         :group []]
        ($sum (+ l:a l:b)))))

  (clj-async-profiler.core/profile
    (criterium.core/quick-bench (rmqdoubs))
    )

  (criterium.core/quick-bench
    (first
      (com.wotbrew.cinq.array-seq/q
        [^DoublePair l double-pairs
         :group []]
        ($sum (+ l:a l:b)))))

  ;; optimal iter 16.261836 ms
  ;; graal 10ms
  (criterium.core/quick-bench
    (let [iter (clojure.lang.ArrayIter/create double-pairs)]
      (loop [sum 0.0]
        (if (.hasNext iter)
          (let [^DoublePair h (.next iter)]
            (recur (+ sum (+ (.-a h) (.-b h)))))
          sum))))

  ;; quick box
  ;; graal 22ms
  (criterium.core/quick-bench
    (let [iter (clojure.lang.ArrayIter/create double-pairs)]
      (loop [sum 0.0]
        (if (.hasNext iter)
          (let [h (.next iter)]
            (recur (+ sum (+ (:a h) (:b h)))))
          sum))))

  ;; optimal array 16.673321 ms
  ;; graal 15ms
  (criterium.core/quick-bench
    (let [^objects pairs double-pairs]
      (loop [i 0
             sum 0.0]
        (if (< i (alength pairs))
          (let [^DoublePair h (aget pairs i)]
            (recur (unchecked-inc i) (+ sum (+ (.-a h) (.-b h)))))
          sum))))

  ;; fast packed 12.361953 ms (not as good as you might think!)
  ;; graal 9.5ms
  (criterium.core/quick-bench
    (let [^java.nio.ByteBuffer pairs (.duplicate doubles-packed)]
      (loop [i 0
             sum 0.0]
        (if (< i 10000000)
          (let [a (.getDouble pairs)
                b (.getDouble pairs)]
            (recur (unchecked-inc i) (+ sum (+ a b))))
          sum))))

  ;; Unsafe ptr magic does not seem any better: graal 9.425494 ms
  (criterium.core/quick-bench
    (let [^sun.nio.ch.DirectBuffer pairs (.duplicate doubles-packed)
          addr (.address pairs)
          max-addr (+ addr 160000000)
          unsafe (jdk.internal.misc.Unsafe/getUnsafe)]
      (loop [addr addr
             sum 0.0]
        (if (< addr max-addr)
          (let [a (.getDouble unsafe addr)
                b (.getDouble unsafe (unchecked-add addr 8))]
            (recur (unchecked-add addr 16) (+ sum (+ a b))))
          sum))))

  (defn pl-sum [^longs pairs]
    (loop [i 0
           sum 0.0]
      (if (< i 10000000)
        (let [a (aget pairs (unchecked-multiply-int i 2))
              b (aget pairs (unchecked-inc (unchecked-multiply-int i 2)))]
          (recur (unchecked-inc i) (+ sum (+ (Double/longBitsToDouble a) (Double/longBitsToDouble b)))))
        sum)))

  ;; graal 10.135512 ms
  (criterium.core/quick-bench (pl-sum doubles-packed-longs))

  )

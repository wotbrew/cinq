(ns scratch)

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
                (recur (unchecked-inc-int n) (unchecked-inc-int i))
                (recur n (unchecked-inc-int i))))
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
                (recur (unchecked-inc-int n) (unchecked-inc-int i))
                (recur n (unchecked-inc-int i))))
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
                (recur (unchecked-inc-int n) (unchecked-inc-int i))
                (recur n (unchecked-inc-int i))))
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
  ;; graal 0.05 4.8ms
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

  ;; sf1 4.7sec
  ;; graal 0.05 103.ms
  (criterium.core/quick-bench
    (loop [sum-qty 0.0
           sum-base-price 0.0
           sum-disc-price 0.0
           xs (set (:lineitem dataset))]
      (if-some [h (first xs)]
        (if (<= (compare (:shipdate h) #inst "1998-09-02") 0)
          (recur (+ sum-qty (:quantity h))
                 (+ sum-base-price (:extendedprice h))
                 (+ sum-disc-price (* (:extendedprice h) (- 1.0 (:discount h))))
                 (rest xs))
          (recur sum-qty sum-base-price sum-disc-price (rest xs)))
        [sum-qty sum-base-price sum-disc-price])))

  (defrecord DoublePair [^double a ^double b])
  (def double-pairs
    (let [n 10000000]
      (->> (map ->DoublePair (repeatedly n #(rand)) (repeatedly n #(rand)))
           object-array)))

  (def ^java.nio.ByteBuffer doubles-packed
    (let [buf (java.nio.ByteBuffer/allocateDirect (* 10000000 16))]
      (run! (fn [^DoublePair p] (.putDouble buf (.-a p)) (.putDouble buf (.-b p))) double-pairs)
      (.flip buf)))

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
    (criterium.core/quick-bench (rmqdoubs)))

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
  ;; graal 10ms
  (criterium.core/quick-bench
    (let [^objects pairs double-pairs]
      (loop [i 0
             sum 0.0]
        (if (< i (alength pairs))
          (let [^DoublePair h (aget pairs i)]
            (recur (unchecked-inc-int i) (+ sum (+ (.-a h) (.-b h)))))
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
            (recur (unchecked-inc-int i) (+ sum (+ a b))))
          sum))))


  )

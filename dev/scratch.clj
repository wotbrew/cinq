(ns scratch)

(require 'criterium.core)
(require 'clj-async-profiler.core)

(comment

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

  ;; adding a box (and clojure compare), no date specialisation
  ;; 8.516412 ms
  (let [a (object-array (:lineitem dataset))]
    (criterium.core/quick-bench
      (let [^objects a a]
        (loop [n 0
               i 0]
          (if (< i (alength a))
            (let [^Lineitem o (aget a i)
                  shipdate (.-shipdate o)]
              (if (<= (compare shipdate #inst "1998-09-02") 0)
                (recur (unchecked-inc-int n) (unchecked-inc-int i))
                (recur n (unchecked-inc-int i))))
            n)))))

  ;; using a vector Iter 10.517340 ms
  (criterium.core/quick-bench
    (iter-count
      (com.wotbrew.cinq.row-major/q
        [^Lineitem l (:lineitem dataset)
         :when (<= l:shipdate #inst "1998-09-02")]
        l)))

  ;; using a vector Iter 34.669596 ms
  (criterium.core/quick-bench
    (iter-count
      (com.wotbrew.cinq.vector-seq/q*
        [^Lineitem l (:lineitem dataset)
         :when (<= l:shipdate #inst "1998-09-02")]
        l)))

  )

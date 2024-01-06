(ns com.wotbrew.cinq.plan-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq.plan :refer :all]))

(deftest rewrite-test
  (are [plan expected-plan] (= expected-plan (rewrite {} *ns* plan))

    '(com.wotbrew.cinq/scan [1, 2, 3] [:cinq/self])
    ;; =>
    '(com.wotbrew.cinq/scan [1, 2, 3] [:cinq/self])


    '(->
       (com.wotbrew.cinq/scan [1 2 3] [:cinq/self])
       (com.wotbrew.cinq/dependent-join
         1
         (com.wotbrew.cinq/tfn
           {a 0}
           (clojure.core/->
             (com.wotbrew.cinq/scan [1 2 3] [:cinq/self])
             (com.wotbrew.cinq/where (com.wotbrew.cinq/tfn {b 0} (= a b)))))))
    ;; =>
    '(com.wotbrew.cinq/equi-join
       (com.wotbrew.cinq/scan
         [1
          2
          3]
         [:cinq/self])
       (com.wotbrew.cinq/tfn
         {a__cinq__1 0}
         [a__cinq__1])
       (com.wotbrew.cinq/scan
         [1
          2
          3]
         [:cinq/self])
       (com.wotbrew.cinq/tfn
         {b__cinq__0 0}
         [b__cinq__0])
       (com.wotbrew.cinq/theta-tfn
         {}
         {}
         true))))

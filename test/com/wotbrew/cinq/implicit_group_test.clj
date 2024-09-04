(ns com.wotbrew.cinq.implicit-group-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest groups-are-added-for-top-level-project-test
  (is (= [4] (vec (c/q [a [1 2 3 4]] (c/count)))))
  (is (= [3] (vec (c/q [a [1 2 3 nil]] (c/count a)))))
  (is (= [3] (vec (c/q [a [1 2]] (c/sum a)))))
  (is (= [1] (vec (c/q [a [2 1]] (c/min a)))))
  (is (= [2] (vec (c/q [a [1 2]] (c/max a)))))
  (is (= [3/2] (vec (c/q [a [1 2]] (c/avg a))))))

(deftest groups-added-to-let-test
  (is (= [1] (vec (c/q [a [] :let [n (+ 1 (c/count))]] n)))))

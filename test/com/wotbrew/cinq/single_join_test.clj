(ns com.wotbrew.cinq.single-join-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest equi-single-re-test
  (is (= [[1 1] [2 2] [3 3]])
      (vec (c/q [a [1, 2, 3]] [a (c/scalar [b [1, 2, 3] :when (= a b)] b)]))))

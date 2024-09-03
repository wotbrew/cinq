(ns com.wotbrew.cinq.distinct-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest distinct-test
  (is (= 1 (c/rel-count (c/q [a [1 2 3 4 5 5 5] :distinct [a] :limit 1] a))))
  (is (= [1 2 3] (vec (c/q [a [1 1 1 2 2 3] :distinct [a]] a)))))

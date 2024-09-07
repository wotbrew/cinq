(ns com.wotbrew.cinq.distinct-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest distinct-test
  (is (= 1 (c/rel-count (c/rel [a [1 2 3 4 5 5 5] :distinct [a] :limit 1] a))))
  (is (= [1 2 3] (vec (c/rel [a [1 1 1 2 2 3] :distinct [a]] a))))
  (is (= [2 1] (vec (c/rel [a [{:k 1 :n 2} {:k 1 :n 1} {:k 2 :n 1}]
                          :distinct [(even? a:k)]]
                           a:n)))))

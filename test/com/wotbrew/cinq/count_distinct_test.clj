(ns com.wotbrew.cinq.count-distinct-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest count-distinct-test
  (is (= [2] (vec (c/q [a [1 1 2 nil nil 1 1 2]] (c/count-distinct a)))))
  (is (= [2] (vec (c/q [a [{:k 0} {:k 1} {:k 1}]] (c/count-distinct (even? (:k a)))))))
  (is (= [1] (vec (c/q [a [1 2 nil 1]] (c/count-distinct 1))))))


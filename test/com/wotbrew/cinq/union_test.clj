(ns com.wotbrew.cinq.union-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest union-test
  (is (= [] (vec (c/union))))
  (is (= [1] (vec (c/union [1]))))
  (is (= [1, 2, 1] (vec (c/union [1, 2] [1]))))
  (is (= [1, 2, 1] (vec (c/union [1, 2] (c/q [a [1]] a)))))
  (is (= [1, 2, "1"] (vec (c/union [1, 2] (c/q [a [1]] (str a)))))))

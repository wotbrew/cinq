(ns com.wotbrew.cinq.first-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest rel-first-test
  (is (= nil (c/rel-first [])))
  (is (= nil (c/rel-first (c/q [a []] a))))
  (is (= 1 (c/rel-first [1 2 3])))
  (is (= 0 (c/rel-first (c/q [a (map (fn [i] (if (= 42 i) (throw (Exception. "foo")) i)) (range 1e6))] a))))
  (is (= 0 (c/scalar [a (map (fn [i] (if (= 42 i) (throw (Exception. "foo")) i)) (range 1e6))] a))))

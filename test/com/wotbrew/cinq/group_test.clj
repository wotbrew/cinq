(ns com.wotbrew.cinq.group-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.column]))

(deftest accessible-as-coll-test
  (is (= [1 2 3] (c/rel-first (c/q [a [1 2 3] :group []] (vec a)))))
  (is (= [[1 3] [2]] (vec (c/q [a [1 2 3] :group [even (even? a)] :order [even :asc]] (vec a))))))

(deftest type-is-column-test
  (is (instance? com.wotbrew.cinq.column.Column (c/rel-first (c/q [a [] :group []] a)))))

(deftest can-mix-aggregate-col-test
  (is (= [[1 2 3] 6] (c/rel-first (c/q [a [1 2 3] :group []] [(vec a) (c/sum a)])))))


(ns com.wotbrew.cinq.null-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest nil-ordering-guarantees-test
  ;; todo prop with ordering on/off equiv
  (is (= [[3 {:a 3, :s "fr"}]]
         (vec (c/rel [a [1, 2, 3]
                      b [{:a 1 :s "he"}, {:a 2 :s nil}, {:a 3 :s "fr"}]
                      :when (and (= a b:a) (not= a 2) (= "fr" (subs b:s 0 2)))]
                [a b])))))

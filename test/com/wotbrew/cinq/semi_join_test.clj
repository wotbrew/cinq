(ns com.wotbrew.cinq.semi-join-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest semi-join-test
  (is (= []
         (vec (c/q
                [a []
                 :when (c/scalar [b []]
                         true)]
                true))))

  (is (= []
         (vec (c/q
                [a [1]
                 :when (c/scalar [b []]
                         true)]
                true))))

  (is (= [true]
         (vec (c/q
                [a [1]
                 :when (c/scalar [b [1]]
                         true)]
                true))))

  (is (= [true]
         (vec
           (c/q
             [a [1]
              :when (c/scalar [b [1]
                               :when (= a b)]
                      true)]
             true))))

  (is (= [2]
         (vec
           (c/q [a [1, 2]
                 :when (not (c/scalar [b [1] :when (= a b)] true))]
             a)))))

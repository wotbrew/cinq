(ns com.wotbrew.cinq.semi-join-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest semi-join-test
  (is (= []
         (c/vec
           [a []
            :when (c/scalar [b []]
                    true)]
           true)))

  (is (= []
         (c/vec
           [a [1]
            :when (c/scalar [b []]
                    true)]
           true)))

  (is (= [true]
         (c/vec
           [a [1]
            :when (c/scalar [b [1]]
                    true)]
           true)))

  (is (= [true]
         (c/vec
           [a [1]
            :when (c/scalar [b [1]
                             :when (= a b)]
                    true)]
           true)))

  (is (= [2] (c/vec [a [1, 2] :when (not (c/scalar [b [1] :when (= a b)] true))] a)))

  )

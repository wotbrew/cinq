(ns com.wotbrew.cinq.semi-join-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest semi-join-test
  (is (= []
         (vec (c/rel
                [a []
                 :when (c/exists? [b []])]
                true))))

  (is (= []
         (vec (c/rel
                [a [1]
                 :when (c/exists? [b []])]
                true))))

  (is (= [true]
         (vec (c/rel
                [a [1]
                 :when (c/exists? [b [1]])]
                true))))

  (is (= [true]
         (vec
           (c/rel
             [a [1]
              :when (c/exists? [b [1] :when (= a b)])]
             true))))

  (is (= [2]
         (vec
           (c/rel [a [1, 2]
                 :when (not (c/exists? [b [1] :when (= a b)]))]
                  a))))

  (is (= [1]
         (vec
           (c/rel [a [1]
                 :when (c/exists? [b [a]])]
                  a))))

  (is (= []
         (vec
           (c/rel [a [1]
                 :when (not (c/exists? [b [a]]))]
                  a))))


  )

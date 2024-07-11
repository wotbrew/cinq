(ns com.wotbrew.cinq.relvar-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest relvar-test
  (let [rv1 (c/relvar)
        rv2 (c/relvar)]

    (c/rel-set rv1 [1, 2, 3, 4])
    (c/rel-set rv2 [2, 4, 5, 6])

    (is (= [2, 4] (vec (c/q [x rv1
                             y rv2
                             :when (= x y)]
                         x))))

    (is (= 4 (c/insert rv1 5)))
    (is (= 5 (c/insert rv1 5)))

    (is (= [1, 2, 3, 4, 5, 5] (vec rv1)))

    (c/delete-where rv1 [x] (= x 42))

    (is (= [1, 2, 3, 4, 5, 5] (vec rv1)))

    (c/delete-where rv1 [x] (= x 5))

    (is (= [1, 2, 3, 4] (vec rv1)))

    (c/update-where rv1 [x] (even? x) 42)

    (is (= {1 1, 42 2, 3 1} (frequencies rv1)))

    (testing "STM works with in memory relvars"
      (try
        (dosync
          (c/insert rv1 3)
          (is (= {1 1, 42 2, 3 2} (frequencies rv1)))
          (throw (ex-info "fail tx" {})))
        (catch Throwable _))

      (is (= {1 1, 42 2, 3 1} (frequencies rv1))))))

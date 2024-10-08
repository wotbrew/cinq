(ns com.wotbrew.cinq.relvar-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest relvar-test
  (let [rv1 (c/relvar)
        rv2 (c/relvar)]

    (c/rel-set rv1 [1 2 3 4])
    (c/rel-set rv2 [2 4 5 6])

    (is (= [2 4] (vec (c/rel [x rv1
                              y rv2
                              :when (= x y)]
                        x))))

    (is (= 4 (c/insert rv1 5)))
    (is (= 5 (c/insert rv1 5)))

    (is (= [1 2 3 4 5 5] (vec rv1)))

    (c/run [x rv1 :when (= 42 x)] (c/delete x))

    (is (= [1 2 3 4 5 5] (vec rv1)))

    (c/run [x rv1 :when (= 5 x)] (c/delete x))

    (is (= [1 2 3 4] (vec rv1)))

    (c/run [x rv1 :when (even? x)] (c/replace x 42))

    (is (= {1 1, 42 2, 3 1} (frequencies rv1)))

    (testing "STM works with in memory relvars"
      (try
        (dosync
          (c/insert rv1 3)
          (c/rel-set rv2 [])
          (is (= {1 1, 42 2, 3 2} (frequencies rv1)))
          (is (= [] (vec rv2)))
          (throw (ex-info "fail tx" {})))
        (catch Throwable _))

      (is (= {1 1, 42 2, 3 1} (frequencies rv1)))
      (is (= [2 4 5 6] (vec rv2))))))

(deftest index-test
  (let [foo (c/relvar)
        idx (c/index foo :id)]

    (c/rel-set foo [])
    (is (= [] (vec (idx 42))))
    (is (= [] (vec (get idx 42))))

    (c/rel-set foo [{:id 42}])
    (is (= [{:id 42}] (vec (idx 42))))
    (is (= [{:id 42}] (vec (get idx 42))))

    (c/insert foo {:id 42})
    (is (= [{:id 42} {:id 42}] (vec (idx 42))))

    (c/insert foo {:id 43})
    (c/insert foo {:id 44})
    (c/insert foo {:id 45})

    (is (= [{:id 42} {:id 42}] (vec (idx 42))))
    (is (= [{:id 43}] (vec (idx 43))))
    (is (= [{:id 44} {:id 45}] (vec (c/range idx > 43))))
    (is (= [{:id 42} {:id 42} {:id 43}] (vec (c/range idx <= 43))))

    (is (= [{:id 43}] (vec (c/range idx > 42 < 44))))

    (c/run [f (idx 42) :limit 1] (c/delete f))

    (is (= [42] (vec (c/rel [f (get idx 42)] f:id))))
    (is (= [42] (vec (c/rel [f (c/range idx > 40) :when (= f:id 42)] f:id))))
    (is (= [42] (vec (c/rel [f (get idx 42) :when (< f:id 43)] f:id))))))

(deftest sorted-scan-test
  (let [foo (c/relvar)
        idx (c/index foo :a)]
    (c/rel-set foo [{:a 3} {:a 4} {:a 2}])

    (is (= [{:a 4} {:a 3} {:a 2}] (vec (c/desc idx))))
    (is (= [{:a 2} {:a 3} {:a 4}] (vec (c/asc idx))))

    (is (= [{:a 4}] (vec (c/top-k idx 1))))
    (is (= [{:a 4}, {:a 3}] (vec (c/top-k idx 2))))
    (is (= [{:a 4}, {:a 3}, {:a 2}] (vec (c/top-k idx 3))))
    (is (= [{:a 4}, {:a 3}, {:a 2}] (vec (c/top-k idx 4))))
    (is (= [{:a 2} {:a 3}] (vec (c/bottom-k idx 2))))

    (let [a-str (str/join "" (concat (repeat 512 "a") ["a"]))
          b-str (str/join "" (concat (repeat 512 "a") ["b"]))
          c-str (str/join "" (concat (repeat 512 "a") ["c"]))]

      (c/rel-set foo [{:a a-str}
                      {:a c-str}
                      {:a b-str}])

      (is (= [{:a a-str}] (vec (c/bottom-k idx 1))))
      (is (= [{:a a-str} {:a b-str} {:a c-str}] (vec (c/bottom-k idx 3))))
      (is (= [{:a a-str} {:a b-str} {:a c-str}] (vec (c/asc idx))))
      (is (= [{:a c-str} {:a b-str} {:a a-str}] (vec (c/desc idx))))
      (is (= [{:a c-str}] (vec (c/top-k idx 1)))))))

(deftest big-count-test
  (let [x (c/relvar)]
    (is (= 0 (c/rel-count x)))
    (c/insert x 0)
    (is (= 1 (c/rel-count x)))
    (c/rel-set x [])
    (is (= 0 (c/rel-count x)))))

(deftest auto-id-test
  (let [x (c/relvar)]
    (is (nil? (c/auto-id x :id)))
    (is (= 0 (c/insert x {})))
    (is (= {:id 0} (c/rel-first x)))
    (is (= 1 (c/insert x {})))
    (is (= [{:id 0} {:id 1}] (vec x)))
    (is (= 2 (c/insert x {:id 42})))
    (is (= [{:id 0} {:id 1} {:id 42}] (vec x)))))

(deftest update-test
  (let [x (c/relvar)]
    (c/rel-set x [1 2 3])
    (c/run [x x] (c/update x inc))
    (is (= [2 3 4] (vec x)))
    (c/run [x x] (c/update x + 42))
    (is (= [44 45 46] (vec x)))))

(deftest put-test
  (let [x (c/relvar)]
    (c/index x :id)
    (is (nil? (c/put x :id 42 {:n 0})))
    (is (nil? (c/put x :id 43 {:n 0})))
    (is (= [{:id 42, :n 0} {:id 43, :n 0}] (vec x)))
    (is (= {:id 42, :n 0} (c/put x :id 42, {:n 1})))
    (is (= {{:id 42, :n 1} 1 {:id 43, :n 0} 1} (frequencies x)))
    (c/insert x {:id 42, :n 2})
    (is (= {{:id 42, :n 1} 1
            {:id 42, :n 2} 1
            {:id 43, :n 0} 1}
           (frequencies x)))
    (c/put x :id 42 {:n 3})
    (is (= {{:id 42, :n 3} 2
            {:id 43, :n 0} 1}
           (frequencies x)))))

(deftest del-key-test
  (let [x (c/relvar)]
    (c/index x :id)
    (is (= 0 (c/del-key x :id 42)))
    (c/rel-set x [{:id 42} {:id 43}])
    (is (= 1 (c/del-key x :id 42)))
    (is (= [{:id 43}] (vec x)))
    (c/rel-set x [{:id 42} {:id 42} {:id 43}])
    (is (= 2 (c/del-key x :id 42)))
    (is (= [{:id 43}] (vec x)))))

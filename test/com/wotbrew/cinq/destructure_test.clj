(ns com.wotbrew.cinq.destructure-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq.parse :as parse]))

(deftest destructure-test
  (are [a b]
    (= (quote b) (parse/normalize-binding (quote a)))

    {} []
    [] []
    a  [[a :cinq/self]]

    {:keys []} []
    {:keys [a]} [[a :a]]
    {:keys [:a]} [[a :a]]
    {:keys [a, b]} [[a :a] [b :b]]
    {:keys [a/b]} [[b :a/b]]
    {:keys [:a/b]} [[b :a/b]]

    {a :a} [[a :a]]
    {a :b, c :d} [[a :b] [c :d]]
    {x 42} [[x 42]]
    {x [1 2]} [[x [1 2]]]

    {:strs []} []
    {:strs [a]} [[a "a"]]

    {a :a :as b} [[a :a] [b :cinq/self]]

    [a] [[a 0]]
    [_ b] [[_ 0] [b 1]]
    [_ b :as c] [[c :cinq/self] [_ 0] [b 1]]))

(deftest test-composite-vec-disabled-test
  (is (thrown? Exception (parse/normalize-binding '[[a b]]))))

(deftest test-composite-destructure-disabled-test
  (is (thrown? Exception (parse/normalize-binding '{[x, y] :a}))))

(deftest test-lookup-sym-dest-test
  (is (thrown? Exception (parse/normalize-binding 'a:b)))
  (is (thrown? Exception (parse/normalize-binding '{:keys [a:b]}))))


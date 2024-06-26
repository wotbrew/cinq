(ns com.wotbrew.cinq.nio-codec-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq.nio-codec :as codec]))

(def st (codec/empty-symbol-table))

(defn cmp [test-val index-val key]
  (let [index-buffer (codec/encode-heap index-val st true)
        key-int (codec/intern-symbol st key true)
        test-buffer (codec/encode-heap test-val st false)]
    (codec/bufcmp-ksv test-buffer index-buffer key-int)))

(deftest bufcmp-ksv-test
  (is (neg? (cmp nil nil :a)))
  (is (= 0 (cmp nil {:a nil} :a)))
  (is (neg? (cmp nil {:a 42} :a)))
  (is (pos? (cmp 44 {:a 42} :a)))
  (is (= 0 (cmp 44 {:a 44} :a)))
  (is (neg? (cmp 42 {:a 44, :b 42} :a)))
  (is (= 0 (cmp 42 {:a 44, :b 42} :b)))
  (is (neg? (cmp 42 {:a 44, :b 42} :c)))
  (is (= 0 (cmp {} {:a {}} :a)))
  (is (not= 0 (cmp {} {:a nil} :a)))
  (is (not= 0 (cmp {} {:a {:b 42}} :a)))
  (is (not= 0 (cmp {:b 42, :c 43} {:a {:b 42}} :a)))
  (is (= 0 (cmp {:b 42} {:a {:b 42}} :a)))
  ;; unordered
  (is (= 0 (cmp (array-map :b 42, :c 43) {:a (array-map :c 43, :b 42)} :a))))

(ns com.wotbrew.cinq.nio-codec-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq.nio-codec :as codec])
  (:import (clojure.lang Util)
           (java.nio ByteBuffer)))

(def st (codec/empty-symbol-table))

(defn cmp [test-val index-val key]
  (let [index-buffer (codec/encode-heap index-val st true)
        key-int (codec/intern-symbol st key true)
        test-buffer (codec/encode-heap test-val st false)]
    (codec/bufcmp-ksv test-buffer index-buffer key-int (codec/symbol-list st))))

(deftest bufcmp-ksv-test
  (is (= nil (cmp nil nil :a)))
  (is (= nil (cmp nil {:a nil} :a)))
  (is (= nil (cmp nil {:a 42} :a)))
  (is (pos? (cmp 44 {:a 42} :a)))
  (is (= 0 (cmp 44 {:a 44} :a)))
  (is (neg? (cmp 42 {:a 44, :b 42} :a)))
  (is (= 0 (cmp 42 {:a 44, :b 42} :b)))
  (is (= nil (cmp 42 {:a 44, :b 42} :c)))
  (is (= 0 (cmp {} {:a {}} :a)))
  (is (not= 0 (cmp {} {:a nil} :a)))
  (is (not= 0 (cmp {} {:a {:b 42}} :a)))
  (is (not= 0 (cmp {:b 42, :c 43} {:a {:b 42}} :a)))
  (is (= 0 (cmp {:b 42} {:a {:b 42}} :a)))
  ;; unordered
  (is (= 0 (cmp (array-map :b 42, :c 43) {:a (array-map :c 43, :b 42)} :a))))

(deftest coding-test
  (are [test-val]
    (let [st (codec/empty-symbol-table)
          tv test-val]
      (and
        (Util/equals tv (codec/decode-object (codec/encode-heap tv nil false) (codec/symbol-list st)))
        (Util/equals tv (codec/decode-object (codec/encode-heap tv st true) (codec/symbol-list st)))))

    nil
    true
    false
    42
    -42
    3.14
    -3.14
    Double/POSITIVE_INFINITY
    Double/NEGATIVE_INFINITY
    "hello"
    :hello
    :a/hello
    'hello
    'a/hello
    {}
    {{42 43} {44 45}}
    {:hello 42}
    [1, 2, "hello", nil, [false]]
    '(1 2 3)
    #inst "2024-01-04"
    #uuid"7e2a70b7-4c20-43ff-951b-c9b5ab77167b"
    ;; bigmap
    (zipmap (range 64) (repeat "abcdefghjik"))

    ))

(defn rt [x]
  (codec/decode-object (codec/encode-heap x nil nil) nil))

(deftest nan-test
  (is (Double/isNaN (rt Double/NaN))))

(defn size-correct? [^ByteBuffer buf]
  (= (.remaining buf) (codec/next-object-size buf)))

(deftest size-test
  (are [x]
    (and (size-correct? (codec/encode-heap x nil nil))
         (size-correct? (codec/encode-heap x (codec/empty-symbol-table) true)))

    nil
    true
    false
    42
    -42
    3.14
    -3.14
    Double/POSITIVE_INFINITY
    Double/NEGATIVE_INFINITY
    "hello"
    :hello
    :a/hello
    'hello
    'a/hello
    {}
    {{42 43} {44 45}}
    {:hello 42}
    [1, 2, "hello", nil, [false]]
    '(1 2 3)

    #inst "2024-01-04"
    #uuid"7e2a70b7-4c20-43ff-951b-c9b5ab77167b"
    (zipmap (range 64) (repeat "abcdefghjik"))

    ))

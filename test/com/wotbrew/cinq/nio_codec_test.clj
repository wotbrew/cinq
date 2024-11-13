(ns com.wotbrew.cinq.nio-codec-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.generators :as gen]
            [clojure.walk :as walk]
            [com.gfredericks.test.chuck :as chuck]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
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
    (zipmap (range 64) (repeat "abcdefghjik"))))

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
    (zipmap (range 64) (repeat "abcdefghjik"))))

(deftest focus-test
  (are [map key expected-result expected-val]
    (let [buf (codec/encode-heap map nil false)
          k (codec/encode-heap key nil false)
          focus-result (codec/focus buf k (object-array 0))]
      (and (= expected-result focus-result)
           (or (false? focus-result) (= expected-val (codec/decode-object buf nil)))))

    nil :a false nil
    {} :a false nil
    {:a nil} :b false nil
    {:a nil} :a true nil
    {:a 41, :b 42, :c 43} :b true 42
    ;; for now (consistency with cinq/eq)
    {nil 42} nil false nil

    ;; like ILookup we should 'work' with other types.
    "hello" :a false nil
    ))

(def primitive-gen
  (gen/one-of [gen/small-integer
               ;; TODO re-enable NaN when we scan binary instead
               (gen/double* {:NaN? false})
               gen/string
               gen/keyword
               gen/symbol]))

(def val-gen
  (gen/recursive-gen
    (fn [inner]
      (gen/one-of [(gen/vector inner)
                   (gen/map inner inner)]))
    primitive-gen))

(deftest equality-rule-test
  (checking "all objects that are clojure equal are buf equal 1" (chuck/times 100)
    [x val-gen]
    (is (= (codec/encode-heap x nil false)
           (codec/encode-heap x nil false))))
  (checking "all objects that are clojure equal are buf equal 2" (chuck/times 100)
    [x val-gen
     y val-gen]
    (if (= x y)
      (is (= (codec/encode-heap x nil false)
             (codec/encode-heap y nil false)))
      (is (not= (codec/encode-heap x nil false) (codec/encode-heap y nil false)))))
  (checking "rt equal to original type" (chuck/times 500)
    [x val-gen]
    (is (= x (rt x)))))

(deftest crazy-map-eq-test
  (let [m  {-1 0
           -10 0
           -11 0
           -12 0
           -13 0
           -14 0
           -15 0
           -16 0
           -17 0
           -18 0
           -19 0
           -2 0
           -3 0
           -4 0
           -5 0
           -6 0
           -7 0
           -8 0
           -9 0
           0 0
           1 0
           1.0 0
           10 0
           2 0
           3 0
           4 0
           5 0
           6 0
           7 0
           8 0
           9 0
           [] 0
           {0 0} 0}
        m2 (rt m)]
    (= m2 m)
    (is (= (rt m) m))
    (is (= m (rt m)))))

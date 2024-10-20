(ns com.wotbrew.cinq.cmp-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.generators :as gen]
            [com.gfredericks.test.chuck :as chuck]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.lmdb :as lmdb])
  (:import (java.io File)))

(defmacro prj [expr] `(c/scalar [x# [{}]] ~expr))

(deftest expr-test
  (are [expr result]
    (= result expr)

    (prj (< nil)) nil
    (prj (< 3)) true
    (prj (< nil nil)) nil
    (prj (< nil nil nil)) nil
    (prj (< 3 4)) true
    (prj (< 4 3)) false
    (prj (< 3 3)) false
    (prj (< 2 3 3)) false
    (prj (< 2 3 4)) true
    (prj (< 3 nil)) nil
    (prj (< 2 3 nil)) nil

    (prj (<= nil)) nil
    (prj (<= 3)) true
    (prj (<= nil nil)) nil
    (prj (<= nil nil nil)) nil
    (prj (<= 3 4)) true
    (prj (<= 4 3)) false
    (prj (<= 3 3)) true
    (prj (<= 3 3 4)) true
    (prj (<= 3 nil)) nil
    (prj (<= 2 3 nil)) nil

    (prj (> nil)) nil
    (prj (> 3)) true
    (prj (> nil nil)) nil
    (prj (> nil nil nil)) nil
    (prj (> 3 4)) false
    (prj (> 4 3)) true
    (prj (> 3 3)) false
    (prj (> 3 4 4)) false
    (prj (> 4 3 2)) true
    (prj (> 3 nil)) nil
    ;; applied as repeated bin-op
    (prj (> 2 3 nil)) false
    (prj (> 4 3 nil)) nil

    (prj (>= nil)) nil
    (prj (>= 3)) true
    (prj (>= nil nil)) nil
    (prj (>= nil nil nil)) nil
    (prj (>= 3 4)) false
    (prj (>= 4 3)) true
    (prj (>= 3 3)) true
    (prj (>= 3 4 4)) false
    (prj (>= 4 3 3)) true
    (prj (>= 3 nil)) nil
    ;; applied as repeated bin-op
    (prj (>= 2 3 nil)) false
    (prj (>= 3 3 nil)) nil

    (prj (< ##NaN 1.0)) false
    (prj (<= ##NaN 1.0)) false
    (prj (> ##NaN 1.0)) false
    (prj (>= ##NaN 1.0)) false))

(def inherit-prim
  (gen/one-of [gen/small-integer
               gen/double
               gen/string
               gen/keyword
               gen/symbol]))

(def inherit
  (gen/recursive-gen
    (fn [inner]
      (gen/one-of [(gen/vector inner)
                   (gen/map inner inner)]))
    inherit-prim))

(def lmdb (delay (lmdb/database (File/createTempFile "cmp" ".cinq"))))

(def ^:dynamic *src-count* nil)

(defn- via-lmdb [coll]
  (let [a (c/create @lmdb (keyword (str "test" *src-count*)))]
    (when *src-count* (set! *src-count* (inc *src-count*)))
    (c/rel-set a coll)
    a))

(def ^:dynamic *mode* :coll)

(defn src [coll]
  (case *mode*
    :coll coll
    :relvar (doto (c/relvar) (c/rel-set coll))
    :lmdb (via-lmdb coll)))

(defn each-fixture [f]
  (binding [*src-count* 0]
    (doseq [mode [:coll :relvar :lmdb]]
      (binding [*mode* mode]
        (f)))))

(use-fixtures :each #'each-fixture)

(deftest nil-cmp-nil-test
  (is (= [nil] (c/q [x (src [nil])] (< x nil))))
  (is (= [nil] (c/q [x (src [nil])] (<= x nil))))
  (is (= [nil] (c/q [x (src [nil])] (> x nil))))
  (is (= [nil] (c/q [x (src [nil])] (>= x nil)))))

(deftest all-things-cmp-to-nil-are-nil-test
  (checking "that all things compared to nil are nil" (chuck/times 100)
    [x inherit]
    (is (= [nil] (c/q [x' (src [x])] (< x' nil))))
    (is (= [nil] (c/q [x' (src [x])] (<= x' nil))))
    (is (= [nil] (c/q [x' (src [x])] (> x' nil))))
    (is (= [nil] (c/q [x' (src [x])] (>= x' nil))))
    (is (= [] (c/q [x' (src [x]) :when (< x' nil)] x')))
    (is (= [] (c/q [x' (src [x]) :when (<= x' nil)] x')))
    (is (= [] (c/q [x' (src [x]) :when (> x' nil)] x')))
    (is (= [] (c/q [x' (src [x]) :when (>= x' nil)] x')))))

(def number-gen
  (gen/one-of [(gen/double* {:NaN? false})
               gen/small-integer]))

(deftest numbers-behave-as-clojure-test
  (checking "that numbers behave as clojure test, except NaN!" (chuck/times 100)
    [x number-gen
     y number-gen]
    (let [xs (src [x])
          xsm (src [{:x x}])]
      (is (= [y x :< (< y x)] (c/scalar [x' xs] [y x' :< (< y x')])))
      (is (= [y x :<= (<= y x)] (c/scalar [x' xs] [y x' :<= (<= y x')])))
      (is (= [y x :> (> y x)] (c/scalar [x' xs] [y x' :> (> y x')])))
      (is (= [y x :>= (>= y x)] (c/scalar [x' xs] [y x' :>= (>= y x')])))

      (is (= [y x :< (< y x)] (c/scalar [x' xsm] [y x':x :< (< y x':x)])))
      (is (= [y x :<= (<= y x)] (c/scalar [x' xsm] [y x':x :<= (<= y x':x)])))
      (is (= [y x :> (> y x)] (c/scalar [x' xsm] [y x':x :> (> y x':x)])))
      (is (= [y x :>= (>= y x)] (c/scalar [x' xsm] [y x':x :>= (>= y x':x)])))

      (is (= (for [x' (vec xs) :when (< y x')] [y :< x']) (c/q [x' xs :when (< y x')] [y :< x'])))
      (is (= (for [x' (vec xs) :when (<= y x')] [y :<= x']) (c/q [x' xs :when (<= y x')] [y :<= x'])))
      (is (= (for [x' (vec xs) :when (> y x')] [y :> x']) (c/q [x' xs :when (> y x')] [y :> x'])))
      (is (= (for [x' (vec xs) :when (>= y x')] [y :>= x']) (c/q [x' xs :when (>= y x')] [y :>= x'])))

      (is (= (for [x' (vec xs) :when (< y x')] [y :< x']) (c/q [x' xsm :when (< y x':x)] [y :< x':x])))
      (is (= (for [x' (vec xs) :when (<= y x')] [y :<= x']) (c/q [x' xsm :when (<= y x':x)] [y :<= x':x])))
      (is (= (for [x' (vec xs) :when (> y x')] [y :> x']) (c/q [x' xsm :when (> y x':x)] [y :> x':x])))
      (is (= (for [x' (vec xs) :when (>= y x')] [y :>= x']) (c/q [x' xsm :when (>= y x':x)] [y :>= x':x]))))))

(ns com.wotbrew.cinq.equality-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.generators :as gen]
            [com.gfredericks.test.chuck :as chuck]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.lmdb :as lmdb])
  (:import (clojure.lang Util)
           (com.wotbrew.cinq CinqUtil)
           (java.io File)))

(def inherit-prim
  (gen/one-of [gen/large-integer
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

(def lmdb (delay (lmdb/database (File/createTempFile "equality" ".cinq"))))

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

(deftest nil-eq-nil-test
  (is (= [nil] (c/q [x (src [nil])] (= x nil)))))

(deftest eq-any-nil-test
  (is (not (CinqUtil/eq nil nil)))
  (is (= [] (c/q [x (src [nil]) :when (= x x)] x)))
  (checking "that all things are not equal to nil" (chuck/times 100)
    [x inherit]
    (is (= [] (c/q [x' (src [x]) :when (= x' nil)] x')))
    (is (= [] (c/q [x' (src [x]) :when (= nil x')] x')))
    (is (not (CinqUtil/eq nil x)))
    (is (not (CinqUtil/eq x nil)))))

(deftest eq-base-test
  (checking "that CinqUtil/equals inherits java equality for named types" (chuck/times 100)
    [x inherit
     y inherit]
    (let [xs (src [x])
          ys (src [y])]
      (is (= (for [x' (vec xs)
                   y' (vec ys)
                   :when (= x' y')]
               [x' y'])
             (c/q [x' xs
                   y' ys
                   :when (= x' y')]
               [x' y']))))
    (is (= (Util/equals x y) (CinqUtil/eq x y)))))

(deftest field-eq-test
  (checking "that base equality rules apply to fields" (chuck/times 100)
    [x inherit
     y inherit]
    (let [xs (src [{:a x, :b x}])
          ys (src [{:a y, :b y}])]
      (is (= (for [x' (vec xs)
                   y' (vec ys)
                   :when (and (= (:a x') x)
                              (= (:a y') y)
                              (= (:b x') (:b y')))]
               [x' y'])
             (c/q [x' xs
                   y' ys
                   :when (and (= x':a x)
                              (= y':a y)
                              (= x':b y':b))]
               [x' y']))))))

(deftest nested-map-test
  (testing "native buffer comparison"
    (is (= [42] (c/q [a (src [{:x {:y 42, :z 43}}])
                      :when (= a:x {:z 43, :y 42})]
                    (:y a:x))))))


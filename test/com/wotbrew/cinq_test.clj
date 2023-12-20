(ns com.wotbrew.cinq-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :refer :all]))

(deftest arity-test
  (are [rel expected] (= expected (arity rel))

    (scan nil []) 0
    (scan nil [:foo]) 1
    (scan nil [:foo, :bar]) 2

    (nested-loop-join (scan nil [:foo]) 2 (fn [_])) 3))

(deftest relation?-test
  (are [x expected] (= expected (relation? x))

    nil false
    [] false
    [1 2] false
    {} false
    () false
    (map inc [1 2]) false

    (scan nil []) true
    (where (scan nil []) (fn [_])) true))

(deftest customers-example
  (let [customers [{:customer-id 0, :firstname "Jane", :lastname "Doe"}
                   {:customer-id 1, :firstname "John", :lastname "Smith"}]]

    (are [rel expected] (= expected (tuple-seq rel))

      (scan customers []) [[], []]
      (scan customers [:cinq/self]) (map vector customers)
      (scan customers [:firstname]) [["Jane"] ["John"]]
      (scan customers [:firstname :lastname]) [["Jane" "Doe"] ["John" "Smith"]]

      (-> customers
          (scan [:firstname :lastname])
          (where (fn [{ln 1}] (= ln "Doe"))))
      [["Jane" "Doe"]])))

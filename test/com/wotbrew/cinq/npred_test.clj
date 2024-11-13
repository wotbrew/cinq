(ns com.wotbrew.cinq.npred-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq.nio-codec :as codec]
            [com.wotbrew.cinq.npred :as npred]))

(defn- test-lambda [expr v]
  (let [st (codec/empty-symbol-table)
        buf1 (codec/encode-heap v st true)
        buf2 (codec/encode-heap v nil false)
        p1 (npred/lambda expr st)
        p2 (npred/lambda expr (codec/empty-symbol-table))
        ret1 (p1 buf1)
        ret2 (p2 buf2)]
    (is (= ret1 ret2))
    ret1))

(deftest lambda-test
  (are [expr v result]
    (= result (test-lambda expr v))

    [:= nil] nil nil
    [:= 42] nil nil

    ;; todo :not and nil behaviour? (not nil) == nil?!

    [:= 42] 42 true
    [:= 42] 43 false
    [:not [:= 42]] 43 true
    [:not [:not [:= 42]]] 43 false
    [:= 43] 42 false
    [:< 41] 42 true
    [:< 42] 42 false
    [:<= 42] 42 true
    [:<= 41] 42 true
    [:<= 43] 42 false
    [:> 42] 41 true
    [:> 40] 40 false
    [:>= 40] 40 true
    [:>= 41] 40 true
    [:>= 39] 40 false

    [:= "hello"] "hello" true
    [:= "bar"] "hello" false

    [:get :foo [:= 42]] {} false
    [:get :foo [:= 42]] {:foo 43} false
    [:get :foo [:= 42]] {:foo 42, :bar 43, :baz 44} true
    [:get :foo [:get "hey" [:< 41]]] {:foo {"hey" 42}} true))

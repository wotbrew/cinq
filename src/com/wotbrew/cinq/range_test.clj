(ns com.wotbrew.cinq.range-test
  (:import (com.wotbrew.cinq CinqUtil)))

(defn lt [a b] (CinqUtil/lt a b))
(defn lte [a b] (CinqUtil/lte a b))
(defn gt [a b] (CinqUtil/gt a b))
(defn gte [a b] (CinqUtil/gte a b))

(defn index-test [test-fn]
  (condp identical? test-fn
    < lt
    <= lte
    > gt
    >= gte
    (throw (ex-info "Not a valid range query test, accepted (<, <=, >, >=)" {}))))

(defn clj-test [test-fn]
  (condp identical? test-fn
    lt <
    lte <=
    gt >
    gte >=
    (throw (ex-info "Not a valid range query test, accepted (<, <=, >, >=)" {}))))

(ns com.wotbrew.cinq.range-test
  (:import (com.wotbrew.cinq CinqUtil)))

(defn lt [a b] (< (CinqUtil/compare a b) 0))
(defn lte [a b] (<= (CinqUtil/compare a b) 0))
(defn gt [a b] (> (CinqUtil/compare a b) 0))
(defn gte [a b] (>= (CinqUtil/compare a b) 0))

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

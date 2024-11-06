(ns com.wotbrew.cinq.npred
  "Native predicate tools, used for scans to apply as many predicates to memory as possible before decoding."
  (:require [com.wotbrew.cinq.nio-codec :as codec])
  (:import (com.wotbrew.cinq CinqUtil)
           (java.nio ByteBuffer)))

;; reference-impl
;; return lambda given symbol table.
;; virtual dispatch, but might still be ok given other overheads
;; ideas:
;; byte code interpreter
;; eval

(defn lambda
  "Given an expression vector e.g [:= 42]
  Returns a predicate function that will take a ByteBuffer argument and test the expression against it.

  Operators:
  [:= value] buf will be tested for equality (nil rules apply)
  [:< value] (:<, :<=, :>, :>=). Compares the value against the buf e.g for < : (< val buf).
  [:and & expr] conjunction
  [:or & expr] disjunction
  [:get key expr] returns a predicate that will test the value at the key, short-circuits if key cannot be found
    * e.g [:get :customer/name [:= \"Bob\"]] will test if the value at :customer/name is equal to Bob."
  [expr symbol-table]
  (let [[op & args] expr
        symbol-list (codec/symbol-list symbol-table)]
    (case (nth expr 0)
      :not (complement (lambda (first args) symbol-table))
      :=
      (let [[x] args
            buf (codec/encode-heap x symbol-table false)]
        #(let [result (codec/compare-bufs buf % symbol-list)]
           (when result (= 0 result))))
      (:< :<= :> :>=)
      (let [[x] args
            buf (codec/encode-heap x symbol-table false)]
        (case op
          :< #(CinqUtil/lt (codec/compare-bufs buf ^ByteBuffer % symbol-list) (long 0))
          :<= #(CinqUtil/lte (codec/compare-bufs buf ^ByteBuffer % symbol-list) (long 0))
          :> #(CinqUtil/gt (codec/compare-bufs buf ^ByteBuffer % symbol-list) (long 0))
          :>= #(CinqUtil/gte (codec/compare-bufs buf ^ByteBuffer % symbol-list) (long 0))))
      :and (apply every-pred (map #(lambda % symbol-table) args))
      :or (apply some-fn (map #(lambda % symbol-table) args))
      :get
      (let [[k expr] args
            key-buf (codec/encode-heap k symbol-table false)
            p (lambda expr symbol-table)]
        (fn [^ByteBuffer buf]
          (.mark buf)
          (let [match (codec/focus buf key-buf symbol-list)
                res (and match (p buf))]
            (.reset buf)
            res))))))

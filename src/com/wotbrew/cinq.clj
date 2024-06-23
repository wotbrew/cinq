(ns com.wotbrew.cinq
  (:refer-clojure :exclude [use max min count set update replace run! read vec range])
  (:require [com.wotbrew.cinq.eager-loop :as el]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.protocols :as p])
  (:import (com.wotbrew.cinq CinqUtil)
           (com.wotbrew.cinq.protocols Scannable)))

(defn optimize-plan [ra] (plan/rewrite ra))

(defn compile-plan [ra] (el/emit-rel ra))

(defn- fix-env [env f & args]
  (let [ctr (atom -1)]
    (binding [parse/*env* env
              plan/*gensym* (fn [pref] (symbol (str pref "__" (swap! ctr inc))))]
      (apply f args))))

(defn parse* [rel-expr] (parse/parse rel-expr))

(defmacro parse [rel-expr] (list `quote (fix-env &env parse* rel-expr)))

(defn tree* [rel-expr]
  (-> (parse* rel-expr)
      optimize-plan
      (plan/prune-cols {})))

(defmacro tree [rel-expr] (list `quote (fix-env &env tree* rel-expr)))

(defmacro plan [rel-expr] (list `quote (plan/stack-view (fix-env &env tree* rel-expr))))

(defmacro q [query body]
  (binding [parse/*env* &env]
    (-> (parse/parse-query query body)
        optimize-plan
        (plan/prune-cols #{})
        compile-plan)))

(defmacro with [bindings rel-expr]
  (binding [parse/*env* &env]
    (-> (parse/parse-cte bindings rel-expr)
        optimize-plan
        (plan/prune-cols #{})
        compile-plan)))

(defmacro union [& rels]
  (binding [parse/*env* &env]
    (-> (parse/parse-union rels)
        optimize-plan
        (plan/prune-cols #{})
        compile-plan)))

(defn- throw-only-in-queries [v]
  (throw (ex-info (format "%s only supported in queries" v) {})))

(defn rel-first [rel] (reduce (fn [_ x] (reduced x)) nil rel))

(defmacro scalar [query selection] `(rel-first (q ~query ~selection)))

(defmacro exists? [query] `(boolean (scalar ~query true)))

(defmacro run! [query & body]
  `(doseq [f# (q ~query (fn [] ~@body))] (f#)))

(defmacro agg [init init-sym query & body]
  {:pre [(symbol? init-sym)]}
  `(clojure.core/reduce (fn [~init-sym f#] (f# ~init-sym)) ~init (q ~query (fn [~init-sym] ~@body))))

(defmacro sum [expr] (#'throw-only-in-queries #'sum))

(defmacro max [expr] (#'throw-only-in-queries #'max))

(defmacro min [expr] (#'throw-only-in-queries #'min))

(defmacro avg [expr] (#'throw-only-in-queries #'avg))

(defmacro count
  ([] (#'throw-only-in-queries #'count))
  ([expr] (#'throw-only-in-queries #'count)))

(defmacro tuple [& kvs] (#'throw-only-in-queries #'tuple))

(defmacro write [binding & body] `(p/write-transaction ~(second binding) (fn [~(first binding)] ~@body)))

(defmacro read [binding & body] `(p/read-transaction ~(second binding) (fn [~(first binding)] ~@body)))

(defn create [db k] (p/create-relvar db k))

(defn rel-set [relvar rel] (p/rel-set relvar rel))

(defn insert [relvar record] (p/insert relvar record))

(defn- add-rsn-to-binding [binding rsn]
  (cond
    (symbol? binding) {rsn :cinq/rsn :as binding}
    (map? binding) (assoc binding rsn :cinq/rsn)
    (sequential? binding) {binding :cinq/self, rsn :cinq/rsn}
    :else (throw (ex-info "Unsupported binding form" {:binding binding}))))

(defmacro delete [query]
  {:pre [(<= 2 (clojure.core/count query))]}
  (let [rsn (gensym "rsn")
        rv (gensym "relvar")
        [binding relvar & query] query]
    `(let [~rv ~relvar]
       (agg
         0
         affected-records#
         ~(into [(add-rsn-to-binding binding rsn) rv] query)
         (if (p/delete ~rv ~rsn)
           (unchecked-inc affected-records#)
           affected-records#)))))

(defmacro update [query expr]
  {:pre [(<= 2 (clojure.core/count query))]}
  (let [rsn (gensym "rsn")
        rv (gensym "relvar")
        [binding relvar & query] query]
    `(let [~rv ~relvar]
       (agg
         0
         affected-records#
         ~(into [(add-rsn-to-binding binding rsn) rv] query)
         (if (p/delete ~rv ~rsn)
           (do (p/insert ~rv ~expr)
               (unchecked-inc affected-records#))
           affected-records#)))))

(extend-protocol p/Scannable
  nil
  (scan [_ _f init] init)
  Object
  (scan [this f init]
    (let [ctr (long-array 1)]
      (reduce
        (fn [acc record]
          (let [i (aget ctr 0)]
            (aset ctr 0 (unchecked-inc i))
            (f acc i record)))
        init
        this))))

(extend-protocol clojure.core.protocols/CollReduce
  Scannable
  (coll-reduce
    ([scan f start] (p/scan scan (fn [acc _rsn record] (f acc record)) start))
    ([scan f] (p/scan scan (fn [acc _rsn record] (f acc record)) (f)))))

(defmethod print-method Scannable
  [o w]
  (binding [*print-level* (and (not *print-dup*) *print-level* (dec *print-level*))]
    (do
      (.write w "#cinq/rel [")
      (let [ictr (volatile! -1)]
        (p/scan o (fn [print-length _ x]
                    (let [i (vswap! ictr inc)]
                      (if (and (not *print-dup*) print-length)
                        (if (<= print-length 0)
                          (do (.write w " ...") (reduced print-length))
                          (do
                            (when-not (= 0 i) (.write w " "))
                            (print-method x w)
                            (dec print-length)))
                        (do
                          (when-not (= 0 i) (.write w " "))
                          (print-method x w)
                          nil))))
                *print-length*))
      (.write w "] "))))

(extend-protocol p/BigCount
  nil
  (big-count [_] 0)
  Object
  (big-count [rel] (agg 0 n [_ rel] (unchecked-inc n))))

(defn rel-count [rel] (p/big-count rel))

(defn- lt [a b] (< (CinqUtil/compare a b) 0))
(defn- lte [a b] (<= (CinqUtil/compare a b) 0))
(defn- gt [a b] (> (CinqUtil/compare a b) 0))
(defn- gte [a b] (>= (CinqUtil/compare a b) 0))

(defn- index-test [test-fn]
  (condp identical? test-fn
    < lt
    <= lte
    > gt
    >= gte
    (throw (ex-info "Not a valid range query test, accepted (<, <=, >, >=)" {}))))

(defn range
  "Index range scan.

  e.g

  (range index > 3) all values over 3.
  (range index < 3) all values below 3.

  (range index < 3 > 1) all values below 3, but above one.
  (range index > 3 <= 100) all values between 3-100 incl."
  ([index test a]
   (condp identical? test
     > (p/range-scan index gt a nil nil)
     >= (p/range-scan index gte a nil nil)
     (p/range-scan index nil nil (index-test test) a)))
  ([index start-test start-key end-test end-key]
   (condp identical? start-test
     < (p/range-scan index (index-test end-test) end-key (index-test start-test) start-key)
     <= (p/range-scan index (index-test end-test) end-key (index-test start-test) start-key)
     (p/range-scan index (index-test start-test) start-key (index-test end-test) end-key))))

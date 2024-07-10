(ns com.wotbrew.cinq
  (:refer-clojure :exclude [use max min count set update replace read range var])
  (:require [clojure.core :as clj]
            [com.wotbrew.cinq.eager-loop :as el]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.plan :as plan]
            [com.wotbrew.cinq.protocols :as p]
            [meander.epsilon :as m]
            [meander.strategy.epsilon :as r])
  (:import (com.wotbrew.cinq CinqUtil)
           (com.wotbrew.cinq.protocols BigCount IncrementalRelvar Index Relvar Scannable)))

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
  (throw (ex-info (format "%s only supported in cinq queries" v) {})))

(defn rel-first
  ([rel] (reduce (fn [_ x] (reduced x)) nil rel))
  ([rel not-found] (reduce (fn [_ x] (reduced x)) not-found rel)))

(defmacro scalar [query selection] `(rel-first (q ~query ~selection)))

(defmacro exists? [query] `(boolean (scalar ~query true)))

(defn- throw-only-in-run [v]
  (throw (ex-info (format "%s only supported in cinq/run bodies" v) {})))

(defmacro delete [target] (throw-only-in-run #'delete))

(defmacro update [target expr] (throw-only-in-run #'update))

(defmacro agg [init init-sym query & body]
  {:pre [(symbol? init-sym)]}
  `(clojure.core/reduce (fn [~init-sym f#] (f# ~init-sym)) ~init (q ~query (fn [~init-sym] ~@body))))

(defmacro run [query & body]
  (let [srelvar (fn [?alias] (symbol (str ?alias ":cinq") "relvar"))
        srsn (fn [?alias] (symbol (str ?alias ":cinq") "rsn"))
        match
        #(m/match %
           (m/and (?f & ?args)
                  (m/guard (symbol? ?f)))
           (condp = (resolve &env ?f)
             #'update
             (m/match ?args
               (m/and (?alias ?expr)
                      (m/guard (simple-symbol? ?alias)))
               `(do (p/delete ~(srelvar ?alias) ~(srsn ?alias))
                    (p/insert ~(srelvar ?alias) ~?expr))
               _
               (throw (ex-info "Invalid update call" {:args ?args})))

             #'delete
             (m/match ?args
               (m/and (?alias)
                      (m/guard (simple-symbol? ?alias)))
               `(p/delete ~(srelvar ?alias) ~(srsn ?alias))
               _ (throw (ex-info "Invalid delete call" {:args ?args})))
             %)
           _ %)
        rw (r/top-down (r/attempt match))]
    `(agg nil _# ~query ~@(rw body))))

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

(defn create [db relvar-id]
  {:pre [(keyword? relvar-id)]}
  (p/create-relvar db relvar-id))

(defn create-index [db [relvar-id indexed-key]]
  {:pre [(keyword? relvar-id)
         (keyword? indexed-key)]}
  (p/create-index db relvar-id indexed-key))

(defn rel-set [relvar rel] (p/rel-set relvar rel))

(defn insert [relvar row] (p/insert relvar row))

(defn ensure-binding-target [binding]
  (let [sym (delay (gensym "self"))]
    (cond
      (symbol? binding) [binding binding]
      (map? binding) [(assoc binding @sym :cinq/self) @sym]
      (sequential? binding) [{binding :cinq/self, @sym :cinq/self} @sym]
      :else (throw (ex-info "Unsupported binding form" {:binding binding})))))

(defn incr-counter [^longs ctr]
  (aset ctr 0 (unchecked-inc (aget ctr 0))))

(defmacro update-where [relvar binding pred expr]
  (let [[bind sym] (ensure-binding-target (first binding))]
    `(let [ctr# (long-array 1)]
       (run ~(into [bind] [relvar :when pred]) (when (update ~sym ~expr) (incr-counter ctr#)))
       (aget ctr# 0))))

(defmacro delete-where [relvar binding pred]
  (let [[bind sym] (ensure-binding-target (first binding))]
    `(let [ctr# (long-array 1)]
       (run ~(into [bind] [relvar :when pred]) (when (delete ~sym) (incr-counter ctr#)))
       (aget ctr# 0))))

(defmacro update-all [relvar binding expr]
  (let [[bind sym] (ensure-binding-target (first binding))]
    `(let [ctr# (long-array 1)]
       (run [~bind ~relvar] (when (update ~sym ~expr) (incr-counter ctr#)))
       (aget ctr# 0))))

(declare rel-count)

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
            (f acc nil i record)))
        init
        this))))

(extend-protocol clojure.core.protocols/CollReduce
  Scannable
  (coll-reduce
    ([scan f start] (p/scan scan (fn [acc _rv _rsn record] (f acc record)) start))
    ([scan f] (p/scan scan (fn [acc _rv _rsn record] (f acc record)) (f)))))

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

(defn getn
  "Index lookup returning a relation of rows (cinq) eq to the key,
  same as (get idx k) for non-unique indexes.

  Like (get idx k), always returns a relation, even on key miss."
  [index k]
  (p/getn index k))

(defn get1
  "Index unique-or-first lookup, returns the matching record or not-found (default nil)."
  ([index k] (p/get1 index k nil))
  ([index k not-found] (p/get1 index k not-found)))

(defmethod print-method Scannable [o w]
  (binding [*print-level* (and (not *print-dup*) *print-level* (dec *print-level*))]
    (do
      (.write w "#cinq/rel [")
      (let [ictr (volatile! -1)]
        (p/scan o (fn [print-length _ _ x]
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

(defmethod print-method Relvar [o w]
  (binding [*print-level* (and (not *print-dup*) *print-level* (dec *print-level*))]
    (do
      (.write w "#cinq/var [")
      (let [ictr (volatile! -1)]
        (p/scan o (fn [print-length _ _ x]
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

(defmethod print-method Index [o w]
  (binding [*print-level* (and (not *print-dup*) *print-level* (dec *print-level*))]
    (do
      (.write w "#cinq/idx [")
      (let [ictr (volatile! -1)
            index-key (p/indexed-key o)]
        (p/scan o (fn [print-length _ _ x]
                    (let [i (vswap! ictr inc)
                          k (get x index-key)]
                      (if (and (not *print-dup*) print-length)
                        (if (<= print-length 0)
                          (do (.write w " ...") (reduced print-length))
                          (do
                            (when-not (= 0 i) (.write w ", "))
                            (.write w "[")
                            (print-method k w)
                            (.write w " ")
                            (print-method x w)
                            (.write w "]")
                            (dec print-length)))
                        (do
                          (when-not (= 0 i) (.write w ", "))
                          (.write w "[")
                          (print-method k w)
                          (.write w " ")
                          (print-method x w)
                          (.write w "]")
                          nil))))
                *print-length*))
      (.write w "] "))))

(prefer-method print-method Relvar Scannable)

(prefer-method print-method Index Scannable)

(deftype RefVariable [ref]
  Scannable
  (scan [rv f init] (reduce-kv (fn [acc rsn o] (f acc rv rsn o)) init @ref))
  Relvar
  (rel-set [rv rel]
    (dosync
      (ref-set ref
               (p/scan
                 rel
                 (fn [acc _ rsn o] (assoc acc rsn o))
                 (sorted-map))))
    nil)
  IncrementalRelvar
  (insert [_ o]
    (dosync
      (let [ret (volatile! nil)]
        (alter ref
               (fn [sm]
                 (let [[last-rsn] (first (reverse sm))
                       rsn (if last-rsn (inc last-rsn) 0)]
                   (vreset! ret rsn)
                   (assoc sm rsn o))))
        @ret)))
  (delete [_ rsn]
    (dosync
      (let [ret (volatile! false)]
        (alter ref
               (fn [sm]
                 (if (contains? sm rsn)
                   (do (vreset! ret true) (dissoc sm rsn))
                   sm)))
        @ret)))
  BigCount
  (big-count [_] (clj/count @ref)))

(defn relvar
  "An in-memory relvar, for testing and experimentation at the repl.

  Participates in Clojure's STM, so inherits the transactional
  behaviour of clojure.lang.Ref.

  Will be quite slow compared to com.wotbrew.cinq.lmdb backed relvars."
  []
  (->RefVariable (ref (sorted-map))))

(comment

  (doto (var)
    (insert 42)
    (insert 43))

  )

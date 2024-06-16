(ns com.wotbrew.cinq.parse
  (:require [clojure.string :as str]
            [com.wotbrew.cinq.expr :as expr]
            [meander.epsilon :as m]
            [meander.strategy.epsilon :as r]
            [com.wotbrew.cinq.plan2 :as plan])
  (:import (com.wotbrew.cinq CinqLongBox)))

(def ^:dynamic *env* {})

(defn normalize-binding [binding]
  (cond
    (vector? binding)
    (if (= :as (butlast binding))
      (into [[(last binding) :cinq/self]]
            (for [[i sym] (map-indexed vector (drop-last 2 binding))]
              [sym i]))
      (vec (for [[i sym] (map-indexed vector binding)] [sym i])))

    (map? binding)
    (vec (for [[k v] binding
               [sym x]
               (cond
                 (symbol? k) [[k v]]
                 (= :keys k)
                 (for [k v]
                   [(symbol nil (name k)) k])

                 (= :strs k)
                 (for [s v]
                   [(symbol nil s) s])

                 (= :as k)
                 [[v :cinq/self]]

                 :else (throw (Exception. "Unsupported binding form")))]
           [sym x]))

    (symbol? binding) [[binding :cinq/self]]

    :else (throw (Exception. "Unsupported binding form"))))

(declare parse)

(defn lookup-sym? [expr]
  (and (symbol? expr)
       (or (when (namespace expr)
             (str/includes? (namespace expr) ":"))
           (str/includes? (name expr) ":"))))

(defn parse-lookup-sym [s]
  (assert (lookup-sym? s))
  (cond (and (namespace s)
             (str/includes? (namespace s) ":"))
        (let [[a b] (str/split (namespace s) #"\:")]
          {:kw (keyword b (name s))
           :a (symbol a)
           :t (:tag (meta s))})
        (str/includes? (name s) ":")
        (let [[a b] (str/split (name s) #"\:")]
          {:kw (keyword (namespace s) b)
           :a (symbol a)
           :t (:tag (meta s))})))

(def ^:redef n2n-rewrites #{})

(defn add-n2n-rewrites [& rewrites]
  (alter-var-root #'n2n-rewrites into rewrites))

(add-n2n-rewrites

  `+
  `*
  `-
  `/
  `quot
  `rem
  `subs
  `clojure.string/includes?
  `clojure.string/starts-with?
  `clojure.string/ends-with?
  `re-find

  )

(def expr-rewrites
  (r/match
    ;; sub query flavors

    ;; scalar valued
    (S ?qry ?expr)
    [::plan/scalar-sq (parse (list 'q ?qry ?expr))]

    ;; lookup symbols, e.g foo:bar == (:bar foo)
    (m/and ?s (m/guard (lookup-sym? ?s)))
    (let [{:keys [kw, a, t]} (parse-lookup-sym ?s)]
      [::plan/lookup kw a t])

    ;; kw lookup without default (therefore nil)
    (m/and (?kw ?s)
           (m/guard (keyword? ?kw))
           (m/guard (simple-symbol? ?s)))
    [::plan/lookup ?kw ?s nil]

    ;; todo remove these, use cinq/*
    ;; aggregates
    ($sum ?expr)
    [::plan/sum ?expr]
    ($avg ?expr)
    [::plan/avg ?expr]
    ($min ?expr)
    [::plan/min ?expr]
    ($max ?expr)
    [::plan/max ?expr]
    ($count ?expr)
    [::plan/count ?expr]
    ($count)
    [::plan/count]

    ;; comparisons
    (= ?a ?b)
    [::plan/= ?a ?b]

    (< ?a ?b)
    [::plan/< ?a ?b]
    (<= ?a ?b)
    [::plan/<= ?a ?b]
    (>= ?a ?b)
    [::plan/>= ?a ?b]
    (> ?a ?b)
    [::plan/> ?a ?b]

    ;; bools
    (and & ?clause)
    (into [::plan/and] ?clause)

    (or & ?clause)
    (into [::plan/or] ?clause)

    (not ?expr)
    [::plan/not ?expr]

    (not= ?a ?b)
    [::plan/not [::plan/= ?a ?b]]

    (contains? ?coll ?expr)
    (if (and (set? ?coll) (every? expr/certainly-const-expr? ?coll))
      [::plan/in ?expr ?coll]
      [::plan/contains ?coll ?expr])

    (m/and (?sym & ?args)
           (m/guard (symbol? ?sym))
           (m/guard (resolve *env* ?sym)))
    (condp = (.toSymbol (resolve *env* ?sym))
      'com.wotbrew.cinq/sum
      (into [::plan/sum] ?args)
      'com.wotbrew.cinq/max
      (into [::plan/max] ?args)
      'com.wotbrew.cinq/min
      (into [::plan/min] ?args)
      'com.wotbrew.cinq/avg
      (into [::plan/avg] ?args)
      'com.wotbrew.cinq/count
      (into [::plan/count] ?args)
      'com.wotbrew.cinq/scalar
      [::plan/scalar-sq (parse (list* 'q ?args))]
      'com.wotbrew.cinq/exists?
      [::plan/scalar-sq (parse (list* 'q (concat ?args [true])))]

      (if (and (n2n-rewrites (some-> (resolve *env* ?sym) .toSymbol))
               (seq ?args))
        ;; todo scoping problem!
        (into [::plan/apply-n2n (some-> (resolve *env* ?sym) .toSymbol)] ?args)
        (list* ?sym ?args)))

    ?any ?any))

;; todo analyze environment (for e.g shadowing cinq vars)
(def rewrite-exprs
  (-> #'expr-rewrites
      r/bottom-up))

(defn parse-stmt [[op arg]]
  (if-not (keyword? op)
    (parse-stmt [:from [op arg]])
    (case op
      :from
      (let [[binding expr] arg]
        {:op :from
         :expr (rewrite-exprs expr)
         :bindings (normalize-binding binding)})

      :when
      {:op :when
       :pred (rewrite-exprs arg)}

      :let
      {:op :let
       :bindings (mapv (fn [[a b]] [a (rewrite-exprs b)]) (partition 2 arg))}

      (:join :left-join :semi-join :anti-join)
      (let [[binding expr & [condition :as cseq]] arg]
        {:op op
         :bindings (normalize-binding binding)
         :expr (rewrite-exprs expr)
         :condition (if (seq cseq) (rewrite-exprs condition) true)})

      :group
      {:op :group
       :bindings (mapv (fn [[a b]] [a (rewrite-exprs b)]) (partition 2 arg))}

      :order
      {:op :order
       :clauses (mapv (fn [[expr dir]] [(rewrite-exprs expr) dir]) (partition 2 arg))}

      :limit
      {:op :limit
       :n arg})))

(defn parse-tree [q]
  (let [statements (mapv parse-stmt (partition 2 q))]
    (reduce (fn [acc stmt] (if acc (assoc stmt :prev acc) stmt)) nil statements)))

(defn parse-selection [selection-binding]
  (let [tree (parse-tree selection-binding)]
    ((fn ! [tree]
       (case (:op tree)
         :from
         (if (:prev tree)
           [::plan/apply
            :cross-join
            (! (:prev tree))
            [::plan/where [::plan/scan (:expr tree) (:bindings tree)] true]]
           [::plan/scan (:expr tree) (:bindings tree)])

         :join
         [::plan/apply
          :cross-join
          (! (:prev tree))
          [::plan/where [::plan/scan (:expr tree) (:bindings tree)] (:condition tree)]]

         :left-join
         [::plan/apply
          :left-join
          (! (:prev tree))
          [::plan/where [::plan/scan (:expr tree) (:bindings tree)] (:condition tree)]]

         :when
         [::plan/where (! (:prev tree)) (:pred tree)]

         :let
         [::plan/let (! (:prev tree)) (:bindings tree)]

         :select
         [::plan/project (! (:prev tree)) (:projection tree)]

         :group
         [::plan/group-by (! (:prev tree)) (:bindings tree)]

         :order
         [::plan/order-by (! (:prev tree)) (:clauses tree)]

         :limit
         [::plan/limit (! (:prev tree)) (:n tree) `(CinqLongBox. 0)]))
     tree)))

(defn parse-projection [selection expr]
  (m/match expr
    :cinq/*
    (let [cols (plan/columns selection)]
      [::plan/project selection
       [[(plan/*gensym* "col")
         (into {} (for [col cols] [(keyword (name col)) (rewrite-exprs col)]))]]])

    (m/and (?sym & ?projection)
           (m/guard (symbol? ?sym))
           (m/guard (= 'com.wotbrew.cinq/tuple (.toSymbol ^clojure.lang.Var (resolve ?sym)))))
    [::plan/project selection [[(plan/*gensym* "col") (mapv (fn [[_ e]] (rewrite-exprs e)) (partition 2 ?projection))]]]

    ?expr
    [::plan/project selection [[(plan/*gensym* "col") (rewrite-exprs ?expr)]]]))

(defn parse [[_ binding :as query]]
  (-> (parse-selection binding)
      (parse-projection (nth query 2 :cinq/*))))

(comment

  (parse '(q [a [1, 2, 3]]))
  (parse '(q [a [1, 2, 3]] a))
  (parse '(q [a [1, 2, 3]] {:foo a, :bar (inc a)}))
  (parse '(q [a [1, 2, 3]] ($select :foo a, :bar (inc a))))

  )

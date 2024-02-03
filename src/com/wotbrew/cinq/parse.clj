(ns com.wotbrew.cinq.parse
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [meander.epsilon :as m]
            [meander.strategy.epsilon :as r]
            [com.wotbrew.cinq.plan2 :as plan]))

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
           :a (symbol a)})
        (str/includes? (name s) ":")
        (let [[a b] (str/split (name s) #"\:")]
          {:kw (keyword (namespace s) b)
           :a (symbol a)})))

(def expr-rewrites
  (r/match
    ;; sub query flavors

    ;; scalar valued
    (S ?qry ?expr)
    [::plan/scalar-sq (parse (list 'q ?qry ?expr))]

    ;; lookup symbols, e.g foo:bar == (:bar foo)
    (m/and ?s (m/guard (lookup-sym? ?s)))
    (let [{:keys [kw, a]} (parse-lookup-sym ?s)]
      [::plan/lookup kw a])

    ;; kw lookup without default (therefore nil)
    (m/and (?kw ?s)
           (m/guard (keyword? ?kw))
           (m/guard (simple-symbol? ?s)))
    [::plan/lookup ?kw ?s]

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
    [::plan/count-some ?expr]

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

    ?any ?any))

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
         [::plan/limit (! (:prev tree)) (:n tree)]))
     tree)))

(defn parse-projection [selection expr]
  (m/match expr
    :cinq/*
    (let [cols (plan/columns selection)]
      [::plan/project selection
       [[(plan/*gensym* "col")
         (into {} (for [col cols] [(keyword (name col)) (rewrite-exprs col)]))]]])

    ;; todo tuple type?
    ($select & ?projection)
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

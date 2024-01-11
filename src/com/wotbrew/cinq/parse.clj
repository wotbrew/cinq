(ns com.wotbrew.cinq.parse
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [meander.epsilon :as m]
            [meander.strategy.epsilon :as r]))

(create-ns 'com.wotbrew.cinq.plan2)
(alias 'plan 'com.wotbrew.cinq.plan2)

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
                   [(symbol nil k) k])

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
    (S ?qry)
    [::plan/scalar-sq (parse ?qry)]

    ;; column valued
    (C ?qry)
    [::plan/column-sq (parse ?qry)]

    ;; relation valued
    (Q ?qry)
    [::plan/sq (parse ?qry)]

    ;; lookup symbols, e.g foo:bar == (:bar foo)
    (m/and ?s (m/guard (lookup-sym? ?s)))
    (let [{:keys [kw, a]} (parse-lookup-sym ?s)]
      [::plan/lookup kw a nil])

    ;; kw lookup with default
    (m/and (?kw ?s ?default)
           (m/guard (keyword? ?kw))
           (m/guard (simple-symbol? ?s)))
    [::plan/lookup ?kw ?s ?default]

    ;; kw lookup without default (therefore nil)
    (m/and (?kw ?s)
           (m/guard (keyword? ?kw))
           (m/guard (simple-symbol? ?s)))
    [::plan/lookup ?kw ?s nil]

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

      :where
      {:op :where
       :pred (rewrite-exprs arg)}

      :let
      {:op :let
       :bindings (mapv (fn [[a b]] [a (rewrite-exprs b)]) (partition 2 arg))}

      (:join :left-join)
      (let [[binding expr & [condition :as cseq]] arg]
        {:op op
         :bindings (normalize-binding binding)
         :expr (rewrite-exprs expr)
         :condition (if (seq cseq) (rewrite-exprs condition) true)})

      :group-by
      {:op :group-by
       :bindings (mapv (fn [[a b]] [a (rewrite-exprs b)]) (partition 2 arg))}

      :order-by
      {:op :order-by
       :clauses (mapv (fn [[expr dir]] [(rewrite-exprs expr) dir]) (partition 2 arg))}

      :limit
      {:op :limit
       :n arg}

      :select
      {:op :select
       :projection (mapv (fn [[a b]] [a (rewrite-exprs b)]) (partition 2 arg))})))

(defn parse-tree [q]
  (let [statements (mapv parse-stmt (partition 2 q))]
    (reduce (fn [acc stmt] (if acc (assoc stmt :prev acc) stmt)) nil statements)))

(defn find-lookup-syms [expr]
  #_(let [ret (atom #{})]
    ((fn ! [expr]
       (m/match expr
         _
         (cond
           (symbol? expr) (when (lookup-sym? expr) (swap! ret conj expr))
           (seq? expr) (run! ! expr)
           (vector? expr) (run! ! expr)
           (map? expr) (run! ! expr)
           (set? expr) (run! ! expr)
           (map-entry? expr) (run! ! expr)
           :else nil))) expr)
    ret)

  (->> (tree-seq seqable? seq expr)
       (filter lookup-sym?)
       (set)))

(defn find-binding [bindings val]
  (first (last (filter #(= val (second %)) bindings))))

(defn push-lookups [tree lookup-syms]
  (when tree
    (case (:op tree)
      :select
      (let [{:keys [projection]} tree
            exprs (map second projection)
            lookup-syms (find-lookup-syms exprs)]
        (update tree :prev push-lookups lookup-syms))

      :where
      (let [{:keys [pred]} tree
            new-lookups (find-lookup-syms pred)]
        (update tree :prev push-lookups (into lookup-syms new-lookups)))

      :let
      (let [{:keys [bindings]} tree
            new-lookups (find-lookup-syms (map second bindings))]
        (update tree :prev push-lookups (into lookup-syms new-lookups)))

      (:from :join :left-join)
      (let [{:keys [bindings, condition, prev]} tree
            self-sym (find-binding bindings :cinq/self)
            already-bound (atom (set (map first bindings)))
            lookup-syms (into lookup-syms (find-lookup-syms condition))
            new-binding (reduce (fn [b s]
                                  (let [{:keys [kw, a]} (parse-lookup-sym s)]
                                    (cond
                                      (@already-bound s) b

                                      (= self-sym a)
                                      (do (swap! already-bound conj s)
                                          (conj b [s kw]))

                                      :else b)))
                                bindings
                                lookup-syms)
            left-syms (set/difference lookup-syms @already-bound)]
        (assoc tree :prev (push-lookups prev left-syms)
                    :bindings new-binding))

      :group-by
      (let [{:keys [bindings, prev]} tree
            ;; todo filter shadowed, destructuring
            left-syms (find-lookup-syms (map second bindings))]
        (assoc tree :prev (push-lookups prev (into lookup-syms left-syms))
                    :bindings bindings))

      :order-by
      (let [{:keys [clauses]} tree]
        (update tree :prev push-lookups (into lookup-syms (find-lookup-syms clauses))))

      :limit
      (update tree :prev push-lookups lookup-syms))))

(defn parse [q]
  (let [tree (parse-tree q)
        #_#_ tree (push-lookups tree #{})]
    ((fn ! [tree]
       (case (:op tree)
         :from
         (if (:prev tree)
           [::plan/dependent-join
            (! (:prev tree))
            [::plan/where [::plan/scan (:expr tree) (:bindings tree)] true]]
           [::plan/scan (:expr tree) (:bindings tree)])

         :join
         [::plan/dependent-join
          (! (:prev tree))
          [::plan/where [::plan/scan (:expr tree) (:bindings tree)] (:condition tree)]]

         :left-join
         [::plan/dependent-left-join
          (! (:prev tree))
          [::plan/where [::plan/scan (:expr tree) (:bindings tree)] (:condition tree)]]

         :where
         [::plan/where (! (:prev tree)) (:pred tree)]

         :select
         [::plan/select (! (:prev tree)) (:projection tree)]

         :group-by
         [::plan/group-by (! (:prev tree)) (:bindings tree)]

         :order-by
         [::plan/order-by (! (:prev tree)) (:clauses tree)]

         ))
     tree)))

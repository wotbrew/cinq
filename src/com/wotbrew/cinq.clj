(ns com.wotbrew.cinq
  (:require [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import (java.util ArrayList HashMap)))

(defprotocol Relation
  :extend-via-metadata true
  (columns [rel])
  (tuple-seq [rel]))

(defn relation? [x]
  (or (and (contains? (meta x) `tuple-seq)
           (contains? (meta x) `columns))
      (satisfies? Relation x)))

(defn arity [rel] (count (columns rel)))

(defn- join-tuples [l r added-col-count]
  (into l (take added-col-count) (concat r (repeat nil))))

(defn nested-loop-join [rel added-col-count f]
  (with-meta
    (for [l (tuple-seq rel)
          r (tuple-seq (f l))]
      (join-tuples l r added-col-count))
    {`tuple-seq identity
     `columns (constantly (into (columns rel) (repeat added-col-count :cinq/anon)))}))

(defn hash-equi-join
  [build
   build-key-fn
   probe
   probe-key-fn
   added-col-count
   theta-condition]
  (-> (lazy-seq
        (let [hm (HashMap.)
              _ (doseq [b (tuple-seq build)]
                  (let [k (build-key-fn b)
                        nested-list (.get hm k)
                        empty-nested-list (when-not nested-list (ArrayList.))
                        _ (when empty-nested-list (.put hm k empty-nested-list))
                        nested-list (or nested-list empty-nested-list)]
                    (.add nested-list b)))]
          (for [p (tuple-seq probe)
                b (.get hm (probe-key-fn p))
                :when (theta-condition b p)]
            (join-tuples b p added-col-count))))
      (with-meta
        {`tuple-seq identity
         `columns (constantly (into (columns build) (repeat added-col-count :cinq/anon)))})))

(defn hash-equi-left-join
  [probe
   probe-key-fn
   build
   build-key-fn
   added-col-count
   theta-condition]
  (-> (lazy-seq
        (let [hm (HashMap.)
              _ (doseq [b (tuple-seq build)]
                  (let [k (build-key-fn b)
                        nested-list (.get hm k)
                        empty-nested-list (when-not nested-list (ArrayList.))
                        _ (when empty-nested-list (.put hm k empty-nested-list))
                        nested-list (or nested-list empty-nested-list)]
                    (.add nested-list b)))]
          (for [p (tuple-seq probe)
                :let [bs (filter #(theta-condition p %) (.get hm (probe-key-fn p)))]
                b (if (seq bs) bs (repeat added-col-count nil))]
            (join-tuples p b added-col-count))))
      (with-meta
        {`tuple-seq identity
         `columns (constantly (into (columns probe) (repeat added-col-count :cinq/anon)))})))

(defn nested-loop-left-join [rel added-col-count f]
  (with-meta
    (for [l (tuple-seq rel)
          :let [rs (tuple-seq (f l))]
          r (if (seq rs) rs [(repeat nil)])]
      (join-tuples l r added-col-count))
    {`tuple-seq identity
     `columns (constantly (into (columns rel) (repeat added-col-count :cinq/anon)))}))

(defn where [rel f]
  (with-meta (lazy-seq (filter f (tuple-seq rel))) (meta rel)))

(defn group [rel & group-keys]
  (let [group-key (if (seq group-keys) (apply juxt group-keys) (constantly []))
        arity (arity rel)]
    (with-meta
      (for [[gk ts] (group-by group-key (tuple-seq rel))]
        (into (mapv (fn [i] (map (fn [t] (nth t i)) ts)) (range arity)) gk))
      {`tuple-seq identity
       `columns (constantly (into (columns rel) (repeat (count group-keys) :cinq/anon)))})))

(defn order [rel & order-clauses]
  rel)

(defn limit [rel n]
  (with-meta (lazy-seq (take n (tuple-seq rel))) (meta rel)))

(defn add [rel & fns]
  (let [f (fn [t] (reduce (fn [t f] (conj t (f t))) t fns))]
    (with-meta
      (lazy-seq (map f (tuple-seq rel)))
      {`tuple-seq identity
       `columns (constantly (into (columns rel) (repeat (count fns) :cinq/anon)))})))

(defn select [rel & fns]
  (let [f (if (seq fns) (apply juxt fns) (constantly []))]
    (with-meta
      (lazy-seq (map f (tuple-seq rel)))
      {`tuple-seq identity
       `columns (constantly (vec (repeat (count fns) :cinq/anon)))})))

(defn scan [x cols]
  (let [tuple-f (if (seq cols) (apply juxt (map (fn [col] (if (= :cinq/self col) identity col)) cols)) (constantly []))]
    (with-meta
      (map tuple-f x)
      {`tuple-seq identity
       `columns (constantly (vec cols))})))

(defn parse [[_ selection {select-binding :select
                           order-by-binding :order-by
                           limit-count :limit}]]
  (letfn [(add-name [[names arity] binding]
            [(assoc names binding arity) (inc arity)])
          (plan-op
            ([env op arg] (plan-op env op arg false))
            ([env op arg first-op]
             (case op
               :from
               (let [[binding x] arg]
                 (if first-op
                   (if (= {} binding)
                     [env (list `scan [nil] [])]
                     (do
                       (assert (symbol? binding))
                       [(add-name env binding) (list `scan x [:cinq/self])]))
                   (plan-op env :join [binding x])))
               :let
               (let [bindings (partition 2 (destructure arg))
                     rf (fn [[env fns] [binding expr]]
                          [(add-name env binding) (conj fns (lambda env expr))])
                     [env fns] (reduce rf [env []] bindings)]
                 [env (list* `add fns)])
               (:join :left-join)
               (let [[binding expr & conditions] arg
                     where-clause
                     (case (count conditions)
                       0 true
                       1 (first conditions)
                       (list* 'and conditions))]
                 (if (= {} binding)
                   [(add-name env binding)
                    (list
                      (case op
                        :join `nested-loop-join
                        :left-join `nested-loop-left-join)
                      1
                      (lambda env `(-> (scan ~expr [])
                                       (where ~(lambda [{} 0] where-clause)))))]
                   (do
                     (assert (symbol? binding))
                     [(add-name env binding)
                      (list
                        (case op
                          :join `nested-loop-join
                          :left-join `nested-loop-left-join)
                        1
                        (lambda env `(-> (scan ~expr [:cinq/self])
                                         (where ~(lambda (add-name [{} 0] binding) where-clause)))))])))
               :where
               (let [expr arg]
                 [env (list `where (lambda env expr))])
               (if (keyword? op)
                 (throw (ex-info "Unknown operation" {}))
                 (plan-op env :from [op arg] first-op)))))

          (plan-projection [env binding expr]
            (if (= '. expr)
              (if (lookup-sym? binding)
                (let [[kw :as kw-lookup] (parse-lookup-sym binding)]
                  (do
                    (when-not (symbol? binding) (throw (ex-info "Automatic bindings are only supported if the binding is a symbol" {})))
                    [(add-name env (symbol (name kw))) kw-lookup])))
              [(add-name env binding) expr]))

          (lookup-sym? [x]
            (and (symbol? x)
                 (= nil (namespace x))
                 (str/includes? (name x) ":")))

          (parse-lookup-sym [sym]
            (let [[part-a part-b :as parts] (str/split (name sym) #":")]
              (if (= 2 (count parts))
                (list (keyword part-b) (symbol part-a))
                (throw (ex-info "Invalid lookup sym" {})))))

          (desugar [form]
            (walk/postwalk (fn [x] (if (lookup-sym? x) (parse-lookup-sym x) x)) form))

          (lambda [env expr]
            (list `fn [(first env)] (desugar expr)))]
    (let [selection (or (not-empty selection) [{} [nil]])

          [selection-env selection-plan]
          (reduce (fn [[env plan] [op arg]]
                    (let [[new-env plan-form] (plan-op env op arg (= 0 (count plan)))]
                      [new-env (conj plan plan-form)]))
                  [[{} 0] []]
                  (partition 2 selection))

          [projection-env projection]
          (if select-binding
            (reduce (fn [[env proj] [binding expr]]
                      (let [[new-env expr] (plan-projection env binding (desugar expr))]
                        [new-env (conj proj (lambda selection-env expr))]))
                    [selection-env []]
                    (partition 2 select-binding))
            [selection-env []])

          projection-plan
          (if (seq projection)
            (conj selection-plan (list* `add projection))
            selection-plan)

          projection-vec (vec (range (second selection-env) (second projection-env)))
          inverse-names (set/map-invert (first projection-env))]
      {:names (if select-binding
                (mapv (comp keyword inverse-names) projection-vec)
                (mapv (comp keyword key) (sort-by val (first projection-env))))
       :plan
       (list* '->
              (concat projection-plan
                      (when order-by-binding
                        [(list* `order (for [[expr dir] order-by-binding] [(lambda projection-env expr) dir]))])
                      (when limit-count
                        [(list `limit limit-count)])
                      (when select-binding
                        [(list* `select (map (fn [i] (lambda projection-env (inverse-names i))) projection-vec))])))})))

(defmacro q
  ([selection] `(q ~selection {}))
  ([selection {:keys [select order-by limit]}]
   (let [{:keys [names, plan]} (parse [nil selection {:select select, :order-by order-by, :limit limit}])]
     `(let [plan# ~plan]
        (with-meta
          plan#
          {`columns (constantly ~names),
           `tuple-seq (fn [_#] (tuple-seq plan#))
           :type ::results})))))

(defn unique-col-map [rel]
  (reduce-kv
    (fn [m i col]
      (if (m col)
        (assoc m (keyword "cinq" (str "col" (count m))) i)
        (assoc m col i)))
    {}
    (columns rel)))

(defn maps
  ([rel] (maps (unique-col-map rel) rel))
  ([col-map rel]
   (map (fn [t] (update-vals col-map t)) rel)))

(defmethod print-method ::results [o w]
  (binding [*out* w]
    (let [col-map (unique-col-map o)]
      (pp/print-table (keys col-map) (maps col-map o)))))

(comment

  (parse '(q [n [1 2 3]]))
  (parse '(q [n [1 2 3] :where (even? n)]))

  (q [n [1 2 3]])
  (q [n [1 2 3] :where (even? n)])

  (parse '(q [c customers]))
  (parse '(q [c customers :where (= c:customer-id 42)]))
  (parse '(q [c customers :join [o orders (= o:customer-id c:customer-id)]]))

  (q [c customers])

  (let [customers [{:id 0, :firstname "fred"}
                   {:id 1, :firstname "bob"}]]
    (q [c customers]
       {:select
        [c:id .
         c:firstname .]}))

  )

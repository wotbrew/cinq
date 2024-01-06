(ns com.wotbrew.cinq
  (:require [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [com.wotbrew.cinq.plan :as plan])
  (:import (java.util ArrayList Comparator HashMap)))

(defprotocol Relation
  :extend-via-metadata true
  (columns [rel])
  (tuple-seq [rel]))

(defn relation? [x]
  (or (and (contains? (meta x) `tuple-seq)
           (contains? (meta x) `columns))
      (satisfies? Relation x)))

(defn arity [rel] (count (columns rel)))

(defn- join-tuples
  ([l r] (into l r))
  ([l r added-col-count]
   (into l (take added-col-count) (concat r (repeat nil)))))

(defn product [rel1 rel2]
  (with-meta
    (for [l (tuple-seq rel1)
          r (tuple-seq rel2)]
      (join-tuples l r))
    {`tuple-seq identity
     `columns (constantly (into (columns rel1) (columns rel2)))}))

(defn dependent-join [rel added-col-count f]
  (with-meta
    (for [l (tuple-seq rel)
          r (tuple-seq (f l))]
      (join-tuples l r added-col-count))
    {`tuple-seq identity
     `columns (constantly (into (columns rel) (repeat added-col-count :cinq/anon)))}))

(defn join [left right pred]
  (with-meta
    (for [l (tuple-seq left)
          r (tuple-seq right)
          :when (pred l r)]
      (join-tuples l r))
    {`tuple-seq identity
     `columns (constantly (into (columns left) (columns right)))}))

(defn equi-join
  [build
   build-key-fn
   probe
   probe-key-fn
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
            (join-tuples b p))))
      (with-meta
        {`tuple-seq identity
         `columns (constantly (into (columns build) (columns probe)))})))

(defn equi-left-join
  [probe
   probe-key-fn
   build
   build-key-fn
   theta-condition]
  (let [build-arity (arity build)]
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
                  b (if (seq bs) bs (repeat build-arity nil))]
              (join-tuples p b))))
        (with-meta
          {`tuple-seq identity
           `columns (constantly (into (columns probe) (columns build)))}))))

(defn dependent-left-join [rel added-col-count f]
  (with-meta
    (for [l (tuple-seq rel)
          :let [rs (tuple-seq (f l))]
          r (if (seq rs) rs [(repeat nil)])]
      (join-tuples l r added-col-count))
    {`tuple-seq identity
     `columns (constantly (into (columns rel) (repeat added-col-count :cinq/anon)))}))

(defn left-join [left right pred]
  (let [n (arity right)]
    (with-meta
      (for [l (tuple-seq left)
            :let [rs (filter #(pred l %) (tuple-seq right))]
            t (if (seq rs) (map #(join-tuples l %) rs) [(into l (repeat n nil))])]
        t)
      {`tuple-seq identity
       `columns (constantly (into (columns left) (columns right)))})))

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

(defn order [rel order-clauses]
  (let [order-clauses (object-array order-clauses)]
    (with-meta
      (lazy-seq
        (sort (reify Comparator
                (compare [_ t0 t1]
                  (loop [i 0]
                    (if (< i (alength order-clauses))
                      (let [[f dir] (aget order-clauses i)
                            res (compare (f t0) (f t1))]
                        (if (= 0 res)
                          (recur (inc i))
                          (case dir :desc (* -1 res) res)))
                      0))))
              (tuple-seq rel)))
      (meta rel))))

(defn limit [rel n]
  (with-meta (lazy-seq (take n (tuple-seq rel))) (meta rel)))

(defn add [rel fns]
  (let [f (fn [t] (reduce (fn [t f] (conj t (f t))) t fns))]
    (with-meta
      (lazy-seq (map f (tuple-seq rel)))
      {`tuple-seq identity
       `columns (constantly (into (columns rel) (repeat (count fns) :cinq/anon)))})))

(defn add* [rel added-col-count f]
  (with-meta
    (lazy-seq (map #(join-tuples % (f %) added-col-count) (tuple-seq rel)))
    {`tuple-seq identity
     `columns (constantly (into (columns rel) (repeat added-col-count :cinq/anon)))}))

(defn select [rel fns]
  (let [f (if (seq fns) (apply juxt fns) (constantly []))]
    (with-meta
      (lazy-seq (map f (tuple-seq rel)))
      {`tuple-seq identity
       `columns (constantly (vec (repeat (count fns) :cinq/anon)))})))

(defn ensure-tuple [v col-count]
  (assert (= col-count (count v)))
  v)

(defn select* [rel col-count f]
  (with-meta
    (lazy-seq (map #(ensure-tuple (f %) col-count) (tuple-seq rel)))
    {`tuple-seq identity
     `columns (constantly (vec (repeat col-count :cinq/anon)))}))

(defn scan [x cols]
  (let [tuple-f (if (seq cols) (apply juxt (map (fn [col] (if (= :cinq/self col) identity col)) cols)) (constantly []))]
    (with-meta
      (map tuple-f x)
      {`tuple-seq identity
       `columns (constantly (vec cols))})))

(defmacro join-condition [equi-pairs theta]
  `(and ~@(for [[a b] (partition 2 equi-pairs)]
            `(= ~a ~b))
        ~theta))

(defmacro tfn [args body] `(clojure.core/fn [~args] ~body))

(defmacro theta-tfn [args1 args2 body] `(clojure.core/fn [~args1 ~args2] ~body))

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
                     (cond
                       (symbol? binding)
                       [(add-name env binding) (list `scan (desugar x) [:cinq/self])]

                       :else (throw (ex-info "Unsupported binding form" {}))))
                   (plan-op env :join [binding x])))
               :let
               (let [bindings (partition 2 (destructure arg))
                     rf (fn [[env fns] [binding expr]]
                          [(add-name env binding) (conj fns (lambda env expr))])
                     [env fns] (reduce rf [env []] bindings)]
                 [env (list `add (vec fns))])

               (:join :left-join)
               (let [[binding expr & conditions] arg
                     where-clause
                     (case (count conditions)
                       0 true
                       1 (first conditions)
                       (list* 'and conditions))]
                 (do
                   (assert (symbol? binding))
                   [(add-name env binding)
                    (list
                      (case op
                        :join `dependent-join
                        :left-join `dependent-left-join)
                      1
                      (lambda env `(-> (scan ~(desugar expr) [:cinq/self])
                                       (where ~(lambda (add-name [{} 0] binding) where-clause)))))]))
               :where
               (let [expr arg]
                 [env (list `where (lambda env expr))])
               (if (keyword? op)
                 (throw (ex-info "Unknown operation" {}))
                 (plan-op env :from [op arg] first-op)))))

          (plan-projection [env binding expr]
            (assert (not-any? symbol? (tree-seq seqable? seq binding)) "Column names in :select must be compile-time constants")
            [(add-name env binding) expr]

            #_(cond
              (and (lookup-sym? binding) (str/ends-with? (name binding) ":."))
              (let [[kw :as kw-lookup] (parse-lookup-sym binding)]
                (do
                  (when-not (symbol? binding) (throw (ex-info "Automatic bindings are only supported if the binding is a symbol" {})))
                  [(add-name env (symbol (name kw))) kw-lookup]))

              (= '. expr)
              (if (lookup-sym? binding)
                (let [[kw :as kw-lookup] (parse-lookup-sym binding)]
                  (do
                    (when-not (symbol? binding) (throw (ex-info "Automatic bindings are only supported if the binding is a symbol" {})))
                    [(add-name env (symbol (name kw))) kw-lookup]))
                [(add-name env binding) binding])
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
            (list `tfn (first env) (desugar expr)))]
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

          projection-vec (vec (range (second selection-env) (second projection-env)))
          inverse-names (set/map-invert (first projection-env))]

      (assert (even? (count select-binding)))
      (assert (even? (count order-by-binding)))
      (assert (or (nil? limit-count) (pos-int? limit-count)))

      {:names (if select-binding
                (mapv inverse-names projection-vec)
                (mapv (comp keyword key) (sort-by val (first projection-env))))
       :plan
       (list* '->
              (concat selection-plan
                      (when order-by-binding
                        [(list `order (vec (for [[expr dir] (partition 2 order-by-binding)]
                                             (do
                                               (assert (#{:asc :desc} dir) ":order-by dir must be :asc or :desc")
                                               [(lambda selection-env expr) dir]))))])
                      (when limit-count
                        [(list `limit limit-count)])
                      (when select-binding
                        [(list `select projection)])))})))

(defmacro q
  ([selection] `(q ~selection {}))
  ([selection {:keys [select order-by limit]}]
   (let [{:keys [names, plan]} (parse [nil selection {:select select, :order-by order-by, :limit limit}])
         plan (plan/rewrite &env *ns* plan)]
     `(let [plan# ~plan]
        (with-meta
          plan#
          {`columns (constantly ~names),
           `tuple-seq (fn [_#] (tuple-seq plan#))
           :type ::results})))))

(defmacro qp
  ([selection] `(qp ~selection {}))
  ([selection {:keys [select order-by limit no-rewrite]}]
   (let [{:keys [plan]} (parse [nil selection {:select select, :order-by order-by, :limit limit}])
         plan (if no-rewrite plan (plan/rewrite &env *ns* plan))]
     (list `quote plan))))

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
  (q [n [1 2 3] n2 [1]])
  (q [n [1 2 3] :where (even? n)])

  (parse '(q [c customers]))
  (parse '(q [c customers :where (= c:customer-id 42)]))
  (parse '(q [c customers :join [o orders (= o:customer-id c:customer-id)]]))

  (qp [c customers :join [o orders (= o:customer-id c:customer-id)]])

  (qp [o orders]
      {:select [:order-id o:order-id
                :customer (first (q [c customers
                                     :where (= c:customer-id o:customer-id)]))]})

  (q [c customers])

  (let [customers [{:id 0, :firstname "fred"}
                   {:id 1, :firstname "bob"}]
        orders [{:customer-id 0, :items {"EGG" 2, "BREAD" 1}}]
        products [{:sku "EGG", :price 3.14M}, {:sku "BREAD", :price 1.49M}]]

    ;; this works
    (qp [o orders
        :join [c customers (= o:customer-id c:id)]
        oi o:items
        :let [[sku qty] oi]
        :join [p products (= p:sku sku)]]
       {:select
        [:customer-id c:id
         :firstname c:firstname
         :sku sku
         :qty qty
         :unit-price p:price
         :price (* p:price qty)]})

    ;; however destructuring should work
    #_(q [o orders
          :join [c customers (= o:customer-id c:id)]
          [sku qty] o:items
          :join [p products (= p:sku sku)]]
         {:select
          [customer-id c:id
           c:firstname .
           sku .
           qty .
           unit-price p:price
           price (* p:price qty)]})

    )

  )

(defn vars []
  (q [lib (loaded-libs)
      public (ns-publics lib)
      :let [[sym var] public]]
     {:select
      [:ns lib
       :name (name sym)]
      :order-by
      [lib :asc,
       (name sym) :asc]}))

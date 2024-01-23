(ns com.wotbrew.cinq.plan2
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [meander.epsilon :as m]
            [meander.strategy.epsilon :as r]
            [com.stuartsierra.dependency :as dep]))

(declare arity column-map columns)

(defn arity [ra] (count (columns ra)))

(defn column-map [ra]
  (let [cols (columns ra)]
    (zipmap cols (range (count cols)))))

(def ^:dynamic *gensym* gensym)

(defn unique-ify* [ra]
  (m/match ra
    [::scan ?src ?bindings]
    (let [smap (into {} (map (fn [[sym]] [sym (*gensym* (name sym))]) ?bindings))]
      [[::scan ?src (mapv (fn [[sym e]] [(smap sym) e]) ?bindings)] smap])

    [::where ?ra ?pred]
    (let [[ra smap] (unique-ify* ?ra)]
      [[::where ra (walk/postwalk-replace smap ?pred)] smap])

    [::project ?ra ?binding]
    (let [[ra smap] (unique-ify* ?ra)]
      [[::project ra (walk/postwalk-replace smap ?binding)] smap])

    [::apply ?mode ?left ?right]
    (let [[left left-smap] (unique-ify* ?left)
          [right right-smap] (unique-ify* ?right)
          right (walk/postwalk-replace left-smap right)]
      [[::apply ?mode left right] (merge left-smap right-smap)])

    [::join ?left ?right ?pred]
    (let [[left left-smap] (unique-ify* ?left)
          [right right-smap] (unique-ify* ?right)
          smap (merge left-smap right-smap)
          pred (walk/postwalk-replace smap ?pred)]
      [[::join left right pred] smap])

    [::group-by ?ra ?bindings]
    (let [[ra inner-smap] (unique-ify* ?ra)
          new-smap (into {} (map (fn [[sym]] [sym (*gensym* (name sym))]) ?bindings))
          bindings (mapv (fn [[s e]] [(new-smap s) (walk/postwalk-replace inner-smap e)]) ?bindings)]
      [[::group-by ra bindings] (merge inner-smap new-smap)])

    [::order-by ?ra ?clauses]
    (let [[ra smap] (unique-ify* ?ra)]
      [[::order-by ra (walk/postwalk-replace smap ?clauses)] smap])

    [::limit ?ra ?n]
    (let [[ra smap] (unique-ify* ?ra)]
      [[::limit ra ?n] smap])

    [::let ?ra ?bindings]
    (let [[ra inner-smap] (unique-ify* ?ra)
          rf (fn [[bindings smap] [sym e]]
               (let [new-sym (*gensym* (name sym))]
                 [(conj bindings [new-sym (walk/postwalk-replace smap e)])
                  (assoc smap sym new-sym)]))
          [bindings smap] (reduce rf [[] inner-smap] ?bindings)]
      [[::let ra bindings] smap])

    _ [ra {}]))

(defn unique-ify [ra]
  (first (unique-ify* ra)))

(defn columns [ra]
  (m/match ra
    [::scan _ ?cols]
    (mapv first ?cols)

    [::where ?ra _]
    (columns ?ra)

    [::project ?ra ?bindings]
    (mapv first ?bindings)

    [::apply ?mode ?left ?right]
    (into (columns ?left) (columns ?right))

    [::join ?left ?right _]
    (into (columns ?left) (columns ?right))

    [::semi-join ?left ?right _]
    (columns ?left)

    [::left-join ?left ?right _]
    (into (columns ?left) (columns ?right))

    [::single-join ?left ?right _]
    (into (columns ?left) (columns ?right))

    [::group-by ?ra ?bindings]
    (let [cols (columns ?ra)]
      (conj (into cols (mapv first ?bindings)) '%count))

    [::order-by ?ra _]
    (columns ?ra)

    [::limit ?ra _]
    (columns ?ra)

    [::let ?ra ?bindings]
    (let [cols (columns ?ra)]
      (into cols (map first ?bindings)))))

(defn dependent-cols* [cmap expr]
  (let [dmap (atom {})]
    ((fn ! [expr]
       (m/match expr
         [::scan ?expr ?bindings]
         (do
           (! ?expr)
           (dotimes [i (count ?bindings)]
             (! (nth (nth ?bindings i) 1))))
         _
         (cond
           (symbol? expr) (when (cmap expr) (swap! dmap conj (find cmap expr)))
           (seq? expr) (run! ! expr)
           (vector? expr) (run! ! expr)
           (map? expr) (run! ! expr)
           (set? expr) (run! ! expr)
           (map-entry? expr) (run! ! expr)
           :else nil))) expr)
    @dmap))

(defn dependent-cols [ra expr] (dependent-cols* (column-map ra) expr))

(defn dependent? [ra expr]
  (boolean (seq (dependent-cols ra expr))))

(defn not-dependent? [ra expr]
  (empty? (dependent-cols ra expr)))

(defn pushable-side
  "Return a sub relation index (1, 2) for joins, left-joins, products if the predicate can be pushed into one of these sub relations.
  nil if not."
  [ra pred]
  (m/match ra
    (m/or [::cross-join ?a ?b]
          [::join ?a ?b _])
    (cond
      (not-dependent? ?b pred) 1
      (not-dependent? ?a pred) 2
      :else nil)

    [::left-join ?a ?b _]
    (cond
      (not-dependent? ?b pred) 1
      :else nil)

    (m/and [::apply ?mode ?a ?b]
           (m/guard (= ?mode :cross-join)))
    (cond
      (not-dependent? ?b pred) 2
      (not-dependent? ?a pred) 3
      :else nil)

    [::apply ?mode ?a ?b]
    (cond
      (not-dependent? ?b pred) 2
      :else nil)

    [::where ?ra _]
    (when (pushable-side ?ra pred)
      1)

    [::let ?ra ?bindings]
    (when (empty? (dependent-cols* (into {} ?bindings) pred))
      1)

    [::group-by ?ra ?bindings]
    (let [group-cols (dependent-cols ?ra pred)
          binding-set (set (map first ?bindings))]
      (when (->> group-cols
                 keys
                 (remove binding-set)
                 empty?)
        1))

    _ nil))

(defn split-dependent-clauses [ra clauses]
  ((juxt filter remove) #(dependent? ra %) clauses))

(defn conjoin-predicates [pred-a pred-b]
  (m/match [pred-a pred-b]
    [true true]
    true

    [?a true]
    ?a

    [true ?b]
    ?b

    [[::and & ?clauses-a] [::and & ?clauses-b]]
    (into [::and] cat [?clauses-a, ?clauses-b])

    [[::and & ?clauses-a] ?b]
    (into [::and] cat [?clauses-a, [?b]])

    [?a [::and & ?clauses-b]]
    (into [::and] cat [[?a], ?clauses-b])

    _ [::and pred-a pred-b]))

(defn push-predicate [ra pred]
  (if-some [side (pushable-side ra pred)]
    (update ra side (fn [sub-ra] [::where sub-ra pred]))
    #_(m/match ra
        [::join ?a ?b ?pred]
        (case side
          2 [::join [::where ?b pred] ?a ?pred]
          [::join [::where ?a pred] ?b ?pred])

        [::cross-join ?a ?b]
        (case side
          2 [::cross-join [::where ?b pred] ?a]
          [::cross-join [::where ?a pred] ?b])

        _
        (update ra side (fn [sub-ra] [::where sub-ra pred])))
    ra))

(defn find-lookups [expr]
  (filter #(and (vector? %) (= ::lookup (nth % 0 nil))) (tree-seq seqable? seq expr)))

(defn lookup-sym [lookup]
  (let [[_ kw a] lookup]
    (if (namespace kw)
      (symbol (str (name a) ":" (namespace kw)) (name kw))
      (symbol (str (name a) ":" (name kw))))))

(defn push-lookups* [ra lookups]
  (m/match ra
    [::scan ?src ?bindings]
    (let [self-sym (some (fn [[sym k]] (when (= :cinq/self k) sym)) ?bindings)
          matching-lookups (filter (fn [[_ kw s]] (= self-sym s)) lookups)]
      [[::scan ?src (into ?bindings (map (fn [[_ kw :as lookup]] [(lookup-sym lookup) kw])) matching-lookups)]
       (into {} (map (fn [lookup] [lookup (lookup-sym lookup)]) matching-lookups))])

    [::where ?ra ?pred]
    (let [expr-lookups (find-lookups ?pred)
          [ra smap] (push-lookups* ?ra (into lookups expr-lookups))]
      [[::where ra (walk/postwalk-replace smap ?pred)] smap])

    [::project ?ra ?bindings]
    (let [expr-lookups (mapcat #(find-lookups (second %)) ?bindings)
          [ra smap] (push-lookups* ?ra (into lookups expr-lookups))]
      [[::project ra (mapv (fn [[k e]] [k (walk/postwalk-replace smap e)]) ?bindings)] smap])

    [::apply ?mode ?left ?right]
    (let [[right-ra right-smap] (push-lookups* ?right lookups)
          [left-ra left-smap] (push-lookups* ?left (set/difference lookups (set (keys right-smap))))]
      [[::apply ?mode left-ra right-ra] (merge left-smap right-smap)])

    [::semi-join ?left ?right ?pred]
    (let [expr-lookups (find-lookups ?pred)
          lookups (into lookups expr-lookups)
          [right-ra right-smap] (push-lookups* ?right (set expr-lookups))
          [left-ra left-smap] (push-lookups* ?left lookups)]
      [[::semi-join left-ra right-ra (walk/postwalk-replace (merge left-smap right-smap) ?pred)] left-smap])

    [::join ?left ?right ?pred]
    (let [expr-lookups (find-lookups ?pred)
          lookups (into lookups expr-lookups)
          [right-ra right-smap] (push-lookups* ?right lookups)
          [left-ra left-smap] (push-lookups* ?left (set/difference lookups (set (keys right-smap))))
          smap (merge left-smap right-smap)]
      [[::join left-ra right-ra (walk/postwalk-replace smap ?pred)] smap])

    [::left-join ?left ?right ?pred]
    (let [expr-lookups (find-lookups ?pred)
          lookups (into lookups expr-lookups)
          [right-ra right-smap] (push-lookups* ?right lookups)
          [left-ra left-smap] (push-lookups* ?left (set/difference lookups (set (keys right-smap))))
          smap (merge left-smap right-smap)]
      [[::left-join left-ra right-ra (walk/postwalk-replace smap ?pred)] smap])

    [::group-by ?ra ?bindings]
    (let [expr-lookups (find-lookups (map second ?bindings))
          lookups (into lookups expr-lookups)
          [ra smap] (push-lookups* ?ra lookups)
          binding-syms (set (map first ?bindings))
          matching-lookups (filter (fn [[_ kw s]] (binding-syms s)) lookups)
          extra-bindings (mapv (fn [lookup] [(smap lookup) (smap lookup)]) matching-lookups)
          new-bindings (mapv (fn [[sym e]] [sym (walk/postwalk-replace smap e)]) ?bindings)]
      [[::group-by ra (into new-bindings extra-bindings)] smap])

    [::order-by ?ra ?clauses]
    (let [expr-lookups (find-lookups (map first ?clauses))
          lookups (into lookups expr-lookups)
          [ra smap] (push-lookups* ?ra lookups)]
      [[::order-by ra (mapv (fn [[e dir]] [(walk/postwalk-replace smap e) dir]) ?clauses)] smap])

    [::limit ?ra ?n]
    (let [[ra smap] (push-lookups* ?ra lookups)]
      [[::limit ra ?n] smap])

    [::let ?ra ?bindings]
    (let [expr-lookups (find-lookups (map second ?bindings))
          lookups (into lookups expr-lookups)
          [ra smap] (push-lookups* ?ra lookups)]
      [[::let ra (mapv (fn [[sym e]] [sym (walk/postwalk-replace smap e)]) ?bindings)] smap])

    _ [ra {}]))

(defn push-lookups [ra]
  (first (push-lookups* ra #{})))

(def rewrites
  (r/match
    ;; region de-correlation rule 1
    ;; apply(T, R, E) = join(T, E, true)
    ;; if E not correlated with R
    (m/and [::apply ?mode ?left ?right]
           (m/guard (#{:cross-join :left-join :single-join} ?mode))
           (m/guard (not-dependent? ?left ?right)))
    ;; =>
    [(case ?mode
       :cross-join ::join
       :left-join ::left-join
       :single-join ::single-join)
     ?left
     ?right
     true]
    ;; endregion

    ;; region de-correlation rule 2
    ;; apply(T, R, select(E, p)) = join(T, E, p)
    ;; if E not correlated with R
    (m/and [::apply ?mode ?left [::where ?right ?pred]]
           (m/guard (#{:cross-join :left-join :single-join} ?mode))
           (m/guard (not-dependent? ?left ?right)))
    ;; =>
    [(case ?mode
       :cross-join ::join
       :left-join ::left-join
       :single-join ::single-join)
     ?left
     ?right
     ?pred]
    ;; endregion

    ;; region de-correlation rule 3
    ;; apply(cross-join, R, select(E, p)) = select(cross-join(T, E), p)
    [::apply :cross-join ?a [::where ?b ?pred]]
    [::where [::join ?a ?b true] ?pred]
    ;; endregion

    ;; region de-correlation rule 9
    ;; todo agg behaviour is not the same as SQL in cinq
    ;; this rule likely does not apply as group-by returns an empty rel for an empty in
    ;; sum, + etc do not accept nil inputs
    [::apply :single-join ?left [::project [::group-by ?right ?group-binding] [[?sym ?expr]]]]
    [::let
     [::group-by
      [::apply :left-join ?left ?right]
      (into (vec (for [sym (columns ?left)] [sym sym])) ?group-binding)]
     [[?sym ?expr]]]
    ;; endregion

    ;; poor mans semi join
    (m/and [::where [::apply :single-join ?left [::project [::where ?right ?pred] [[?sym true]]]] ?sym]
           (m/guard (not-dependent? ?left ?right)))
    [::semi-join ?left ?right ?pred]

    ;; region non dependent pred past join
    (m/and [::join ?left ?right ?pred]
           (m/guard (not-dependent? ?right ?pred)))
    ;; =>
    [::join [::where ?left ?pred] ?right true]

    (m/and [::join ?left ?right ?pred]
           (m/guard (not-dependent? ?left ?pred)))
    ;; =>
    [::join ?left [::where ?right ?pred] true]
    ;; endregion

    ;; region noop where
    [::where ?ra true]
    ?ra
    ;; endregion

    ;; region push predicate past joins
    (m/and [::where ?ra ?pred]
           (m/guard (pushable-side ?ra ?pred)))
    (push-predicate ?ra ?pred)
    ;; endregion

    ;; region split predicate
    [::where ?ra [::and & ?clause]]
    (reduce (fn [ra clause] [::where ra clause]) ?ra ?clause)
    ;; endregion

    ;;region push predicate into join
    [::where [::join ?a ?b ?pred-a] ?pred-b]
    [::join ?a ?b (conjoin-predicates ?pred-a ?pred-b)]
    ;; endregion

    #_#_(m/and [::join [::join ?a ?b ?pred-a] ?c ?pred-b]
               (m/guard (not-dependent? ?b ?pred-b)))
            [::join [::join ?a ?c ?pred-b] ?b ?pred-a]

    ))

(defn fix-max
  "Fixed point strategy combinator with a maximum iteration count.
  See r/fix."
  [s n]
  (fn [t]
    (loop [t t
           i 0]
      (let [t* (s t)]
        (cond
          (= t* t) t*
          (= i n) t*
          :else (recur t* (inc i)))))))

(def fuse
  (r/match
    [::where ?ra true]
    ?ra

    [::where [::where ?ra ?pred-a] ?pred-b]
    [::where ?ra (conjoin-predicates ?pred-a ?pred-b)]

    ))

(def rewrite-logical
  (-> #'rewrites
      r/attempt
      r/top-down
      (fix-max 100)))

(defn conjoin-predicate-list [pred-list pred]
  (m/match pred
    true pred-list
    [::and & ?clauses] (reduce conjoin-predicate-list pred-list ?clauses)
    _ (conj pred-list pred)))

(def join-collect
  (r/match

    [::join [::join* ?rels-a ?preds-a] [::join* ?rels-b ?preds-b] ?pred]
    [::join* (into ?rels-a ?rels-b) (conjoin-predicate-list (into ?preds-a ?preds-b) ?pred)]

    [::join [::join* ?rels-a ?preds-a] ?b ?pred]
    [::join* (conj ?rels-a ?b) (conjoin-predicate-list ?preds-a ?pred)]

    [::join ?a [::join* ?rels-b ?preds-b] ?pred]
    [::join* (vec (cons ?a ?rels-b)) (into (conjoin-predicate-list [] ?pred) ?preds-b)]

    [::join ?a ?b ?pred]
    [::join* [?a ?b] (conjoin-predicate-list [] ?pred)]

    ))

(def rewrite-fuse
  (-> #'fuse
      r/attempt
      r/bottom-up
      (fix-max 100)))

(def rewrite-join-collect
  (-> #'join-collect
      r/attempt
      r/bottom-up
      (fix-max 100)))

(def rewrite-join-order
  "Depends on join-collect"
  (-> (r/match
        [::join* ?rels ?preds]
        (do (assert (seq ?rels))
            ;; sort each unsatisfied predicate by how many relations do I need to add to satisfy the join
            (loop [unsatisfied-predicates ?preds
                   pending-relations (set ?rels)
                   ra nil
                   ;; todo not sure if we need outer-where, check the loop
                   outer-where []]
              (if (seq unsatisfied-predicates)
                (let [dependent-relations (fn [pred] (filter #(dependent? % pred) pending-relations))
                      cost (fn [pred] (count (dependent-relations pred)))
                      [pred & unsatisfied-predicates] (sort-by cost unsatisfied-predicates)
                      add-relations (dependent-relations pred)]
                  (recur unsatisfied-predicates
                         (reduce disj pending-relations add-relations)
                         (let [ra (reduce (fn [ra rel] (if (nil? ra) rel [::join ra rel true])) ra add-relations)]
                           (m/match ra
                             [::join ?left ?right ?pred]
                             [::join ?left ?right (conjoin-predicates ?pred pred)]

                             [::where ?left ?pred]
                             [::where ?left (conjoin-predicates ?pred pred)]

                             ?ra
                             [::where ?ra pred]))
                         outer-where))
                (let [ra (reduce (fn [ra rel] (if (nil? ra) rel [::join ra rel true])) ra pending-relations)]
                  (if (seq outer-where)
                    [::where ra (reduce conjoin-predicates (first outer-where) (rest outer-where))]
                    ra))))))
      r/attempt
      r/bottom-up))

(def unique-ify-sub-queries
  (-> (r/match [::scalar-sq ?ra] [::scalar-sq (unique-ify ?ra)])
      r/attempt
      r/bottom-up))

(def push-lookups-sub-queries
  (-> (r/match [::scalar-sq ?ra] [::scalar-sq (push-lookups ?ra)])
      r/attempt
      r/bottom-up))

(def substitute-sub-queries
  ;; walk ra, replace sub queries with symbols, wrap with apply
  ;; PROBLEM: local envs let/fn/reify clauses over sub queries in expressions would cause sub queries to capture different scopes
  ;; SOLUTION 1: disallow let? disallow fn? ...
  ;; SOLUTION 2: detect new scopes, let, fn and reify. subqueries within these scopes cannot be turned into joins
  ;;             e.g rewrite let as [::plan/local-let ...] etc.
  ;;             problem: macros introducing variables - this is already an issue for column shadowing, but actually you could normally use these
  (r/match
    [::where ?ra ?pred]
    (let [replacement-syms (atom {})
          replacement-sym (fn [sq]
                            (or (@replacement-syms sq)
                                (let [sym (*gensym* "scalar-subquery")]
                                  (swap! replacement-syms assoc sq sym)
                                  sym)))
          all (-> (r/match
                    (m/and [::scalar-sq [::project ?ra [[?col ?expr]]]] ?sq)
                    (replacement-sym ?sq))
                  r/attempt
                  r/top-down)
          pred (all ?pred)
          ra (reduce
               (fn [ra [sq sym]]
                 (m/match sq
                   [::scalar-sq [::project ?sq [[?col ?expr]]]]
                   [::apply :single-join ra [::project ?sq [[sym ?expr]]]]))
               ?ra
               @replacement-syms)]
      [::where ra pred])))

(def rewrite-sub-queries
  (-> #'substitute-sub-queries
      r/attempt
      r/top-down))

(def ^:dynamic *pull-correlated-relation*)

(defn correlated*? [expr]
  (dependent? *pull-correlated-relation* expr))

(def pull-correlated-selects
  (r/match
    (m/and
      [::where [::where ?ra ?pred2] ?pred1]
      (m/guard (and (correlated*? ?pred2)
                    (not (correlated*? ?pred1)))))
    [::where [::where ?ra ?pred1] ?pred2]

    (m/and [::join [::where ?left ?pred] ?right ?join-pred]
           (m/guard (correlated*? ?pred)))
    [::where [::join ?left ?right ?join-pred] ?pred]

    (m/and [::join ?left [::where ?right ?pred] ?join-pred]
           (m/guard (correlated*? ?pred)))
    [::where [::join ?left ?right ?join-pred] ?pred]

    ))

(def rewrite-pull-correlated-select
  (-> (r/match
        [::apply ?mode ?left ?right]
        (binding [*pull-correlated-relation* ?left]
          [::apply ?mode ?left
           ((-> pull-correlated-selects
                r/attempt
                r/bottom-up
                (fix-max 100))
            ?right)]))
      r/attempt
      r/top-down))

(defn rewrite [ra]
  (-> ra
      unique-ify-sub-queries
      unique-ify
      rewrite-sub-queries
      rewrite-logical
      rewrite-pull-correlated-select
      rewrite-logical
      rewrite-fuse
      rewrite-join-collect
      rewrite-join-order
      push-lookups-sub-queries
      push-lookups))

(defn stack-view [ra]
  (-> ((fn ! [ra]
         (m/match ra
           [::scan ?src ?bindings]
           [[:scan ?src ?bindings]]

           [::join* ?rels ?preds]
           [:join* ?rels ?preds]

           [::join ?a ?b ?pred]
           (conj (! ?a) [:join (! ?b) ?pred])

           [::join ?a ?b ?pred]
           (conj (! ?a) [:join (! ?b) ?pred])

           [::left-join ?a ?b ?pred]
           (conj (! ?a) [:left-join (! ?b) ?pred])

           [::apply ?mode ?a ?b]
           (conj (! ?a) [:apply ?mode (! ?b)])

           [?op ?ra & ?args]
           (conj (! ?ra) (into [(keyword (name ?op))] ?args))))
       ra)

      ((-> (r/match
             [::lookup ?kw ?s nil]
             (list ?kw ?s)
             [::lookup ?kw ?s ?default]
             (list ?kw ?s ?default)
             [::= ?a ?b]
             (list '= ?a ?b)
             [::and & ?clauses]
             (list* 'and ?clauses)
             )
           r/attempt
           r/bottom-up))

      ))

(defn equi-theta [left right pred]
  (m/match pred
    [::and & ?clauses]
    (apply merge-with into (map #(equi-theta left right %) ?clauses))

    [::= ?a ?b]
    (m/match [(dependent? left ?a) (dependent? right ?a)
              (dependent? left ?b) (dependent? right ?b)]
      [true false false true] {:left-key [?a], :right-key [?b]}
      [false true true false] {:left-key [?b], :right-key [?a]}
      _ {:theta [pred]})

    ?pred
    {:theta [?pred]}))

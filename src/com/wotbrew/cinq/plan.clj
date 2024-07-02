(ns com.wotbrew.cinq.plan
  (:require [clojure.set :as set]
            [clojure.walk :as walk]
            [com.wotbrew.cinq.expr :as expr]
            [meander.epsilon :as m]
            [com.wotbrew.cinq.column]
            [meander.strategy.epsilon :as r])
  (:import (clojure.lang IRecord)
           (com.wotbrew.cinq CinqUtil)
           (com.wotbrew.cinq.column Column DoubleColumn LongColumn)
           (java.lang.reflect Field)))

(declare arity column-map columns)

(defn arity [ra] (count (columns ra)))

(defn column-map [ra]
  (let [cols (columns ra)]
    (zipmap cols (range (count cols)))))

(def ^:dynamic *gensym* gensym)

(defn unique-ify* [ra]
  (m/match ra
    [::scan ?src ?bindings]
    (let [smap (into {} (map (fn [[sym]] [sym (with-meta (*gensym* (name sym)) (meta sym))]) ?bindings))]
      [[::scan ?src (mapv (fn [[sym e pred]] [(smap sym) e pred]) ?bindings)] smap])

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

    [::union ?ras]
    (let [all (mapv (fn [ra] (unique-ify* ra)) ?ras)
          smap (apply merge (map second all))]
      [[::union (mapv first all)] smap])

    [::cte ?bindings ?ra]
    (let [all (mapv (fn [[sym ra]] (let [[ra smap] (unique-ify* ra)] [[sym ra] smap])) ?bindings)
          [ra ra-smap] (unique-ify* ?ra)
          smap (apply merge (concat (map second all) [ra-smap]))]
      [[::cte (mapv first all) ra] smap])

    [::group-by ?ra ?bindings]
    (let [[ra inner-smap] (unique-ify* ?ra)
          new-smap (into {} (map (fn [[sym]] [sym (*gensym* (name sym))]) ?bindings))
          bindings (mapv (fn [[s e]] [(new-smap s) (walk/postwalk-replace inner-smap e)]) ?bindings)]
      [[::group-by ra bindings] (merge inner-smap new-smap)])

    [::order-by ?ra ?clauses]
    (let [[ra smap] (unique-ify* ?ra)]
      [[::order-by ra (walk/postwalk-replace smap ?clauses)] smap])

    [::limit ?ra ?n ?box]
    (let [[ra smap] (unique-ify* ?ra)]
      [[::limit ra ?n ?box] smap])

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

(defn self-class [scan-bindings]
  (let [self-binding (last (keep #(when (= :cinq/self (second %)) (first %)) scan-bindings))
        self-tag (:tag (meta self-binding))]
    (if (symbol? self-tag) (resolve self-tag) self-tag)))

(defn kw-field [^Class class kw]
  (when (isa? class IRecord)
    (let [munged-name (munge (name kw))]
      (->> (.getFields class)
           (some (fn [^Field f] (when (= munged-name (.getName f)) f)))))))

(def %count-sym '%count)

(def ^:dynamic *specialise-group-column-types*
  "Bind to false if you do not want to type hint group columns (aggregate macros use these hints to assume array types)
   to always assume boxed Column arrays."
  true)

(defn group-column-type [column-sym]
  (if *specialise-group-column-types*
    (condp = (:tag (meta column-sym))
      'double `DoubleColumn
      'long `LongColumn
      `Column)
    `Column))

(defn group-column-tag [column-sym]
  (if (= %count-sym column-sym)
    column-sym
    (vary-meta column-sym assoc :tag (group-column-type column-sym))))

(defn optional-tag [column-sym] (vary-meta column-sym dissoc :tag))

(defn resolve-tag [column-sym]
  (if-some [tag (:tag (meta column-sym))]
    (condp = tag
      'byte column-sym
      'short column-sym
      'int column-sym
      'long column-sym
      'double column-sym
      'float column-sym
      'boolean column-sym
      (let [rt (resolve tag)]
        (if (class? rt)
          (vary-meta column-sym assoc :tag (symbol (.getName ^Class rt)))
          column-sym)))
    column-sym))

(defonce union-out-col (gensym "cinq-union-out"))

(defn columns [ra]
  (m/match ra
    [::scan _ ?cols]
    (let [self-class (self-class ?cols)]
      (if (isa? self-class IRecord)
        (mapv (fn [[col k _pred]]
                (resolve-tag
                  (if-some
                    [new-tag
                     (when (keyword? k)
                       (if-some [^Field f (kw-field self-class k)]
                         (condp = (.getType f)
                           Byte/TYPE 'byte
                           Short/TYPE 'short
                           Integer/TYPE 'int
                           Long/TYPE 'long
                           Double/TYPE 'double
                           Float/TYPE 'float
                           Boolean/TYPE 'boolean
                           (condp = (.getComponentType (.getType f))
                             Byte/TYPE 'bytes
                             Short/TYPE 'shorts
                             Integer/TYPE 'ints
                             Long/TYPE 'longs
                             Double/TYPE 'doubles
                             Float/TYPE 'floats
                             Boolean/TYPE 'booleans
                             (symbol (.getName (.getType f)))))))]
                    ;; keep old tag if it resolves
                    (vary-meta col (fn [m] (if (:tag m) m (assoc m :tag new-tag))))
                    col)))
              ?cols)
        (mapv (fn [[col]] (resolve-tag col)) ?cols)))

    [::where ?ra _]
    (columns ?ra)

    [::project ?ra ?bindings]
    (mapv first ?bindings)

    [::group-project ?ra ?bindings ?aggregates ?projection]
    (mapv first ?projection)

    [::apply :left-join ?left ?right]
    (into (columns ?left) (mapv optional-tag (columns ?right)))

    [::apply ?mode ?left ?right]
    (into (columns ?left) (columns ?right))

    [::join ?left ?right _]
    (into (columns ?left) (columns ?right))

    [::semi-join ?left ?right _]
    (columns ?left)

    [::anti-join ?left ?right _]
    (columns ?left)

    [::left-join ?left ?right _]
    (into (columns ?left) (mapv optional-tag (columns ?right)))

    [::single-join ?left ?right _]
    (into (columns ?left) (mapv optional-tag (columns ?right)))

    [::group-by ?ra ?bindings]
    (let [cols (columns ?ra)]
      ;; todo Column type
      (conj (into (mapv group-column-tag cols)
                  (mapv first ?bindings))
            %count-sym))

    [::order-by ?ra _]
    (columns ?ra)

    [::limit ?ra _ ?box]
    (columns ?ra)

    [::let ?ra ?bindings]
    (let [cols (columns ?ra)]
      (into cols (map first ?bindings)))

    [::without ?ra ?not-needed]
    (let [cols (columns ?ra)]
      (filterv (complement (set ?not-needed)) cols))

    [::cte ?bindings ?ra] (columns ?ra)

    [::union ?ras] [union-out-col]

    _ (throw (ex-info "Not sure how to get columns from ra" {:ra ra}))))

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
           (vector? expr)
           (if (= [::count] expr)
             (when (cmap %count-sym)
               (swap! dmap conj (find cmap %count-sym)))
             (run! ! expr))
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
  (when (expr/pred-can-be-reordered? pred)
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

      _ nil)))

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
  (let [[_ kw a t] lookup]
    (if (namespace kw)
      (with-meta (symbol (str (name a) ":" (namespace kw) ":" (name kw))) {:tag t})
      (with-meta (symbol (str (name a) ":" (name kw))) {:tag t}))))

(defn push-lookups* [ra lookups]
  (m/match ra
    [::scan ?src ?bindings]
    (let [self-sym (some (fn [[sym k]] (when (= :cinq/self k) sym)) ?bindings)
          matching-lookups (filter (fn [[_ _ s]] (= self-sym s)) lookups)]
      [[::scan ?src (into ?bindings (map (fn [[_ kw :as lookup]] [(lookup-sym lookup) kw true])) matching-lookups)]
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
          expr-lookups (find-lookups ?right)
          lookups (into lookups (set/difference (set expr-lookups) (set (keys right-smap))))
          [left-ra left-smap] (push-lookups* ?left lookups)
          smap (merge left-smap right-smap)]
      [[::apply ?mode left-ra (walk/postwalk-replace smap right-ra)] smap])

    [::semi-join ?left ?right ?pred]
    (let [expr-lookups (find-lookups ?pred)
          lookups (into lookups expr-lookups)
          [right-ra right-smap] (push-lookups* ?right (set expr-lookups))
          [left-ra left-smap] (push-lookups* ?left lookups)]
      [[::semi-join left-ra right-ra (walk/postwalk-replace (merge left-smap right-smap) ?pred)] left-smap])

    [::anti-join ?left ?right ?pred]
    (let [expr-lookups (find-lookups ?pred)
          lookups (into lookups expr-lookups)
          [right-ra right-smap] (push-lookups* ?right (set expr-lookups))
          [left-ra left-smap] (push-lookups* ?left lookups)]
      [[::anti-join left-ra right-ra (walk/postwalk-replace (merge left-smap right-smap) ?pred)] left-smap])

    [::join ?left ?right ?pred]
    (let [expr-lookups (find-lookups ?pred)
          lookups (into lookups expr-lookups)
          [right-ra right-smap] (push-lookups* ?right lookups)
          [left-ra left-smap] (push-lookups* ?left (set/difference lookups (set (keys right-smap))))
          smap (merge left-smap right-smap)]
      [[::join left-ra right-ra (walk/postwalk-replace smap ?pred)] smap])

    [::single-join ?left ?right ?pred]
    (let [expr-lookups (find-lookups ?pred)
          lookups (into lookups expr-lookups)
          [right-ra right-smap] (push-lookups* ?right lookups)
          [left-ra left-smap] (push-lookups* ?left (set/difference lookups (set (keys right-smap))))
          smap (merge left-smap right-smap)]
      [[::single-join left-ra right-ra (walk/postwalk-replace smap ?pred)] smap])

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

    [::limit ?ra ?n ?box]
    (let [[ra smap] (push-lookups* ?ra lookups)]
      [[::limit ra ?n ?box] smap])

    [::let ?ra ?bindings]
    (let [expr-lookups (find-lookups (map second ?bindings))
          lookups (into lookups expr-lookups)
          [ra smap] (push-lookups* ?ra lookups)]
      [[::let ra (mapv (fn [[sym e]] [sym (walk/postwalk-replace smap e)]) ?bindings)] smap])

    _ [ra {}]))

(defn push-lookups [ra]
  (first (push-lookups* ra #{})))

(defn pred-scan-binding [scan-bindings pred]
  (let [binding-idx (into {} (map-indexed (fn [i [sym]] [sym i])) scan-bindings)
        scan-bin-ops #{::= ::< ::<= ::> ::>=}]
    (m/match pred
      (m/and [?op ?a ?b]
             (m/guard (and (not (binding-idx ?a))
                           (symbol? ?b)
                           (scan-bin-ops ?op)
                           (binding-idx ?b))))
      (binding-idx ?b)

      (m/and [?op ?b ?a]
             (m/guard (and (not (binding-idx ?a))
                           (symbol? ?b)
                           (scan-bin-ops ?op)
                           (binding-idx ?b))))
      (binding-idx ?b)

      _ nil)))

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

    (m/and [::apply ?mode ?left [::where ?right [::and & ?clauses]]]
           (m/guard (#{:cross-join :left-join :single-join} ?mode))
           (m/guard (not-dependent? ?left ?right)))
    ;; =>
    (let [{dependent true, not-dependent false} (group-by #(dependent? ?left %) ?clauses)]
      [(case ?mode
         :cross-join ::join
         :left-join ::left-join
         :single-join ::single-join)
       ?left
       (case (count not-dependent)
         0 ?right
         1 [::where ?right (first not-dependent)]
         [::where ?right (into [::and] not-dependent)])
       (case (count dependent)
         0 true
         1 (first dependent)
         (into [::and] dependent))])

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
    ;; TODO verify this
    ;; apply(cross-join, R, select(E, p)) = select(cross-join(T, E), p)
    [::apply :cross-join ?a [::where ?b ?pred]]
    [::where [::apply :cross-join ?a ?b] ?pred]
    ;; endregion

    ;; region de-correlation rule 9
    [::apply :single-join ?left [::project [::group-by ?right ?group-binding] [[?sym ?expr]]]]
    [::let
     [::group-by
      [::apply :left-join ?left ?right]
      (into (vec (for [sym (columns ?left)] [sym sym])) ?group-binding)]
     [[?sym ?expr]]]
    ;; endregion

    ;; region semi/anti
    ;; poor mans version for now
    (m/and [::where
            [::apply :single-join ?left
             [::project
              [::where ?right ?pred]
              [[?sym true]]]]
            ?sym]
           (m/guard (not-dependent? ?left ?right)))
    [::semi-join ?left ?right ?pred]
    (m/and [::where
            [::apply :single-join ?left
             [::project
              [::where ?right ?pred]
              [[?sym true]]]]
            [::not ?sym]]
           (m/guard (not-dependent? ?left ?right)))
    [::anti-join ?left ?right ?pred]
    ;;endregion

    ;; region non dependent pred past join

    (m/and [::join ?left ?right ?pred]
           (m/guard (not-dependent? ?right ?pred))
           (m/guard (expr/pred-can-be-reordered? ?pred)))
    ;; =>
    [::join [::where ?left ?pred] ?right true]

    (m/and [::join ?left ?right [::and & ?preds]]
           (m/guard (some #(and (not-dependent? ?right %)
                                (expr/pred-can-be-reordered? %))
                          ?preds)))
    ;; =>
    (let [[push-left keep-pred] ((juxt filter remove) #(and (not-dependent? ?right %) (expr/pred-can-be-reordered? %)) ?preds)]
      [::join [::where ?left (reduce conjoin-predicates true push-left)]
       ?right
       (reduce conjoin-predicates true keep-pred)])

    (m/and [::join ?left ?right ?pred]
           (m/guard (not-dependent? ?left ?pred))
           (m/guard (expr/pred-can-be-reordered? ?pred)))
    ;; =>
    [::join ?left [::where ?right ?pred] true]

    (m/and [::join ?left ?right [::and & ?preds]]
           (m/guard (some #(and (not-dependent? ?left %)
                                (expr/pred-can-be-reordered? %))
                          ?preds)))
    ;; =>
    (let [[push-right keep-pred] ((juxt filter remove) #(and (not-dependent? ?right %) (expr/pred-can-be-reordered? %)) ?preds)]
      [::join ?left
       [::where ?right (reduce conjoin-predicates true push-right)]
       (reduce conjoin-predicates true keep-pred)])

    ;; endregion

    ;; region where fusion
    [::where ?ra true]
    ?ra

    [::and ?pred] ?pred

    [::where [::where ?ra ?pred-a] ?pred-b]
    [::where ?ra (conjoin-predicates ?pred-a ?pred-b)]
    ;; endregion

    ;; region push predicate past joins
    (m/and [::where ?ra ?pred]
           (m/guard (pushable-side ?ra ?pred)))
    (push-predicate ?ra ?pred)

    (m/and [::where ?ra [::and & ?preds]]
           (m/guard (some #(pushable-side ?ra %) ?preds)))
    (let [groups (group-by #(pushable-side ?ra %) ?preds)
          not-pushable (groups nil)
          pushable (mapcat val (dissoc groups nil))
          ra (reduce push-predicate ?ra pushable)]
      (if (seq not-pushable)
        [::where ra (into [::and] not-pushable)]
        ra))
    ;; endregion

    ;;region push predicate into join
    [::where [::join ?a ?b ?pred-a] ?pred-b]
    [::join ?a ?b (conjoin-predicates ?pred-a ?pred-b)]
    ;; endregion

    ;; region let fusion
    [::let [::let ?ra ?binding-a] ?binding-b]
    [::let ?ra (into ?binding-a ?binding-b)]
    ;; endregion

    ;; region push predicate into scan
    (m/and [::where [::scan ?expr ?bindings] ?pred]
           (m/guard (pred-scan-binding ?bindings ?pred)))
    (let [i (pred-scan-binding ?bindings ?pred)]
      [::scan ?expr (update ?bindings i (fn [[col k pred]] [col k (conjoin-predicates pred ?pred)]))])

    (m/and [::where [::scan ?expr ?bindings] [::and & ?preds]]
           (m/guard (some #(pred-scan-binding ?bindings %) ?preds)))
    (let [bind-deps (group-by #(pred-scan-binding ?bindings %) ?preds)
          no-bind (get bind-deps nil)
          new-bindings (reduce-kv
                         #(update %1 %2 (fn [[col k pred]] [col k (reduce conjoin-predicates pred %3)]))
                         ?bindings
                         (dissoc bind-deps nil))
          new-scan [::scan ?expr new-bindings]]
      (case (count no-bind)
        0 new-scan
        1 [::where new-scan (first no-bind)]
        [::where new-scan (into [::and] no-bind)]))
    ;; endregion

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

;; a ::group-by-project
;; can permit faster aggregation where grouped columns do not need to be materialized and multiple aggregates
;; can be computed in one loop
;; where columns do not leak out of the projection
(def ^:dynamic *group-project-fusion* false)

(defn infer-type [cols expr]
  (let [col-types (zipmap cols (map (comp :tag meta) cols))
        lower-bound
        (fn [a b]
          (cond
            (nil? a) nil
            (nil? b) nil
            (= 'double a) 'double
            (= 'double b) 'double
            (and (= 'long a) (= 'long b)) 'long
            :else nil))
        local-infer
        (fn local-infer [expr]
          (cond
            (symbol? expr) (col-types expr nil)

            (int? expr) 'long
            (float? expr) 'double

            :else
            (m/match expr

              [::apply-n2n clojure.core/+ ?a ?b]
              (let [a-t (local-infer ?a)
                    b-t (local-infer ?b)]
                (lower-bound a-t b-t))

              [::apply-n2n clojure.core/* ?a ?b]
              (let [a-t (local-infer ?a)
                    b-t (local-infer ?b)]
                (lower-bound a-t b-t))

              [::apply-n2n clojure.core/- ?a ?b]
              (let [a-t (local-infer ?a)
                    b-t (local-infer ?b)]
                (lower-bound a-t b-t))

              [::apply-n2n clojure.core// ?a ?b]
              (let [a-t (local-infer ?a)
                    b-t (local-infer ?b)]
                (lower-bound a-t b-t))
              _ nil)))]
    (local-infer expr)))

(defn aggregate-defaults [cols expr]
  (let [zero (fn [expr]
               (condp = (infer-type cols expr)
                 'double 0.0
                 'long 0
                 `(Long/valueOf 0)))]
    (m/match expr
      [::count] [`0]
      [::count ?expr] [(zero ?expr)]
      [::sum ?expr] [(zero ?expr)]
      [::avg ?expr] [(zero ?expr) 0]
      [::min ?expr] [nil]
      [::max ?expr] [nil]
      _ (throw (ex-info "Unknown aggregate" {:expr expr})))))

(defn aggregate-reduction [acc-syms expr]
  ;; multiples
  (let [acc-sym (first acc-syms)]
    (m/match expr
      [::count] [`(unchecked-inc ~acc-sym)]
      [::count ?expr] [`(if ~?expr (unchecked-inc ~acc-sym) ~acc-sym)]
      [::sum ?expr] [`(CinqUtil/sumStep ~acc-sym ~?expr)]
      [::avg ?expr] [`(CinqUtil/sumStep ~acc-sym ~?expr) `(unchecked-inc ~(second acc-syms))]
      [::min ?expr] [`(CinqUtil/minStep ~acc-sym ~?expr)]
      [::max ?expr] [`(CinqUtil/maxStep ~acc-sym ~?expr)]
      ;; todo min/max
      _ (throw (ex-info "Unknown aggregate" {:expr expr})))))

(defn aggregate-completion [acc-syms expr]
  (let [acc-sym (first acc-syms)]
    (m/match expr
      [::count] acc-sym
      [::count ?expr] acc-sym
      [::sum ?expr] acc-sym
      [::avg ?expr] `[::apply-n2n / ~acc-sym [::apply-n2n max 1 ~(second acc-syms)]]
      _ acc-sym)))

(defn aggregate? [expr]
  (m/match expr
    [::sum _]
    true
    [::avg _]
    true
    [::min _]
    true
    [::max _]
    true
    [::count _]
    true
    [::count]
    true
    _
    false))

(defn hoist-aggregates [group-columns projection-bindings]
  (let [smap (atom {})
        new-sym (fn [expr] (or (get @smap expr) (get (swap! smap assoc expr (*gensym* "agg")) expr)))
        aggregates (->> (tree-seq seqable? seq (mapv second projection-bindings))
                        (keep (fn [expr] (when (aggregate? expr) [(new-sym expr) expr])))
                        (distinct)
                        vec)
        new-projections (mapv (fn [[col expr]] [col (walk/postwalk-replace @smap expr)]) projection-bindings)
        no-leakage (empty? (expr/possible-dependencies group-columns (mapv second new-projections)))]
    (when no-leakage
      [aggregates new-projections])))

(def fuse
  (r/match
    [::where ?ra true]
    ?ra

    [::where [::where ?ra ?pred-a] ?pred-b]
    [::where ?ra (conjoin-predicates ?pred-a ?pred-b)]

    ;; it might be better to have something like ::group-let and an ana pass
    ;; this only works in a tiny subset of occasions
    ;; to determine whether group columns leak
    (m/and [::project [::group-by ?ra ?bindings] ?projection]
           (m/guard *group-project-fusion*))
    (let [;; filter out shadowed group columns
          group-columns (filterv (complement (set (map first ?bindings))) (columns ?ra))
          [agg-bindings new-projection :as no-leakage] (hoist-aggregates group-columns ?projection)
          agg-bindings (for [[sym agg] agg-bindings
                             :let [inits (aggregate-defaults (columns ?ra) agg)
                                   acc-syms (mapv #(*gensym* (str "acc-" % "-" sym)) (range (count inits)))
                                   exprs (aggregate-reduction acc-syms agg)]]
                         [sym (mapv vector acc-syms inits exprs) (aggregate-completion acc-syms agg)])]
      (if no-leakage
        [::group-project ?ra ?bindings (vec agg-bindings) new-projection]
        [::project [::group-by ?ra ?bindings] ?projection]))))

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
    ;; todo deal with column shadowing (implying ordering), guard to stop the fusion because who cares or try to work around it with renames
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
        (let [_ (assert (seq ?rels))
              {reordable-preds true, outer-preds false} (group-by expr/pred-can-be-reordered? ?preds)
              original-order (zipmap ?rels (range))]

          ;; sort each unsatisfied predicate by how many relations do I need to add to satisfy the join
          ;; this is not good. Look at minimum spanning tree algs
          ;; todo if the user hints cards to us we will want to use that for ordering
          ;; See https://blobs.duckdb.org/papers/tom-ebergen-msc-thesis-join-order-optimization-with-almost-no-statistics.pdf
          (loop [unsatisfied-predicates reordable-preds
                 pending-relations (set ?rels)
                 ra nil
                 outer-where outer-preds]
            (if (seq unsatisfied-predicates)
              (let [dependent-relations (fn [pred] (filter #(dependent? % pred) pending-relations))
                    cost (fn [pred]
                           (let [dep-rels (dependent-relations pred)]
                             [(count dep-rels)
                              (reduce min Long/MAX_VALUE (map original-order dep-rels))]))
                    [pred & unsatisfied-predicates] (sort-by cost unsatisfied-predicates)
                    add-relations (sort-by original-order (dependent-relations pred))]
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

(defn do-substitute-sub-queries [ra expr]
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
        new-expr (all expr)
        new-ra (reduce
                 (fn [ra [sq sym]]
                   (m/match sq
                     [::scalar-sq [::project ?sq [[?col ?expr]]]]
                     [::apply :single-join ra [::project ?sq [[sym ?expr]]]]))
                 ra
                 @replacement-syms)]
    [new-ra new-expr]))

(def substitute-sub-queries
  ;; walk ra, replace sub queries with symbols, wrap with apply
  ;; todo PROBLEM: local envs let/fn/reify clauses over sub queries in expressions would cause sub queries to capture different scopes
  ;; SOLUTION 1: disallow let? disallow fn? ...
  ;; SOLUTION 2: detect new scopes, let, fn and reify. subqueries within these scopes cannot be turned into joins
  ;;             e.g rewrite let as [::plan/local-let ...] etc.
  ;;             problem: macros introducing variables - this is already an issue for column shadowing, but actually you could normally use these
  (r/match
    [::where ?ra ?pred]
    (let [[ra pred] (do-substitute-sub-queries ?ra ?pred)]
      [::where ra pred])

    [::project ?ra ?expr]
    (let [[ra expr] (do-substitute-sub-queries ?ra ?expr)]
      [::project ra expr])

    [::let ?ra ?expr]
    (let [[ra expr] (do-substitute-sub-queries ?ra ?expr)]
      [::let ra expr])
    ))

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
      push-lookups-sub-queries
      push-lookups
      rewrite-fuse
      rewrite-join-collect
      rewrite-join-order
      rewrite-logical))

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

(defn equi-join? [left right pred]
  (seq (:left-key (equi-theta left right pred))))

(defn stack-view [ra]
  (-> ((fn ! [ra]
         (m/match ra
           [::scan ?src ?bindings]
           [[:scan ?src ?bindings]]

           [::join* ?rels ?preds]
           [:join* ?rels ?preds]

           [::join ?a ?b ?pred]
           (conj (! ?a) [(if (equi-join? ?a ?b ?pred) :equi-join :theta-join) (! ?b) ?pred])

           [::single-join ?a ?b ?pred]
           (conj (! ?a) [(if (equi-join? ?a ?b ?pred) :equi-single-join :single-join) (! ?b) ?pred])

           [::left-join ?a ?b ?pred]
           (conj (! ?a) [(if (equi-join? ?a ?b ?pred) :equi-left-join :left-join) (! ?b) ?pred])

           [::apply ?mode ?a ?b]
           (conj (! ?a) [:apply ?mode (! ?b)])

           [::cte ?bindings ?ra]
           [:cte (vec (for [[sym ra] ?bindings
                            form [sym (stack-view ra)]]
                        form))
            (stack-view ?ra)]

           [::project ?ra ?bindings]
           (conj (! ?ra) [:project (into {} ?bindings)])

           [::semi-join ?a ?b ?pred]
           (conj (! ?a) [:semi-join (! ?b) ?pred])

           [?op ?ra & ?args]
           (conj (! ?ra) (into [(keyword (name ?op))] ?args))))
       ra)

      ((-> (r/match
             [::lookup ?kw ?s ?t]
             (list ?kw ?s)
             [::= ?a ?b]
             (list '= ?a ?b)
             [::< ?a ?b]
             (list '< ?a ?b)
             [::<= ?a ?b]
             (list '<= ?a ?b)
             [::> ?a ?b]
             (list '> ?a ?b)
             [::>= ?a ?b]
             (list '>= ?a ?b)
             [::and & ?clauses]
             (list* 'and ?clauses)
             [::or & ?clauses]
             (list* 'or ?clauses)
             )
           r/attempt
           r/bottom-up))

      ))

(declare prune-cols)

(defn prune-cols* [ra expr req-cols]
  (let [cols (columns ra)
        req-cols (->> (expr/possible-dependencies cols expr)
                      (into (set req-cols)))
        new-ra (prune-cols ra req-cols)
        not-needed (set (remove (set req-cols) (columns new-ra)))]
    (if (seq not-needed)
      [::without new-ra not-needed]
      new-ra)))

(defn prune-cols
  [ra req-cols]
  (m/match ra
    [::project ?ra ?bindings]
    [::project (prune-cols* ?ra (map second ?bindings) #{}) ?bindings]

    [::where ?ra ?pred]
    [::where (prune-cols* ?ra ?pred req-cols) ?pred]

    [::join ?left ?right ?pred]
    [::join (prune-cols* ?left ?pred req-cols) (prune-cols* ?right ?pred req-cols) ?pred]

    [::left-join ?left ?right ?pred]
    [::left-join (prune-cols* ?left ?pred req-cols) (prune-cols* ?right ?pred req-cols) ?pred]

    [::single-join ?left ?right ?pred]
    [::single-join (prune-cols* ?left ?pred req-cols) (prune-cols* ?right ?pred req-cols) ?pred]

    [::semi-join ?left ?right ?pred]
    [::semi-join (prune-cols* ?left ?pred req-cols) (prune-cols* ?right ?pred #{}) ?pred]

    [::anti-join ?left ?right ?pred]
    [::anti-join (prune-cols* ?left ?pred req-cols) (prune-cols* ?right ?pred #{}) ?pred]

    ;; todo only add this one conveying self tag is done differently (right now removing self when redudant stops type hints)
    ;; check perf - seems slower branch prune-scans, even when re-conveying the self tag (using meta)
    ;; jvm weirdness, maybe cache coincidence
    [::scan ?src ?bindings]
    [::scan ?src (vec (keep (fn [[col k pred :as binding]]
                              (cond
                                (contains? req-cols col)
                                binding

                                ;; keep self for tags, mark as unused
                                (= :cinq/self k)
                                [(vary-meta col assoc :not-used (= true pred)) k pred]

                                ;; keep if filtered
                                (not= true pred)
                                [(vary-meta col assoc :not-used (= true pred)) k pred]))
                            ?bindings))]

    ;; todo group-project
    #_#_[::group-project ?ra ?bindings ?aggregates ?projection]
            nil

    [::apply ?mode ?left ?right]
    [::apply ?mode (prune-cols* ?left ?right req-cols) (prune-cols* ?right nil req-cols)]

    [::group-by ?ra ?bindings]
    [::group-by (prune-cols* ?ra (map second ?bindings) req-cols) ?bindings]

    [::order-by ?ra ?order-clauses]
    [::order-by (prune-cols* ?ra (map first ?order-clauses) req-cols) ?order-clauses]

    [::limit ?ra ?n ?box]
    [::limit (prune-cols* ?ra nil req-cols) ?n ?box]

    [::let ?ra ?bindings]
    [::let (prune-cols* ?ra (map second ?bindings) req-cols) ?bindings]

    _ ra

    ))

(defn possibly-dependent? [ra syms]
  (boolean (seq (expr/possible-dependencies syms ra))))

(ns com.wotbrew.cinq.plan
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [meander.epsilon :as m]
            [meander.strategy.epsilon :as r]))

(defn fix-max
  "Fixed point strategy combinator with a maximum iteration count.
  See r/fix."
  [n s]
  (fn [t]
    (loop [t t
           i 0]
      (let [t* (s t)]
        (cond
          (= t* t) t*
          (= i n) t*
          :else (recur t* (inc i)))))))

(defn tuple-destructure? [arg]
  (and (map? arg)
       (every? symbol? (keys arg))
       (every? nat-int? (vals arg))))

(def ^:dynamic *rw-ns* nil)
(def ^:dynamic *rw-env* {})
(def ^:dynamic *gensym-counter* nil)
(def ^:dynamic *rewrite* nil)

(defn set-rewrite [s] (set! *rewrite* s))

(defn unique-sym [sym]
  (if *gensym-counter*
    (symbol (str (name sym) "__cinq__" (swap! *gensym-counter* inc)))
    (gensym (str (name sym) "__cinq__"))))

(defn free-variables [form]
  (->> (tree-seq seqable? seq form)
       (filter symbol?)
       (remove namespace)
       (remove (or *rw-env* {}))
       (remove (if *rw-ns* #(ns-resolve *rw-ns* %) (constantly false)))))

(defn arity [rel-form]
  (m/match rel-form

    (com.wotbrew.cinq/scan _ ?cols)
    (count ?cols)

    (com.wotbrew.cinq/product ?left ?right)
    (+ (arity ?left) (arity ?right))

    (com.wotbrew.cinq/join ?left ?right _)
    (+ (arity ?left) (arity ?right))

    (com.wotbrew.cinq/left-join ?left ?right _)
    (+ (arity ?left) (arity ?right))

    (com.wotbrew.cinq/dependent-join ?left ?added-col-count _)
    (+ (arity ?left) ?added-col-count)

    (com.wotbrew.cinq/dependent-left-join ?left ?added-col-count _)
    (+ (arity ?left) ?added-col-count)

    (com.wotbrew.cinq/equi-join ?build _ ?probe _ _)
    (+ (arity ?build) (arity ?probe))

    (com.wotbrew.cinq/equi-left-join ?probe _ ?build _ _)
    (+ (arity ?build) (arity ?probe))

    (com.wotbrew.cinq/where ?rel _)
    (arity ?rel)

    (com.wotbrew.cinq/order ?rel _)
    (arity ?rel)

    (com.wotbrew.cinq/select _ ?fns)
    (count ?fns)

    (com.wotbrew.cinq/select* _ ?col-count _)
    ?col-count

    (com.wotbrew.cinq/add ?rel ?fns)
    (+ (arity ?rel) (count ?fns))

    (com.wotbrew.cinq/add* ?rel ?col-count _)
    (+ (arity ?rel) ?col-count)

    _
    (throw (ex-info "Unknown relation form" {:rel-form rel-form, :rewrite *rewrite*}))))

(defn not-unique? [sym]
  (not (str/includes? (name sym) "__cinq__")))

(defn filter-keys [m pred]
  (reduce-kv (fn [m k _] (if (pred k) m (dissoc m k))) m m))

(def mega-rule
  (r/match

    ;; region apply ->
    (-> & ?rest)
    (macroexpand-1 (list* `-> ?rest))

    (clojure.core/-> & ?rest)
    (macroexpand-1 (list* `-> ?rest))
    ;; endregion

    ;; region strip unused col args, uniqueify args
    (m/and (com.wotbrew.cinq/tfn ?arg ?body)
           (m/guard (some not-unique? (keys ?arg))))
    (let [_ (set-rewrite "strip unused cols")
          arg-syms (filter not-unique? (keys ?arg))
          arg-smap (zipmap arg-syms (map unique-sym arg-syms))
          body (walk/postwalk-replace arg-smap ?body)
          used-syms (into #{} (filter symbol?) (tree-seq seqable? seq body))]
      (list `com.wotbrew.cinq/tfn (into {} (for [[s o] ?arg :when (used-syms (arg-smap s s))] [(arg-smap s s) o])) body))
    ;; endregion

    ;; region fuse select into select*
    #_#_(com.wotbrew.cinq/select ?rel ?fns)
    (let [_ (set-rewrite "fuse select")
          n (arity ?rel)

          arg-map (atom {})
          let-binding (atom [])
          let-map (atom {})

          _ (doseq [[fn-i [_ [arg] body]] (map-indexed vector ?fns)
                    [sym i] arg]
              (if (< i n)
                (swap! arg-map assoc sym i)
                (swap! let-binding conj sym (@let-map i)))
              (swap! let-map assoc (+ n fn-i) body))]
      `(com.wotbrew.cinq/select*
         ~?rel
         ~(count ?fns)
         (fn [~(deref arg-map)]
           (let ~(deref let-binding)
             [~@(for [[_ sym] (sort-by key @let-map)] sym)]))))
    ;; endregion

    ;; region fuse add.add
    (com.wotbrew.cinq/add (com.wotbrew.cinq/add ?rel ?fns-a) ?fns-b)
    (let [_ (set-rewrite "fuse add.add")]
      `(com.wotbrew.cinq/add ~?rel ~(vec (concat ?fns-a ?fns-b))))
    ;; endregion

    ;; region fuse add into add*
    #_#_(com.wotbrew.cinq/add ?rel ?fns)
    (let [_ (set-rewrite "fuse add")
          n (arity ?rel)

          arg-map (atom {})
          let-binding (atom [])
          let-map (atom {})

          _ (doseq [[fn-i [_ [arg] body]] (map-indexed vector ?fns)
                    [sym i] arg]
              (if (< i n)
                (swap! arg-map assoc sym i)
                (swap! let-binding conj sym (@let-map i)))
              (swap! let-map assoc (+ n fn-i) body))]
      `(com.wotbrew.cinq/add*
         ~?rel ~(count ?fns)
         (fn [~(deref arg-map)]
           (let ~(deref let-binding)
             [~@(for [[_ sym] (sort-by key @let-map)] sym)]))))
    ;; endregion

    ;; region fuse add.select
    #_#_(com.wotbrew.cinq/select*
      (com.wotbrew.cinq/add* ?rel ?add-count (clojure.core/fn [?add-arg] (clojure.core/let ?add-binding ?add-expr)))
      ?select-count
      (clojure.core/fn [?select-arg] (clojure.core/let ?select-binding ?select-expr)))
    (let [_ (set-rewrite "fuse add.select")
          n (arity ?rel)
          inverse-select-arg (set/map-invert ?select-arg)

          add-expr-bindings
          (for [[i expr] (map-indexed vector ?add-expr)
                :let [sym (inverse-select-arg (+ n i))]
                ;; unused by select if sym is nil
                :when sym
                binding [sym expr]]
            binding)

          let-binding
          (into ?add-binding cat [add-expr-bindings, ?select-binding])]

      `(com.wotbrew.cinq/select* ~?rel ~?select-count (fn [~(merge ?add-arg ?select-arg)] (let ~let-binding ~?select-expr))))
    ;; endregion

    ;; region boolean simplification
    (clojure.core/and ?a)
    ?a

    (and ?a)
    ?a
    ;; endregion

    ;; region de-correlate standard joins
    (m/and
      (?join-sym
        ?left
        ?n-cols
        (com.wotbrew.cinq/tfn ?args
          (com.wotbrew.cinq/where
            ?right
            (com.wotbrew.cinq/tfn ?pred-args ?pred-expr))))
      (m/guard
        (`#{com.wotbrew.cinq/dependent-join
            com.wotbrew.cinq/dependent-left-join} ?join-sym))
      (m/guard
        (not-any? ?args (free-variables ?right)))
      (m/guard
        (= ?n-cols (arity ?right))))
    ;; =>
    (let [_ (set-rewrite "de-correlate standard joins")]
      `(~(condp = ?join-sym
           `com.wotbrew.cinq/dependent-join `com.wotbrew.cinq/join
           `com.wotbrew.cinq/dependent-left-join `com.wotbrew.cinq/left-join)
         ~?left
         ~?right
         (com.wotbrew.cinq/theta-tfn ~?args ~?pred-args ~?pred-expr)))
    ;; endregion

    ;; region rewrite join as equi-join
    (m/and
      (?join-sym
        ?left ?right
        (com.wotbrew.cinq/theta-tfn ?left-args ?right-args
          (com.wotbrew.cinq/join-condition ?pairs ?theta)))
      (m/guard (seq ?pairs))
      (m/guard (`#{com.wotbrew.cinq/join,
                   com.wotbrew.cinq/left-join}
                 ?join-sym)))
    ;; =>
    (let [_ (set-rewrite "joins to equi-joins")
          theta-variables (set (free-variables ?theta))]
      `(~(condp = ?join-sym
           `com.wotbrew.cinq/left-join `com.wotbrew.cinq/equi-left-join
           `com.wotbrew.cinq/join `com.wotbrew.cinq/equi-join)
         ~?left
         (com.wotbrew.cinq/tfn ~?left-args
           [~@(for [[a _] (partition 2 ?pairs)] a)])
         ~?right
         (com.wotbrew.cinq/tfn ~?right-args
           [~@(for [[_ b] (partition 2 ?pairs)] b)])
         (com.wotbrew.cinq/theta-tfn ~(filter-keys ?left-args theta-variables) ~(filter-keys ?right-args theta-variables) ~?theta)))
    ;; endregion

    ;; region normalise join conditions
    (m/and
      (?join-sym
        ?left
        ?right
        (com.wotbrew.cinq/theta-tfn ?left-args ?right-args ?cond))
      (m/guard
        (`#{com.wotbrew.cinq/join, com.wotbrew.cinq/left-join} ?join-sym))
      (m/guard
        (m/match ?cond
          (com.wotbrew.cinq/join-condition _ _)
          false
          _
          true)))
    ;; =>
    (let [_ (set-rewrite "join pred to join-condition")
          equi-pairs (atom [])
          theta (atom [true])

          left-variables (set (keys ?left-args))
          right-variables (set (keys ?right-args))

          add-cond
          (fn add-cond [c]
            (m/match c

              ;; flatten equi pairs
              (com.wotbrew.cinq/join-condition ?pairs2 ?theta2)
              (do (swap! equi-pairs into ?pairs2)
                  (add-cond ?theta2))

              ;; theta always contains nil when emitted by this rule
              true
              nil

              ;; binary equality rule
              (m/or (clojure.core/= ?a ?b)
                    (= ?a ?b))
              (let [free-a (free-variables ?a)
                    free-b (free-variables ?b)
                    a-left (every? left-variables free-a)
                    a-right (and (not a-left) (every? right-variables free-a))
                    b-left (every? left-variables free-b)
                    b-right (and (not b-left) (every? right-variables free-b))]
                (cond
                  (and a-left b-right) (swap! equi-pairs conj ?a ?b)
                  (and a-right b-left) (swap! equi-pairs conj ?b ?a)
                  :else (swap! theta conj `(~'= ~?a ~?b))))

              ;; append conjunctions
              (m/or (and & ?cond2)
                    (clojure.core/and & ?cond2))
              (run! add-cond ?cond2)

              _ (swap! theta conj c)))

          _ (add-cond ?cond)

          equi-pairs @equi-pairs
          theta @theta]
      `(~?join-sym
         ~?left
         ~?right
         (com.wotbrew.cinq/theta-tfn ~?left-args ~?right-args (com.wotbrew.cinq/join-condition ~equi-pairs (and ~@theta)))))
    ;; endregion

    ;; region remove redundant where
    (com.wotbrew.cinq/where ?rel (com.wotbrew.cinq/tfn [_] true))
    ?rel
    ;; endregion

    ;; region simple join -> product
    (com.wotbrew.cinq/join
      ?a
      ?b
      (com.wotbrew.cinq/tfn _ (com.wotbrew.cinq/join-condition [] true)))
    `(com.wotbrew.cinq/product ~?a ~?b)
    ;; endregion

    ;; region dependent join with no select -> product
    (m/and
      (com.wotbrew.cinq/dependent-join
        ?rel-a
        ?n-cols
        (com.wotbrew.cinq/tfn ?args (com.wotbrew.cinq/scan ?scan-target ?scan-cols)))
      (m/guard (= ?n-cols (count ?scan-cols)))
      (m/guard (not-any? (set (free-variables ?scan-target)) (keys ?args))))
    `(com.wotbrew.cinq/product ~?rel-a (com.wotbrew.cinq/scan ~?scan-target ~?scan-cols))
    ;; endregion

    ))

(def mega-strat
  (->> #'mega-rule
       ((fn [s] (fn [x] (set! *rewrite* nil) (s x))))
       r/attempt
       r/bottom-up
       (fix-max 1000)))

(defn rewrite [env ns plan]
  (binding [*rw-env* (or env {})
            *rw-ns* ns
            *gensym-counter* (atom -1)
            *rewrite* nil]
    (mega-strat plan)))

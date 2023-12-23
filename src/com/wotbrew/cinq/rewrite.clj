(ns com.wotbrew.cinq.rewrite
  (:require [clojure.set :as set]
            [clojure.walk :as walk]
            [meander.epsilon :as m]
            [meander.strategy.epsilon :as r]))

(defn tuple-destructure? [arg]
  (and (map? arg)
       (every? symbol? (keys arg))
       (every? nat-int? (vals arg))))

(def ^:dynamic *rw-ns* nil)
(def ^:dynamic *rw-env* {})
(def ^:dynamic *gensym-counter* nil)

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

    (com.wotbrew.cinq/select _ & ?fns)
    (count ?fns)

    (com.wotbrew.cinq/select* _ ?col-count _)
    ?col-count

    (com.wotbrew.cinq/add ?rel & ?fns)
    (+ (arity ?rel) (count ?fns))

    (com.wotbrew.cinq/add* ?rel ?col-count _)
    (+ (arity ?rel) ?col-count)

    ))

(def pass0-rule
  (r/match
    ;; apply ->
    (-> & ?rest)
    (macroexpand-1 (list* `-> ?rest))

    (clojure.core/-> & ?rest)
    (macroexpand-1 (list* `-> ?rest))

    ;; strip unused col args, uniqueify args
    (m/and (clojure.core/fn [?arg] ?body)
           (m/guard (tuple-destructure? ?arg)))
    (let [arg-syms (keys ?arg)
          arg-smap (zipmap arg-syms (map unique-sym arg-syms))
          body (walk/postwalk-replace arg-smap ?body)
          used-syms (into #{} (filter symbol?) (tree-seq seqable? seq body))]
      (list `fn [(into {} (for [[s o] ?arg :when (used-syms (arg-smap s))] [(arg-smap s) o]))] body))))

(def pass0-strategy
  (r/bottom-up (r/attempt pass0-rule)))

(def fusion-rule
  (r/match

    ;; fuse select into select*
    (com.wotbrew.cinq/select ?rel & ?fns)
    (let [n (arity ?rel)

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

    ;; fuse add into add*
    (com.wotbrew.cinq/add ?rel & ?fns)
    (let [n (arity ?rel)

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

    ;; fuse add.select
    (com.wotbrew.cinq/select*
      (com.wotbrew.cinq/add* ?rel ?add-count (clojure.core/fn [?add-arg] (clojure.core/let ?add-binding ?add-expr)))
      ?select-count
      (clojure.core/fn [?select-arg] (clojure.core/let ?select-binding ?select-expr)))
    (let [n (arity ?rel)
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

      `(com.wotbrew.cinq/select* ~?rel ~?select-count (fn [~?add-arg] (let ~let-binding ~?select-expr))))))

(def fusion-strategy
  (r/fix (r/bottom-up (r/attempt fusion-rule))))

(def join-rule-alpha
  (r/match

    ;; de-correlate standard joins
    (m/and
      (?join-sym
        ?left
        ?n-cols
        (clojure.core/fn [?args]
          (com.wotbrew.cinq/where
            ?right
            (clojure.core/fn [?pred-args] ?pred-expr))))
      (m/guard
        (`#{com.wotbrew.cinq/dependent-join
            com.wotbrew.cinq/dependent-left-join} ?join-sym))
      (m/guard
        (not-any? ?args (free-variables ?right))))
    ;; =>
    (let [left-arity (arity ?left)]
      `(~(condp = ?join-sym
           `com.wotbrew.cinq/dependent-join `com.wotbrew.cinq/join
           `com.wotbrew.cinq/dependent-left-join `com.wotbrew.cinq/left-join)
         ~?left
         ~?right
         (fn [~(merge ?args (update-vals ?pred-args #(+ % left-arity)))] ~?pred-expr)))

    ;; rewrite join as equi-join
    (m/and
      (?join-sym
        ?left ?right
        (clojure.core/fn [?arg]
          (com.wotbrew.cinq/join-condition ?pairs ?theta)))
      (m/guard (seq ?pairs))
      (m/guard (`#{com.wotbrew.cinq/join,
                   com.wotbrew.cinq/left-join}
                 ?join-sym)))
    ;; =>
    (let [left-arity (arity ?left)
          left-args (into {} (for [[s i] ?arg :when (< i left-arity)] [s i]))
          right-args (into {} (for [[s i] ?arg :when (<= left-arity i)] [s (- i left-arity)]))]
      `(~(condp = ?join-sym
           `com.wotbrew.cinq/left-join `com.wotbrew.cinq/equi-left-join
           `com.wotbrew.cinq/join `com.wotbrew.cinq/equi-join)
         ~?left
         (fn [~left-args]
           [~@(for [[a _] (partition 2 ?pairs)] a)])
         ~?right
         (fn [~right-args]
           [~@(for [[_ b] (partition 2 ?pairs)] b)])
         (fn [~left-args ~right-args] ~?theta)))

    ;; normalise join conditions
    (m/and
      (?join-sym
        ?left
        ?right
        (clojure.core/fn [?arg] ?cond))
      (m/guard
        (`#{com.wotbrew.cinq/join, com.wotbrew.cinq/left-join} ?join-sym)))
    ;; =>
    (let [left-arity (arity ?left)
          equi-pairs (atom [])
          theta (atom [true])

          left-variables (set (for [[s i] ?arg :when (< i left-arity)] s))
          right-variables (set (remove left-variables (keys ?arg)))

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
         (clojure.core/fn [~?arg] (com.wotbrew.cinq/join-condition ~equi-pairs (and ~@theta)))))

    #_
    (com.wotbrew.cinq/dependent-join
      ?left
      ?n-cols
      (clojure.core/fn [?args]
        ?rel2))))

(defn fix-max
  "Fixed point strategy combinator with a maximum iteration count.
  See r/fix."
  [n s]
  (fn [t]
    (loop [t t
           i 0]
      (let [t* (s t)]
        (cond
          (= t* t) t
          (= i n) t
          :else (recur t* (inc n)))))))

(def join-strategy
  (fix-max 10 (r/bottom-up (r/attempt join-rule-alpha))))

(def where-rules
  (r/match
    (com.wotbrew.cinq/where ?rel (clojure.core/fn [_] true))
    ?rel))

(def where-strategy
  (fix-max 10 (r/bottom-up (r/attempt where-rules))))

(defn rewrite [env ns plan]
  (binding [*rw-env* (or env {})
            *rw-ns* ns
            *gensym-counter* (atom -1)]
    (-> plan
        pass0-strategy
        join-strategy
        where-strategy
        fusion-strategy
        (->> (walk/postwalk (fn [x] (if (seq? x) (doall x) x)))))))

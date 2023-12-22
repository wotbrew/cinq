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
       (remove *rw-env*)
       (remove (if *rw-ns* #(ns-resolve *rw-ns* %) (constantly false)))))

(defn arity [rel-form]
  (m/match rel-form

    (com.wotbrew.cinq/scan _ ?cols)
    (count ?cols)

    (com.wotbrew.cinq/dependent-join ?left ?added-col-count _)
    (+ (arity ?left) ?added-col-count)

    (com.wotbrew.cinq/dependent-left-join ?left ?added-col-count _)
    (+ (arity ?left) ?added-col-count)

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

(def r-pass0
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

(def pass0
  (r/bottom-up (r/attempt r-pass0)))

(def r-pass1
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

(def pass1
  (r/fix (r/bottom-up (r/attempt r-pass1))))

(defn rewrite [env ns plan]
  (binding [*rw-env* env
            *rw-ns* ns
            *gensym-counter* (atom -1)]
    (-> plan
        pass0
        pass1)))

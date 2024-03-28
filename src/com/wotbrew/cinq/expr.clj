(ns com.wotbrew.cinq.expr
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [com.wotbrew.cinq.column :as col]
            [meander.epsilon :as m]))

(create-ns 'com.wotbrew.cinq.plan2)
(alias 'plan 'com.wotbrew.cinq.plan2)

(defn possible-dependencies [dep-cols expr]
  (let [count-sym '%count]
    (if (sequential? dep-cols)
      (possible-dependencies (zipmap dep-cols (range)) expr)
      (->> (tree-seq seqable? seq expr)
           (keep #(cond
                    (simple-symbol? %)
                    (some-> (find dep-cols %) key)
                    (= [::plan/count] %) (some-> (find dep-cols count-sym) key)))
           (distinct)))))

(defn like-includes? [s pat]
  (if s
    (str/includes? s pat)
    false))

(defn like-starts? [s pat]
  (if s
    (str/starts-with? s pat)
    false))

(defn like-ends? [s pat]
  (if s
    (str/ends-with? s pat)
    false))

(defn- emit-like [expr pattern]
  (let [re #"(%?)([^%]*)(%?)"
        [_ ends pat starts] (re-find re pattern)
        ends (not-empty ends)
        starts (not-empty starts)]
    ;; todo escaping, re fallback (for complicated cases)
    (cond

      (and starts ends)
      `(like-includes? ~expr ~pat)

      starts
      `(like-starts? ~expr ~pat)

      ends
      `(like-ends? ~expr ~pat)

      :else
      `(= ~expr ~pat))))

(defn rewrite [col-maps clj-expr compile-plan]
  (let [dep-cols (apply merge-with (fn [_ b] b) col-maps)
        rw (fn [form]
             (m/match form

               [::plan/lookup ?kw ?s]
               (list ?kw ?s)

               [::plan/count] '%count

               [::plan/count ?expr]
               (let [deps (possible-dependencies dep-cols ?expr)]
                 (list `col/count-some (vec (interleave deps deps)) ?expr))

               [::plan/sum ?expr]
               (let [deps (possible-dependencies dep-cols ?expr)]
                 (list `col/sum (vec (interleave deps deps)) ?expr))

               [::plan/avg ?expr]
               (let [deps (possible-dependencies dep-cols ?expr)]
                 (list `col/avg (vec (interleave deps deps)) ?expr))

               [::plan/min ?expr]
               (let [deps (possible-dependencies dep-cols ?expr)]
                 (list `col/minimum (vec (interleave deps deps)) ?expr))

               [::plan/max ?expr]
               (let [deps (possible-dependencies dep-cols ?expr)]
                 (list `col/maximum (vec (interleave deps deps)) ?expr))

               [::plan/like ?expr ?pattern]
               (do
                 (assert (string? ?pattern) "Only constant like patterns are supported")
                 (emit-like ?expr ?pattern))

               [::plan/= ?a ?b]
               `(= ~?a ~?b)

               [::plan/<= ?a ?b]
               `(<= (compare ~?a ~?b) 0)
               [::plan/< ?a ?b]
               `(< (compare ~?a ~?b) 0)
               [::plan/>= ?a ?b]
               `(>= (compare ~?a ~?b) 0)
               [::plan/> ?a ?b]
               `(> (compare ~?a ~?b) 0)

               [::plan/scalar-sq ?plan]
               `(first ~(compile-plan ?plan))

               [::plan/and & ?clause]
               (if (seq ?clause)
                 `(and ~@?clause)
                 true)

               [::plan/or & ?clause]
               (if (seq ?clause)
                 `(or ~@?clause)
                 nil)

               [::plan/not ?e]
               `(not ~?e)

               [::plan/in ?expr ?set]
               `(contains? ~?set ~?expr)

               _ form))]
    ;; todo should keep meta on this walk? (e.g return hints on lists)
    (walk/prewalk rw clj-expr)))

(defn certainly-const-expr? [expr]
  (cond
    (nil? expr) true
    (number? expr) true
    (string? expr) true
    (keyword? expr) true
    (inst? expr) true
    :else false))

(defn pred-can-be-reordered? [expr]
  (m/match
    expr
    [::plan/= ?a ?b] (and (pred-can-be-reordered? ?a) (pred-can-be-reordered? ?b))
    [::plan/and & ?clause] (every? pred-can-be-reordered? ?clause)
    [::plan/or & ?clause] (every? pred-can-be-reordered? ?clause)
    [::plan/< ?a ?b] (and (pred-can-be-reordered? ?a) (pred-can-be-reordered? ?b))
    [::plan/<= ?a ?b] (and (pred-can-be-reordered? ?a) (pred-can-be-reordered? ?b))
    [::plan/> ?a ?b] (and (pred-can-be-reordered? ?a) (pred-can-be-reordered? ?b))
    [::plan/>= ?a ?b] (and (pred-can-be-reordered? ?a) (pred-can-be-reordered? ?b))
    [::plan/lookup ?kw ?expr] (pred-can-be-reordered? ?expr)
    [::plan/not ?expr] (pred-can-be-reordered? ?expr)
    [::plan/in ?expr ?set] (and (pred-can-be-reordered? ?expr) (pred-can-be-reordered? ?set))
    (m/guard (symbol? expr)) true
    (m/guard (map? expr)) (every? #(and (pred-can-be-reordered? (key %)) (pred-can-be-reordered? (val %))) expr)
    (m/guard (vector? expr)) (every? pred-can-be-reordered? expr)
    (m/guard (set? expr)) (every? pred-can-be-reordered? expr)
    (m/guard (certainly-const-expr? expr)) true
    (m/guard (seq? expr)) false
    ;; reader literals should be ok as they have no env access
    _ true))

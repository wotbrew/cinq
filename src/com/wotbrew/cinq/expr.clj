(ns com.wotbrew.cinq.expr
  (:require [clojure.walk :as walk]
            [com.wotbrew.cinq.column :as col]
            [meander.epsilon :as m])
  (:import (com.wotbrew.cinq CinqUtil)))

(create-ns 'com.wotbrew.cinq.plan)
(alias 'plan 'com.wotbrew.cinq.plan)

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

(defn apply-n2n
  ([f] (f))
  ([f a]
   (when (some? a)
     (f a)))
  ([f a b]
   (when (and (some? a) (some? b))
     (f a b)))
  ([f a b c]
   (when (and (some? a) (some? b) (some? c))
     (f a b c)))
  ([f a b c d]
   (when (and (some? a) (some? b) (some? c) (some? d))
     (f a b c d)))
  ([f a b c d e]
   (when (and (some? a) (some? b) (some? c) (some? d) (some? e))
     (f a b c e)))
  ([f a b c d e & args]
   (when (and (some? a) (some? b) (some? c) (some? d) (some? e) (every? some? args))
     (apply f a b c d e args))))

(defn rewrite-vararg-apply-to-binop [bin-form x y args]
  (case (count args)
    0 `(~bin-form ~x ~y)
    1 `(~bin-form ~x (~bin-form ~y ~(first args)))
    `(~bin-form ~x ~(rewrite-vararg-apply-to-binop bin-form y (first args) (rest args)))))

(defn rewrite [col-maps clj-expr compile-plan]
  (let [dep-cols (apply merge-with (fn [_ b] b) col-maps)
        rw (fn [form]
             (m/match form

               [::plan/lookup ?kw ?s ?t]
               (if ?t
                 (with-meta (list ?kw ?s) {:tag ?t})
                 (list ?kw ?s))

               [::plan/count] '%count

               [::plan/count ?expr]
               (let [deps (possible-dependencies dep-cols ?expr)]
                 (list `col/count-some (vec (interleave deps deps)) ?expr))

               [::plan/count-distinct ?expr]
               (let [deps (possible-dependencies dep-cols ?expr)]
                 (list `col/count-distinct-some (vec (interleave deps deps)) ?expr))

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

               [::plan/= ?a ?b]
               `(CinqUtil/eq ~?a ~?b)

               [::plan/<= ?a ?b]
               `(CinqUtil/lte ~?a ~?b)
               [::plan/< ?a ?b]
               `(CinqUtil/lt ~?a ~?b)
               [::plan/>= ?a ?b]
               `(CinqUtil/gte ~?a ~?b)
               [::plan/> ?a ?b]
               `(CinqUtil/gt ~?a ~?b)

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

               ;; any coll
               [::plan/contains ?coll ?expr]
               `(contains? ~?coll ~?expr)

               ;; set only
               [::plan/in ?expr ?set]
               `(contains? ~?set ~?expr)

               [::plan/apply-n2n clojure.core/+ ?a ?b & ?args]
               (rewrite-vararg-apply-to-binop `CinqUtil/add ?a ?b ?args)
               [::plan/apply-n2n clojure.core/* ?a ?b & ?args]
               (rewrite-vararg-apply-to-binop `CinqUtil/mul ?a ?b ?args)
               [::plan/apply-n2n clojure.core/- ?a ?b & ?args]
               (rewrite-vararg-apply-to-binop `CinqUtil/sub ?a ?b ?args)
               [::plan/apply-n2n clojure.core// ?a ?b & ?args]
               (rewrite-vararg-apply-to-binop `CinqUtil/div ?a ?b ?args)

               [::plan/apply-n2n ?sym & ?args]
               `(apply-n2n ~?sym ~@?args)

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

(defn expr-can-be-reordered? [expr]
  (m/match
    expr
    [::plan/= ?a ?b] (and (expr-can-be-reordered? ?a) (expr-can-be-reordered? ?b))
    [::plan/and & ?clause] (every? expr-can-be-reordered? ?clause)
    [::plan/or & ?clause] (every? expr-can-be-reordered? ?clause)
    [::plan/< ?a ?b] (and (expr-can-be-reordered? ?a) (expr-can-be-reordered? ?b))
    [::plan/<= ?a ?b] (and (expr-can-be-reordered? ?a) (expr-can-be-reordered? ?b))
    [::plan/> ?a ?b] (and (expr-can-be-reordered? ?a) (expr-can-be-reordered? ?b))
    [::plan/>= ?a ?b] (and (expr-can-be-reordered? ?a) (expr-can-be-reordered? ?b))
    [::plan/lookup ?kw ?expr ?t] (expr-can-be-reordered? ?expr)
    [::plan/not ?expr] (expr-can-be-reordered? ?expr)
    [::plan/in ?expr ?set] (expr-can-be-reordered? ?expr)
    [::plan/contains ?coll ?expr] (and (expr-can-be-reordered? ?coll) (expr-can-be-reordered? ?expr))
    [::plan/apply-n2n ?sym & ?args] (every? expr-can-be-reordered? ?args)
    (m/guard (symbol? expr)) true
    (m/guard (map? expr)) (every? #(and (expr-can-be-reordered? (key %)) (expr-can-be-reordered? (val %))) expr)
    (m/guard (vector? expr)) (every? expr-can-be-reordered? expr)
    (m/guard (set? expr)) (every? expr-can-be-reordered? expr)
    (m/guard (certainly-const-expr? expr)) true
    (m/guard (seq? expr)) false
    ;; reader literals should be ok as they have no env access
    _ true))



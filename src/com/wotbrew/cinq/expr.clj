(ns com.wotbrew.cinq.expr
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [com.wotbrew.cinq.column :as col]
            [meander.epsilon :as m])
  (:import (com.wotbrew.cinq CinqUtil)))

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
    [::plan/lookup ?kw ?expr ?t] (pred-can-be-reordered? ?expr)
    [::plan/not ?expr] (pred-can-be-reordered? ?expr)
    [::plan/in ?expr ?set] (pred-can-be-reordered? ?expr)
    [::plan/contains ?coll ?expr] (and (pred-can-be-reordered? ?coll) (pred-can-be-reordered? ?expr))
    [::plan/apply-n2n ?sym & ?args] (every? pred-can-be-reordered? ?args)
    (m/guard (symbol? expr)) true
    (m/guard (map? expr)) (every? #(and (pred-can-be-reordered? (key %)) (pred-can-be-reordered? (val %))) expr)
    (m/guard (vector? expr)) (every? pred-can-be-reordered? expr)
    (m/guard (set? expr)) (every? pred-can-be-reordered? expr)
    (m/guard (certainly-const-expr? expr)) true
    (m/guard (seq? expr)) false
    ;; reader literals should be ok as they have no env access
    _ true))



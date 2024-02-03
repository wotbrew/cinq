(ns com.wotbrew.cinq.expr
  (:require [clojure.walk :as walk]
            [com.wotbrew.cinq.column :as col]
            [com.wotbrew.cinq.plan2 :as plan]
            [meander.epsilon :as m]))

(defn possible-dependencies [dep-cols expr]
  (if (sequential? dep-cols)
    (possible-dependencies (zipmap dep-cols (range)) expr)
    (->> (tree-seq seqable? seq expr)
         (keep #(when (simple-symbol? %) (some-> (find dep-cols %) key)))
         (distinct))))

(defn rewrite [col-maps clj-expr compile-plan]
  (let [dep-cols (apply merge-with (fn [_ b] b) col-maps)
        rw (fn [form]
             (m/match form

               [::plan/lookup ?kw ?s]
               (list ?kw ?s)

               [::plan/count-some ?expr]
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

               _ form))]
    ;; todo should keep meta on this walk? (e.g return hints on lists)
    (walk/prewalk rw clj-expr)))
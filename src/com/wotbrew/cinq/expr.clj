(ns com.wotbrew.cinq.expr
  (:require [clojure.walk :as walk]
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

               _ form))]
    ;; todo should keep meta on this walk? (e.g return hints on lists)
    (walk/prewalk rw clj-expr)))

(ns com.wotbrew.cinq.vector-seq
  "Seq of vectors runtime algebra representation. Very slow, but more likely correct due to its simple implementation.
   Good to test compiler results against."
  (:require [clojure.walk :as walk]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.column :as col]
            [meander.epsilon :as m])
  (:import (java.util Comparator)))

;; src

(defn scan [src cols]
  (for [o src]
    (mapv (fn [c] (if (identical? :cinq/self c) o (get o c))) cols)))

;; ra

(defn where [vseq pred]
  (filter pred vseq))

(defn group [vseq key-fn]
  (lazy-seq
    (map (fn [[k v]]
           (let [tc (count (first v))]
             (into (mapv (fn [i]
                           (let [arr (object-array (count v))]
                             (dotimes [j (count v)]
                               (let [t (nth v j)]
                                 (aset arr j (nth t i))))
                             (col/->Column arr))) (range tc))
                   k)))
         (group-by key-fn vseq))))

(defn order [vseq order-clauses]
  (lazy-seq
    (sort (reify Comparator
            (compare [_ t0 t1]
              (loop [i 0]
                (if (< i (count order-clauses))
                  (let [[f dir] (nth order-clauses i)
                        res (compare (f t0) (f t1))]
                    (if (= 0 res)
                      (recur (inc i))
                      (case dir :desc (* -1 res) res)))
                  0))))
          vseq)))

(defn add-columns [vseq f]
  (for [t vseq]
    (into t (f t))))

(defn limit [vseq n] (take n vseq))

(defn select [vseq f]
  (map f vseq))

(defn dependent-join [vseq f n-cols]
  (for [l vseq
        r (f l)]
    (into l (take n-cols) (concat r (repeat nil)))))

(defn dependent-left-join [vseq f n-cols]
  (for [l vseq
        :let [rs (f l)]
        l+r (if (seq rs) (map #(into l (take n-cols) (concat % (repeat nil))) rs) [(into l (take n-cols) (repeat nil))])]
    l+r))

(defn join [left right theta-pred]
  (for [l left
        r right
        :when (theta-pred l r)]
    (into l r)))

(defn left-join [left right theta-pred n-cols]
  (for [l left
        :let [rs (filter #(theta-pred l %) right)]
        l+r (if (seq rs) (map #(into l %) rs) [(into l (take n-cols) (repeat nil))])]
    l+r))

(defn equi-join [left left-key right right-key theta-pred]
  (let [build (group-by left-key left)]
    (for [r right
          l (build (right-key r))
          :when (theta-pred l r)]
      (into l r))))

(defn equi-left-join [left left-key right right-key theta-pred n-cols]
  (let [build (group-by right-key right)]
    (for [l left
          :let [rs (filter #(theta-pred l %) (build (left-key l)))]
          l+r (if (seq rs) (map #(into l %) rs) [(into l (take n-cols) (repeat nil))])]
      l+r)))

(defn product [left right]
  (for [l left
        r right]
    (into l r)))

;; aggregates
;; todo these should be macros, look for free vars in env using resolve. Expand them as late as possible.

(defn vectorize [variables]
  (let [vectors (mapv #(if (coll? %) (vec %) (vector %)) variables)
        max-count (apply max 0 (map count vectors))]
    (mapv (fn [v] (if (< (count v) max-count) (into v (take (- max-count (count v))) (repeat nil)) v)) vectors)))

(defn %count [f variables]
  (let [variables (vectorize variables)
        size (count (first variables))]
    (loop [acc 0
           i (int 0)]
      (if (< i size)
        (let [args (mapv #(nth % i) variables)
              ret (f args)]
          (if ret
            (recur (inc acc) (inc i))
            (recur acc (inc i))))
        acc))))

(defn %sum [f variables]
  (let [variables (vectorize variables)
        size (count (first variables))]
    (loop [acc 0
           i (int 0)]
      (if (< i size)
        (let [args (mapv #(nth % i) variables)]
          (recur (+ acc (f args)) (inc i)))
        acc))))

(defn %avg [f variables]
  (let [variables (vectorize variables)
        size (count (first variables))
        sum
        (loop [acc 0
               i 0]
          (if (< i size)
            (let [args (mapv #(nth % i) variables)]
              (recur (+ acc (f args)) (inc i)))
            acc))]
    (/ sum size)))

(defn %max [f variables]
  (let [variables (vectorize variables)
        size (count (first variables))]
    (loop [acc nil
           i 0]
      (if (< i size)
        (let [args (mapv #(nth % i) variables)
              ret (f args)]
          (cond
            (nil? ret) (recur acc (inc i))
            (nil? acc) (recur ret (inc i))
            :else (recur (if (neg? (compare acc ret)) ret acc) (inc i))))
        acc))))

(defn %min [f variables]
  (let [variables (vectorize variables)
        size (count (first variables))]
    (loop [acc nil
           i 0]
      (if (< i size)
        (let [args (mapv #(nth % i) variables)
              ret (f args)]
          (cond
            (nil? ret) (recur acc (inc i))
            (nil? acc) (recur ret (inc i))
            :else (recur (if (neg? (compare acc ret)) acc ret) (inc i))))
        acc))))

(defn possible-dependencies [dep-cols expr]
  (->> (tree-seq seqable? seq expr)
       (filter #(and (simple-symbol? %) (dep-cols %)))
       (distinct)))

(defn rewrite-expr [col-maps clj-expr]
  (let [dep-cols (apply merge-with (fn [_ b] b) col-maps)
        rw (fn [form]
             (m/match form

               [::plan/lookup ?kw ?s ?default]
               (if (nil? ?default)
                 (list ?kw ?s)
                 (list ?kw ?s ?default))

               _ form))]
    ;; todo should keep meta on this walk? (e.g return hints on lists)
    (walk/postwalk rw clj-expr)))

(defn compile-src [clj-expr] (rewrite-expr [] clj-expr))

(defn compile-lambda [dep-relations clj-expr]
  (let [col-maps (mapv #(plan/dependent-cols % clj-expr) dep-relations)]
    `(fn ~col-maps ~(rewrite-expr col-maps clj-expr))))

(defn compile-plan [plan]
  (m/match plan

    [::plan/scan ?src ?cols]
    `(scan ~(compile-src ?src) ~(mapv second ?cols))

    [::plan/where ?ra ?pred]
    `(where
       ~(compile-plan ?ra)
       ~(compile-lambda [?ra] ?pred))

    [::plan/product ?left ?right]
    `(product ~(compile-plan ?left) ~(compile-plan ?right))

    [::plan/dependent-join ?left ?right]
    `(dependent-join
       ~(compile-plan ?left)
       ~(compile-lambda [?left] ?right)
       ~(plan/arity ?right))

    [::plan/dependent-left-join ?left ?right]
    `(dependent-left-join
       ~(compile-plan ?left)
       ~(compile-lambda [?left] ?right)
       ~(plan/arity ?right))

    [::plan/join ?left ?right ?theta-expr]
    `(join ~(compile-plan ?left)
           ~(compile-plan ?right)
           ~(compile-lambda [?left ?right] ?theta-expr))

    [::plan/left-join ?left ?right ?theta-expr]
    `(left-join ~(compile-plan ?left)
                ~(compile-plan ?right)
                ~(compile-lambda [?left ?right] ?theta-expr)
                ~(plan/arity ?right))

    [::plan/equi-join ?left ?left-keys ?right ?right-keys ?theta-expr]
    `(equi-join ~(compile-plan ?left)
                ~(compile-lambda [?left] ?left-keys)
                ~(compile-plan ?right)
                ~(compile-lambda [?right] ?right-keys)
                ~(compile-lambda [?left ?right] ?theta-expr))

    [::plan/equi-left-join ?left ?left-keys ?right ?right-keys ?theta-expr]
    `(equi-left-join
       ~(compile-plan ?left)
       ~(compile-lambda [?left] ?left-keys)
       ~(compile-plan ?right)
       ~(compile-lambda [?right] ?right-keys)
       ~(compile-lambda [?left ?right] ?theta-expr)
       ~(plan/arity ?right))

    [::plan/let ?ra ?bindings]
    `(add-columns
       ~(compile-plan ?ra)
       ~(compile-lambda
          [?ra]
          `(let [~@(for [[sym expr] ?bindings
                         form [sym expr]]
                     form)]
             [~@(map first ?bindings)])))

    [::plan/select ?ra ?projection]
    `(select
       ~(compile-plan ?ra)
       ~(compile-lambda [?ra] (mapv second ?projection)))

    [::plan/group-by ?ra ?bindings]
    `(group ~(compile-plan ?ra) ~(compile-lambda
                                   [?ra]
                                   `(let [~@(for [[sym expr] ?bindings
                                                  form [sym expr]]
                                              form)]
                                      [~@(map first ?bindings)])))

    [::plan/order-by ?ra ?order-clauses]
    `(order ~(compile-plan ?ra)
            [~@(for [[expr dir] ?order-clauses]
                 [(compile-lambda [?ra] expr) dir])])

    [::plan/limit ?ra ?n]
    `(limit ~(compile-plan ?ra) ~?n)))

(defmacro q [ra]
  (let [lp (parse/parse ra)
        lp' (plan/rewrite lp)]
    (compile-plan lp')))

(comment

  (q [a [1, 2, 3]])
  (macroexpand '(q [a [1, 2, 3]]))


  (q [a [1, 2, 3]
      b [1, 2, 3]])
  (macroexpand '(q [a [1, 2, 3]
                    b [1, 2, 3]]))

  (q [a [1, 2, 3]
      b [1, 2, 3]
      :where (= a 2)])
  (macroexpand '(q [a [1, 2, 3]
                    b [1, 2, 3]
                    :where (= a 2)]))

  (q [a [1, 2, 3]
      b [1, 2, 3]
      :where (= a b)])
  (macroexpand '(q [a [1, 2, 3]
                    b [1, 2, 3]
                    :where (= a b)]))

  (q [a [1, 2, 3, 4]
      :group-by []
      :select [:a (col/sum a)]])

  (macroexpand
    '(q [a [1, 2, 3]
        :group-by [even (even? a)]
        :select [:n (count a)]]))

  (q [a [1, 2, 3, 4, 4, 5]
      :group-by [even (even? a)]
      :select [:even even, :n (count a), :sum (SUM a)]])

  (q [c [{:firstname "Dan" :lastname "Stone", :id 42}]
      o [{:basket {"egg" 1, "bacon" 2}, :customer-id 42}]
      :where (= c:id o:customer-id)
      :select [:name (str c:firstname " " c:lastname)
               :basket o:basket]])

  ;; reserved names, not allowed in user args, reify args, let symbol.
  ;; columns! e.g if I have 'a bound to a column, I cannot shadow it
  ;; let*, let
  ;; Q, S, C, EXISTS, NOT-EXISTS, SOME, ANY, ALL
  ;; lookup-symbols a:b
  ;; =, <=, >, >=, not=, not
  ;; ALSO: macros clobbering symbols are not supported in queries?

  (macroexpand
    '(q [c [{:firstname "Dan" :lastname "Stone", :id 42}]
         o [{:basket {"egg" 1, "bacon" 2}, :customer-id 42}]
         :where (= c:id o:customer-id)
         :select [:name (str c:firstname " " c:lastname)
                  :basket o:basket]]))



  )

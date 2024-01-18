(ns com.wotbrew.cinq.vector-seq
  "Seq of vectors runtime algebra representation. Very slow, but more likely correct due to its simple implementation.
   Good to test compiler results against."
  (:require [clojure.walk :as walk]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.column :as col]
            [meander.epsilon :as m]
            [meander.strategy.epsilon :as r])
  (:import (clojure.lang ILookup Indexed)
           (java.util ArrayList Arrays Comparator HashMap List)))

(set! *warn-on-reflection* true)

;; src

(defn scan [src cols]
  (for [o src]
    (mapv (fn [c] (if (identical? :cinq/self c) o (get o c))) cols)))

;; ra

(defn where [vseq pred]
  (filter pred vseq))

(defn conjoin-tuples
  ([^objects arr1 ^objects arr2]
   (let [out (object-array (+ (alength arr1) (alength arr2)))]
     (dotimes [i (alength arr1)]
       (aset out i (aget arr1 i)))
     (dotimes [i (alength arr2)]
       (aset out (+ (alength arr1) i) (aget arr2 i)))
     out))
  ([^objects l ^objects r ^long n-cols]
   (let [ret (Arrays/copyOf l (+ (alength l) (int n-cols)))]
     (System/arraycopy r 0 ret (alength l) (alength r))
     ret)))

(defn group [vseq key-fn]
  (let [ht (HashMap.)
        add (fn [o]
              (let [k (key-fn o), ^List al (.get ht k)]
                (if al
                  (.add al o)
                  (.put ht k (doto (ArrayList.) (.add o))))))
        _ (run! add vseq)]
    (map (fn [[k ^ArrayList al]]
           (let [tc (alength ^objects (.get al 0))]
             (-> (mapv (fn [i]
                         (let [arr (object-array (.size al))]
                           (dotimes [j (.size al)]
                             (let [^objects t (.get al j)]
                               (aset arr j (aget t i))))
                           (col/->Column arr))) (range tc))
                 (into k)
                 (conj (.size al))
                 (object-array))))
         ht)))

(defn order [vseq order-clauses]
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
        vseq))

(defn add-columns [vseq f]
  (for [t vseq]
    (conjoin-tuples t (f t))))

(defn limit [vseq n] (take n vseq))

(defn select [vseq f]
  (map f vseq))

(defn apply-join [vseq f n-cols]
  (for [^objects l vseq
        ^objects r (f l)]
    (conjoin-tuples l r n-cols)))

(defn apply-left-join [vseq f n-cols]
  (for [^objects l vseq
        :let [rs (f l)]
        l+r (if (seq rs)
              (map #(conjoin-tuples l % (int n-cols)) rs)
              [(Arrays/copyOf l (+ (alength l) (int n-cols)))])]
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
  (let [ht (HashMap.)
        add (fn [o]
              (let [k (left-key o), ^List al (.get ht k)]
                (if al
                  (.add al o)
                  (.put ht k (doto (ArrayList.) (.add o))))))
        _ (run! add left)]
    (eduction
      (mapcat (fn [r]
                (when-some [^List al (.get ht (right-key r))]
                  (let [out (ArrayList.)]
                    (dotimes [i (.size al)]
                      (let [l (.get al i)]
                        (when (theta-pred l r)
                          (.add out (conjoin-tuples l r)))))
                    out))))
      right)))

(defn equi-left-join [left left-key right right-key theta-pred n-cols]
  (let [ht (HashMap.)
        add (fn [o]
              (let [k (right-key o), ^List al (.get ht k)]
                (if al
                  (.add al o)
                  (.put ht k (doto (ArrayList.) (.add o))))))
        _ (run! add right)]
    (for [^objects l left
          :let [rs (filter #(theta-pred l %) (.get ht (left-key l)))]
          l+r (if (seq rs) (map #(conjoin-tuples l %) rs) [(Arrays/copyOf l (+ (alength l) (int n-cols)))])]
      l+r)))

(defn cross-join [left right]
  (for [l left
        r right]
    (conjoin-tuples l r)))

(defn possible-dependencies [dep-cols expr]
  (->> (tree-seq seqable? seq expr)
       (filter #(and (simple-symbol? %) (dep-cols %)))
       (distinct)))

(declare compile-plan)

(defn rewrite-expr [col-maps clj-expr]
  (let [dep-cols (apply merge-with (fn [_ b] b) col-maps)
        rw (fn [form]
             (m/match form

               [::plan/lookup ?kw ?s]
               (list ?kw ?s)

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
               `(ffirst ~(compile-plan ?plan))

               [::plan/column-sq ?plan]
               `(col/->Column (object-array (map first ~(compile-plan ?plan))))

               ;; todo tuple type?
               [::plan/sq ?plan]
               (compile-plan ?plan)

               [::plan/and & ?clause]
               (if (seq ?clause)
                 `(and ~@?clause)
                 true)

               _ form))]
    ;; todo should keep meta on this walk? (e.g return hints on lists)
    (walk/prewalk rw clj-expr)))

(defn compile-src [clj-expr] (rewrite-expr [] clj-expr))

(defn compile-lambda [dep-relations clj-expr]
  (let [col-maps (mapv #(plan/dependent-cols % clj-expr) dep-relations)
        array-syms (mapv #(with-meta (gensym (str "a" %)) {:tag 'objects}) (range (count col-maps)))]
    `(fn ~array-syms (let [~@(for [[i col-map] (map-indexed vector col-maps)
                                   [sym j] col-map
                                   form [sym `(aget ~(nth array-syms i) ~j)]]
                               form)]
                       ~(rewrite-expr col-maps clj-expr)))))

(defn compile-key [dep-relations key-exprs]
  (case (count key-exprs)
    1 (compile-lambda dep-relations (first key-exprs))
    (compile-lambda dep-relations (vec key-exprs))))

(defn collapse-plan [[src xforms]]
  (if (seq xforms)
    `(eduction (comp ~@xforms) ~src)
    src))

(declare compile-plan*)
(defn compile-plan [plan] (collapse-plan (compile-plan* plan)))

(defn compile-plan* [plan]
  (m/match plan

    [::plan/where [::plan/scan ?src ?cols] ?pred]
    [(compile-src ?src)
     (list (let [osym (gensym "o")
                 asym (gensym "a")]
             ;; permitting the keep transducer to be specialised (rather than passing a function to keep)
             ;; reduces megamorphic completing dispatch and speeds things up quite a bit (30-40% on some benchmarks)
             `(fn [rf#]
                (fn
                  ([] (rf#))
                  ([result#] (rf# result#))
                  ([result# ~osym]
                   (let [~@(for [[sym k] ?cols
                                 form [sym (cond
                                             (= :cinq/self k) osym
                                             (keyword? k) (list k osym)
                                             :else (list `get osym k))]]
                             form)]
                     (when ~(rewrite-expr [] ?pred)
                       (let [~asym (object-array ~(count ?cols))]
                         ~@(for [[i sym] (map-indexed vector (map first ?cols))]
                             `(aset ~asym ~i ~sym))
                         (rf# result# ~asym)))))))))]

    [::plan/scan ?src ?cols]
    (let [osym (gensym "o")
          asym (gensym "a")]
      [(compile-src ?src)
       (list `(map (fn [~osym]
                     (let [~@(for [[sym k] ?cols
                                   form [sym (cond
                                               (= :cinq/self k) osym
                                               (keyword? k) (list k osym)
                                               :else (list `get osym k))]]
                               form)
                           ~asym (object-array ~(count ?cols))]
                       ~@(for [[i sym] (map-indexed vector (map first ?cols))]
                           `(aset ~asym ~i ~sym))
                       ~asym))))])

    [::plan/where ?ra ?pred]
    (let [[src xforms] (compile-plan* ?ra)]
      [src
       (conj xforms `(filter ~(compile-lambda [?ra] ?pred)))])

    [::plan/cross-join ?left ?right]
    [`(cross-join ~(collapse-plan (compile-plan* ?left))
                  ~(collapse-plan (compile-plan* ?right)))]

    [::plan/apply :cross-join ?left ?right]
    [`(apply-join
        ~(compile-plan ?left)
        ~(compile-lambda [?left] ?right)
        ~(plan/arity ?right))]

    [::plan/apply :left-join ?left ?right]
    [`(apply-left-join
        ~(compile-plan ?left)
        ~(compile-lambda [?left] ?right)
        ~(plan/arity ?right))]

    [::plan/join ?left ?right ?pred]
    (let [{:keys [left-key right-key theta]
           :or {theta [true]}}
          (plan/equi-theta ?left ?right ?pred)]
      [(if (seq left-key)
         `(equi-join ~(compile-plan ?left)
                     ~(compile-key [?left] left-key)
                     ~(compile-plan ?right)
                     ~(compile-key [?right] right-key)
                     ~(compile-lambda [?left ?right] theta))
         `(join ~(compile-plan ?left)
                ~(compile-plan ?right)
                ~(compile-lambda [?left ?right] ?pred)))])

    [::plan/left-join ?left ?right ?pred]
    (let [{:keys [left-key right-key theta]
           :or {theta [true]}}
          (plan/equi-theta ?left ?right ?pred)]
      [(if (seq left-key)
         `(equi-left-join ~(compile-plan ?left)
                          ~(compile-key [?left] left-key)
                          ~(compile-plan ?right)
                          ~(compile-key [?right] right-key)
                          ~(compile-lambda [?left ?right] theta)
                          ~(plan/arity ?right))
         `(left-join ~(compile-plan ?left)
                     ~(compile-plan ?right)
                     ~(compile-lambda [?left ?right] ?pred)
                     ~(plan/arity ?right)))])

    [::plan/let ?ra ?bindings]
    `[(add-columns
        ~(compile-plan ?ra)
        ~(compile-lambda
           [?ra]
           `(let [~@(for [[sym expr] ?bindings
                          form [sym expr]]
                      form)]
              (doto (object-array ~(count ?bindings))
                ~@(for [[i [sym]] (map-indexed vector ?bindings)]
                    `(aset ~i ~sym))))))]

    [::plan/select ?ra ?projection]
    `[(select
        ~(compile-plan ?ra)
        ~(compile-lambda [?ra] (mapv second ?projection)))]

    [::plan/group-by ?ra ?bindings]
    `[(group ~(compile-plan ?ra) ~(compile-lambda
                                    [?ra]
                                    `(let [~@(for [[sym expr] ?bindings
                                                   form [sym expr]]
                                               form)]
                                       ~(mapv first ?bindings))))]

    [::plan/order-by ?ra ?order-clauses]
    `[(order ~(compile-plan ?ra)
             [~@(for [[expr dir] ?order-clauses]
                  [(compile-lambda [?ra] expr) dir])])]

    [::plan/limit ?ra ?n]
    `[(limit ~(compile-plan ?ra) ~?n)]))

(defmacro q [ra expr]
  (let [lp (parse/parse ra expr)
        lp' (plan/rewrite lp)
        [src xforms] (compile-plan* lp')]
    `(sequence ~(collapse-plan [src (conj xforms `(map vec))]))))

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

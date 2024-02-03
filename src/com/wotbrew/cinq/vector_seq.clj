(ns com.wotbrew.cinq.vector-seq
  "Seq of vectors runtime algebra representation. Very slow, but more likely correct due to its simple implementation.
   Good to test compiler results against."
  (:require [com.wotbrew.cinq.expr :as expr]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.column :as col]
            [meander.epsilon :as m])
  (:import (clojure.lang IRecord)
           (java.lang.reflect Field)
           (java.util ArrayList Arrays Comparator HashMap List)))

(set! *warn-on-reflection* true)

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
           (let [tc (alength ^objects (.get al 0))
                 out-array (object-array (+ tc (count k) 1))]
             ;; group columns
             (dotimes [i tc]
               (aset out-array i (col/->Column nil #(let [arr (object-array (.size al))]
                                                      (dotimes [j (.size al)]
                                                        (let [^objects t (.get al j)]
                                                          (aset arr j (aget t i))))
                                                      arr))))
             ;; key columns
             (dotimes [i (count k)]
               (aset out-array (+ tc i) (nth k i)))
             ;; %count variable
             (aset out-array (+ tc (count k)) (Long/valueOf (long (.size al))))
             out-array))
         ht)))

(deftype Key2 [a b hash]
  Object
  (hashCode [_] hash)
  (equals [_ o]
    (and (instance? Key2 o)
         (let [^Key2 o o]
           (and (= a (.-a o))
                (= b (.-b o)))))))

(defn group2 [vseq key-fn]
  (let [ht (HashMap.)
        add (fn [o]
              (let [k (key-fn o), ^List al (.get ht k)]
                (if al
                  (.add al o)
                  (.put ht k (doto (ArrayList.) (.add o))))))
        _ (run! add vseq)]
    (map (fn [[^Key2 k ^ArrayList al]]
           (let [tc (alength ^objects (.get al 0))
                 out-array (object-array (+ tc 3))]
             ;; group columns
             (dotimes [i tc]
               (aset out-array i (col/->Column nil #(let [arr (object-array (.size al))
                                                          i (int i)]
                                                      (dotimes [j (.size al)]
                                                        (let [^objects t (.get al j)]
                                                          (aset arr j (aget t i))))
                                                      arr))))
             ;; key columns
             (aset out-array tc (.-a k))
             (aset out-array (+ tc 1) (.-b k))
             ;; %count variable
             (aset out-array (+ tc 2) (Long/valueOf (long (.size al))))
             out-array))
         ht)))

(defn group1 [vseq key-fn]
  (let [ht (HashMap.)
        add (fn [o]
              (let [k (key-fn o), ^List al (.get ht k)]
                (if al
                  (.add al o)
                  (.put ht k (doto (ArrayList.) (.add o))))))
        _ (run! add vseq)]
    (map (fn [[k ^ArrayList al]]
           (let [tc (alength ^objects (.get al 0))
                 out-array (object-array (+ tc 2))]
             ;; group columns
             (dotimes [i tc]
               (aset out-array i (col/->Column nil #(let [arr (object-array (.size al))]
                                                      (dotimes [j (.size al)]
                                                        (let [^objects t (.get al j)]
                                                          (aset arr j (aget t i))))
                                                      arr))))
             ;; key columns
             (aset out-array tc k)
             ;; %count variable
             (aset out-array (+ tc 1) (Long/valueOf (long (.size al))))
             out-array))
         ht)))

(defn group-all [vseq]
  (let [al (ArrayList.)
        _ (run! (fn [t] (.add al t)) vseq)]
    (if (= 0 (.size al))
      ;; todo sql semantics or empty here?
      []
      [(let [tc (alength ^objects (.get al 0))
             out-array (object-array (inc tc))]
         ;; group columns
         (dotimes [i tc]
           (aset out-array i (col/->Column nil #(let [arr (object-array (.size al))]
                                                  (dotimes [j (.size al)]
                                                    (let [^objects t (.get al j)]
                                                      (aset arr j (aget t i))))
                                                  arr))))
         ;; %count variable
         (aset out-array tc (Long/valueOf (long (.size al))))
         out-array)])))

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

(defn apply-single-join [vseq f]
  (for [^objects l vseq
        :let [x (first (f l))
              ret (object-array (unchecked-inc-int (alength l)))
              _ (System/arraycopy l 0 ret 0 (alength l))
              _ (aset ret (alength l) x)]]
    ret))

(defn join [left right theta-pred]
  (for [l left
        r right
        :when (theta-pred l r)]
    (conjoin-tuples l r)))

(defn semi-join [left right theta-pred]
  (for [l left
        :when (seq (filter #(theta-pred l %) right))]
    l))

(defn left-join [left right theta-pred n-cols]
  (for [^objects l left
        :let [rs (filter #(theta-pred l %) right)]
        l+r (if (seq rs) (map #(conjoin-tuples l %) rs) [(Arrays/copyOf l (+ (alength l) (int n-cols)))])]
    l+r))

(defn single-join [left right theta-pred]
  (for [^objects l left
        :let [r (first (filter #(theta-pred l %) right))]]
    (doto (Arrays/copyOf l (inc (alength l)))
      (aset (alength l) r))))

(defn const-single-join [left right]
  (let [r (first right)]
    (for [^objects l left]
      (doto (Arrays/copyOf l (inc (alength l)))
        (aset (alength l) r)))))

(defn equi-join [left left-key right right-key theta-pred]
  (let [ht (HashMap.)
        _ (reduce (fn [_ o]
                    ;; itable left-key
                    (let [k (left-key o), ^List al (.get ht k)]
                      (if al
                        (.add al o)
                        (.put ht k (doto (ArrayList.) (.add o))))))
                  nil left)]
    (eduction
      (fn [rf]
        (fn
          ([] (rf))
          ([result] (rf result))
          ([result r]
           ;; itable right-key
           (if-some [^List al (.get ht (right-key r))]
             (let [len (.size al)]
               (loop [result result
                      i 0]
                 (if (< i len)
                   (let [l (.get al i)]
                     ;; itable theta-pred
                     (if (theta-pred l r)
                       (recur (rf result (conjoin-tuples l r)) (unchecked-inc-int i))
                       (recur result (unchecked-inc-int i))))
                   result)))
             result))))

#_
      (mapcat (fn [r]
                (when-some [^List al (.get ht (right-key r))]
                  (let [out (ArrayList.)]
                    (dotimes [i (.size al)]
                      (let [l (.get al i)]
                        (when (theta-pred l r)
                          (.add out (conjoin-tuples l r)))))
                    out))))
      right)))

(defn equi-semi-join [left left-key right right-key theta-pred]
  (let [ht (HashMap.)
        add (fn [o]
              (let [k (right-key o), ^List al (.get ht k)]
                (if al
                  (.add al o)
                  (.put ht k (doto (ArrayList.) (.add o))))))
        _ (run! add right)]
    (eduction
      (keep (fn [l]
              (when-some [^List al (.get ht (left-key l))]
                (loop [i (int 0)]
                  (when (< i (.size al))
                    (let [r (.get al i)]
                      (if (theta-pred l r)
                        l
                        (recur (unchecked-inc-int i)))))))))
      left)))

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

(defn equi-single-join [left left-key right right-key theta-pred]
  (let [ht (HashMap.)
        add (fn [o]
              (let [k (right-key o), ^List al (.get ht k)]
                (if al
                  (.add al o)
                  (.put ht k (doto (ArrayList.) (.add o))))))
        _ (run! add right)]
    (for [^objects l left
          :let [r (first (filter #(theta-pred l %) (.get ht (left-key l))))]]
      (doto (Arrays/copyOf l (inc (alength l)))
        (aset (alength l) r)))))

(defn cross-join [left right]
  (for [l left
        r right]
    (conjoin-tuples l r)))

(declare compile-plan)

(defn rewrite-expr [col-maps clj-expr]
  (expr/rewrite col-maps clj-expr compile-plan))

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

(defmacro safe-hash [a] `(clojure.lang.Util/hasheq ~a))

(defn has-kw-field? [^Class class kw]
  (and (isa? class IRecord)
       (let [munged-name (munge (name kw))]
         (->> (.getFields class)
              (some (fn [^Field f] (= munged-name (.getName f))))))))

(defn compile-plan* [plan]
  (m/match plan

    [::plan/where [::plan/scan ?src ?cols] ?pred]
    [(compile-src ?src)
     [(let [self-binding (last (keep #(when (= :cinq/self (second %)) (first %)) ?cols))
            self-tag (:tag (meta self-binding))
            self-class (if (symbol? self-tag) (resolve self-tag) self-tag)
            o (with-meta (gensym "o") {:tag self-tag})
            asym (gensym "a")]
        ;; permitting the keep transducer to be specialised (rather than passing a function to keep)
        ;; reduces megamorphic completing dispatch and speeds things up quite a bit (30-40% on some benchmarks)
        `(fn [rf#]
           (fn
             ([] (rf#))
             ([result#] (rf# result#))
             ([result# ~o]
              (let [~@(for [[sym k] ?cols
                            form [sym (cond
                                        (= :cinq/self k) o
                                        (and self-class (class? self-class) (has-kw-field? self-class k))
                                        (list (symbol (str ".-" (munge (name k)))) o)
                                        (keyword? k) (list k o)
                                        :else (list `get o k))]]
                        form)]
                (when ~(rewrite-expr [] ?pred)
                  (let [~asym (object-array ~(count ?cols))]
                    ~@(for [[i sym] (map-indexed vector (map first ?cols))]
                        `(aset ~asym ~i ~sym))
                    (rf# result# ~asym))))))))]]

    [::plan/scan ?src ?cols]
    (let [self-binding (last (keep #(when (= :cinq/self (second %)) (first %)) ?cols))
          self-tag (:tag (meta self-binding))
          self-class (if (symbol? self-tag) (resolve self-tag) self-tag)
          o (with-meta (gensym "o") {:tag self-tag})
          asym (gensym "a")]
      [(compile-src ?src)
       [`(fn [rf#]
           (fn
             ([] (rf#))
             ([result#] (rf# result#))
             ([result# ~o]
              (let [~@(for [[sym k] ?cols
                            form [sym (cond
                                        (= :cinq/self k) o
                                        (and self-class (class? self-class) (has-kw-field? self-class k))
                                        (list (symbol (str ".-" (munge (name k)))) o)
                                        (keyword? k) (list k o)
                                        :else (list `get o k))]]
                        form)]
                (let [~asym (object-array ~(count ?cols))]
                  ~@(for [[i sym] (map-indexed vector (map first ?cols))]
                      `(aset ~asym ~i ~sym))
                  (rf# result# ~asym))))))]])

    [::plan/where ?ra ?pred]
    (let [[src xforms] (compile-plan* ?ra)]
      [src
       (conj xforms `(filter ~(compile-lambda [?ra] ?pred)))])

    [::plan/cross-join ?left ?right]
    [`(cross-join ~(collapse-plan (compile-plan* ?left))
                  ~(collapse-plan (compile-plan* ?right)))
     []]

    [::plan/apply :cross-join ?left ?right]
    [`(apply-join
        ~(compile-plan ?left)
        ~(compile-lambda [?left] (compile-plan ?right))
        ~(plan/arity ?right))
     []]

    [::plan/apply :left-join ?left ?right]
    [`(apply-left-join
        ~(compile-plan ?left)
        ~(compile-lambda [?left] (compile-plan ?right))
        ~(plan/arity ?right))
     []]

    [::plan/apply :single-join ?left ?right]
    [`(apply-single-join
        ~(compile-plan ?left)
        ~(compile-lambda [?left] (compile-plan ?right)))
     []]

    [::plan/join ?left ?right ?pred]
    (let [{:keys [left-key right-key theta]
           :or {theta [true]}}
          (plan/equi-theta ?left ?right ?pred)]
      [(if (seq left-key)
         `(equi-join ~(compile-plan ?left)
                     ~(compile-key [?left] left-key)
                     ~(compile-plan ?right)
                     ~(compile-key [?right] right-key)
                     ~(compile-lambda [?left ?right] `(and ~@theta)))
         `(join ~(compile-plan ?left)
                ~(compile-plan ?right)
                ~(compile-lambda [?left ?right] ?pred)))
       []])

    [::plan/semi-join ?left ?right ?pred]
    (let [{:keys [left-key right-key theta]
           :or {theta [true]}}
          (plan/equi-theta ?left ?right ?pred)]
      [(if (seq left-key)
         `(equi-semi-join ~(compile-plan ?left)
                          ~(compile-key [?left] left-key)
                          ~(compile-plan ?right)
                          ~(compile-key [?right] right-key)
                          ~(compile-lambda [?left ?right] `(and ~@theta)))
         `(semi-join ~(compile-plan ?left)
                     ~(compile-plan ?right)
                     ~(compile-lambda [?left ?right] ?pred)))
       []])

    [::plan/left-join ?left ?right ?pred]
    (let [{:keys [left-key right-key theta]
           :or {theta [true]}}
          (plan/equi-theta ?left ?right ?pred)]
      [(if (seq left-key)
         `(equi-left-join ~(compile-plan ?left)
                          ~(compile-key [?left] left-key)
                          ~(compile-plan ?right)
                          ~(compile-key [?right] right-key)
                          ~(compile-lambda [?left ?right] `(and ~@theta))
                          ~(plan/arity ?right))
         `(left-join ~(compile-plan ?left)
                     ~(compile-plan ?right)
                     ~(compile-lambda [?left ?right] ?pred)
                     ~(plan/arity ?right)))
       []])

    [::plan/single-join ?left ?right ?pred]
    (let [{:keys [left-key right-key theta]
           :or {theta [true]}}
          (plan/equi-theta ?left ?right ?pred)]
      [(cond
         (seq left-key)
         `(equi-single-join ~(compile-plan ?left)
                            ~(compile-key [?left] left-key)
                            ~(compile-plan ?right)
                            ~(compile-key [?right] right-key)
                            ~(compile-lambda [?left ?right] `(and ~@theta)))
         (= theta [true])
         `(const-single-join ~(compile-plan ?left) ~(compile-plan ?right))

         :else
         `(single-join ~(compile-plan ?left)
                       ~(compile-plan ?right)
                       ~(compile-lambda [?left ?right] ?pred)))
       []])

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
                    `(aset ~i ~sym))))))
      []]

    [::plan/project ?ra ?projections]
    (let [[src xform] (compile-plan* ?ra)]
      (if (= 1 (count ?projections))
        [src (conj xform `(map ~(compile-lambda [?ra] (second (first ?projections)))))]
        [src (conj xform `(map ~(compile-lambda [?ra] (mapv second ?projections))))]))

    [::plan/group-by ?ra ?bindings]
    (case (count ?bindings)
      0
      `[(group-all ~(compile-plan ?ra)) []]

      1
      `[(group1 ~(compile-plan ?ra)
                ~(compile-lambda
                   [?ra]
                   `(let [~@(for [[sym expr] ?bindings
                                  form [sym expr]]
                              form)]
                      ~(ffirst ?bindings))))
        []]

      2
      `[(group2 ~(compile-plan ?ra)
                ~(compile-lambda
                   [?ra]
                   `(let [~@(for [[sym expr] ?bindings
                                  form [(with-meta sym {:tag Object}) expr]]
                              form)]
                      (new Key2
                           ~(first (first ?bindings))
                           ~(first (second ?bindings))
                           ~(let [[[sym1] [sym2]] ?bindings]
                              `(bit-xor (safe-hash ~sym1) (safe-hash ~sym2)))))))
        []]

      `[(group ~(compile-plan ?ra)
               ~(compile-lambda
                  [?ra]
                  `(let [~@(for [[sym expr] ?bindings
                                 form [sym expr]]
                             form)]
                     ~(mapv first ?bindings))))
        []])

    [::plan/order-by ?ra ?order-clauses]
    `[(order ~(compile-plan ?ra)
             [~@(for [[expr dir] ?order-clauses]
                  [(compile-lambda [?ra] expr) dir])])
      []]

    [::plan/limit ?ra ?n]
    `[(limit ~(compile-plan ?ra) ~?n)
      []]))

(defmacro q*
  ([ra] `(q* ~ra :cinq/*))
  ([ra expr]
   (let [lp (parse/parse (list 'q ra expr))
         lp' (plan/rewrite lp)
         [src xforms] (compile-plan* lp')]
     (if (seq xforms)
       `(eduction (comp ~@xforms) ~src)
       `(eduction identity ~src)))))

(defmacro q
  ([ra] `(q ~ra :cinq/*))
  ([ra expr]
   (let [lp (parse/parse (list 'q ra expr))
         lp' (plan/rewrite lp)
         [src xforms] (compile-plan* lp')]
     (if (seq xforms)
       `(sequence (comp ~@xforms) ~src)
       `(sequence ~src)))))

(comment

  (q [a [1, 2, 3]])
  (q [a [1, 2, 3]] a)
  (q [a [1, 2, 3] b [1, 2]])
  (macroexpand '(q [a [1, 2, 3]] a))


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
      :where (= c:id o:customer-id)]
     {:name (str c:firstname " " c:lastname)
      :basket o:basket})

  (q [c [{:firstname "Dan" :lastname "Stone", :id 42}]
      o [{:basket {"egg" 1, "bacon" 2}, :customer-id 42}]
      :where (= c:id o:customer-id)]
     ($select :name (str c:firstname " " c:lastname)
              :basket o:basket))

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

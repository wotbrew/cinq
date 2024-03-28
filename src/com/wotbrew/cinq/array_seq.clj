(ns com.wotbrew.cinq.array-seq
  (:require [com.wotbrew.cinq.expr :as expr]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.column :as col]
            [meander.epsilon :as m])
  (:import (clojure.lang IRecord RT)
           (com.wotbrew.cinq CinqUtil)
           (java.lang.reflect Field)
           (java.util ArrayList Arrays Comparator HashMap List)
           (java.util.function Function)))

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

(deftype ArrayKey [^int hc ^objects arr]
  Object
  (equals [_ o]
    ;; assume the cast as we will only ever use this internally
    (Arrays/equals ^objects arr ^objects (.-arr ^ArrayKey o)))
  (hashCode [_] hc))

(defn group [aseq key-fn]
  (let [ht (HashMap.)
        add (fn [o]
              (let [k (key-fn o), ^List al (.get ht k)]
                (if al
                  (.add al o)
                  (.put ht k (doto (ArrayList.) (.add o))))))
        _ (run! add aseq)]
    (map (fn [[^ArrayKey k ^ArrayList al]]
           (let [tc (alength ^objects (.get al 0))
                 ^objects karr (.-arr k)
                 out-array (object-array (inc (+ tc (alength karr))))]
             ;; group columns
             (dotimes [i tc]
               (aset out-array i (col/->Column nil #(let [arr (object-array (.size al))]
                                                      (dotimes [j (.size al)]
                                                        (let [^objects t (.get al j)]
                                                          (aset arr j (aget t i))))
                                                      arr))))
             ;; key columns
             (dotimes [i (alength karr)]
               (aset out-array (+ tc i) (aget karr i)))
             ;; %count variable
             (aset out-array (+ tc (alength karr)) (Long/valueOf (long (.size al))))
             out-array))
         ht)))

(defn group1 [aseq key-fn]
  (let [ht (HashMap.)
        add (fn [o]
              (let [k (key-fn o), ^List al (.get ht k)]
                (if al
                  (.add al o)
                  (.put ht k (doto (ArrayList.) (.add o))))))
        _ (run! add aseq)]
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

(defn group-all [aseq n-cols]
  (let [al (ArrayList.)
        _ (run! (fn [t] (.add al t)) aseq)]
    [(let [out-array (object-array (inc n-cols))]
       ;; group columns
       (dotimes [i n-cols]
         (aset out-array i (col/->Column nil #(let [arr (object-array (.size al))]
                                                (dotimes [j (.size al)]
                                                  (let [^objects t (.get al j)]
                                                    (aset arr j (aget t i))))
                                                arr))))
       ;; %count variable
       (aset out-array n-cols (Long/valueOf (long (.size al))))
       out-array)]))

(defn order [aseq order-clauses]
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
        aseq))

(defn add-columns [aseq f]
  (for [t aseq]
    (conjoin-tuples t (f t))))

(defn limit [aseq n] (take n aseq))

(defn apply-join [aseq f n-cols]
  (for [^objects l aseq
        ^objects r (f l)]
    (conjoin-tuples l r n-cols)))

(defn apply-left-join [aseq f n-cols]
  (for [^objects l aseq
        :let [rs (f l)]
        l+r (if (seq rs)
              (map #(conjoin-tuples l % (int n-cols)) rs)
              [(Arrays/copyOf l (+ (alength l) (int n-cols)))])]
    l+r))

(defn apply-single-join [aseq f]
  (for [^objects l aseq
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

(defn const-semi-join [left right]
  (if (seq right)
    left
    ()))

(defn anti-join [left right theta-pred]
  (for [l left
        :when (empty? (filter #(theta-pred l %) right))]
    l))

(defn const-anti-join [left right]
  (if (empty? right)
    left
    ()))

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
        add (fn [o]
              (let [k (left-key o)
                    ^List al (.computeIfAbsent ht k (reify Function (apply [_ _] (ArrayList.))))]
                (.add al o)))
        _ (run! add left)]
    (eduction
      (fn [rf]
        (fn
          ([] (rf))
          ([result] (rf result))
          ([result r]
           (if-some [^List al (.get ht (right-key r))]
             (let [len (.size al)]
               (loop [result result
                      i 0]
                 (if (< i len)
                   (let [l (.get al i)]
                     (if (theta-pred l r)
                       (recur (rf result (conjoin-tuples l r)) (unchecked-inc-int i))
                       (recur result (unchecked-inc-int i))))
                   result)))
             result))))
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
      (filter
        (fn [l]
          (when-some [^List al (.get ht (left-key l))]
            (loop [i (int 0)]
              (when (< i (.size al))
                (let [r (.get al i)]
                  (if (theta-pred l r)
                    true
                    (recur (unchecked-inc-int i)))))))))
      left)))

(defn equi-anti-join [left left-key right right-key theta-pred]
  (let [ht (HashMap.)
        add (fn [o]
              (let [k (right-key o), ^List al (.get ht k)]
                (if al
                  (.add al o)
                  (.put ht k (doto (ArrayList.) (.add o))))))
        _ (run! add right)]
    (eduction
      (filter (fn [l]
                (if-some [^List al (.get ht (left-key l))]
                  (loop [i (int 0)]
                    (if (< i (.size al))
                      (let [r (.get al i)]
                        (if (theta-pred l r)
                          false
                          (recur (unchecked-inc-int i))))
                      true))
                  true)))
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
    (compile-lambda dep-relations `(let [arr# (doto (object-array ~(count key-exprs))
                                                ~@(for [[i expr] (map-indexed vector key-exprs)]
                                                    `(aset ~i ~expr)))]
                                     (new ArrayKey (CinqUtil/hashArray arr#) arr#)))))

(defn collapse-plan [[src xforms]]
  (if (seq xforms)
    `(eduction (comp ~@xforms) ~src)
    src))

(declare compile-plan*)
(defn compile-plan [plan] (collapse-plan (compile-plan* plan)))

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
                        `(aset ~asym ~i (RT/box ~sym)))
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
                      `(aset ~asym ~i (RT/box ~sym)))
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
      [(cond
         (seq left-key)
         `(equi-semi-join ~(compile-plan ?left)
                          ~(compile-key [?left] left-key)
                          ~(compile-plan ?right)
                          ~(compile-key [?right] right-key)
                          ~(compile-lambda [?left ?right] `(and ~@theta)))

         (= [true] theta)
         `(const-semi-join ~(compile-plan ?left) ~(compile-plan ?right))

         :else
         `(semi-join ~(compile-plan ?left)
                     ~(compile-plan ?right)
                     ~(compile-lambda [?left ?right] ?pred)))
       []])

    [::plan/anti-join ?left ?right ?pred]
    (let [{:keys [left-key right-key theta]
           :or {theta [true]}}
          (plan/equi-theta ?left ?right ?pred)]
      [(cond
         (seq left-key)
         `(equi-anti-join ~(compile-plan ?left)
                          ~(compile-key [?left] left-key)
                          ~(compile-plan ?right)
                          ~(compile-key [?right] right-key)
                          ~(compile-lambda [?left ?right] `(and ~@theta)))

         (= [true] theta)
         `(const-anti-join ~(compile-plan ?left) ~(compile-plan ?right))

         :else
         `(anti-join ~(compile-plan ?left)
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
                    `(aset ~i (RT/box ~sym)))))))
      []]

    [::plan/project ?ra ?projections]
    (let [[src xform] (compile-plan* ?ra)]
      (if (= 1 (count ?projections))
        [src (conj xform `(map ~(compile-lambda [?ra] (second (first ?projections)))))]
        [src (conj xform `(map ~(compile-lambda [?ra] (mapv second ?projections))))]))

    [::plan/group-by ?ra ?bindings]
    (case (count ?bindings)
      0
      `[(group-all ~(compile-plan ?ra) ~(count (plan/columns ?ra))) []]

      1
      `[(group1 ~(compile-plan ?ra)
                ~(compile-lambda
                   [?ra]
                   `(let [~@(for [[sym expr] ?bindings
                                  form [sym expr]]
                              form)]
                      ~(ffirst ?bindings))))
        []]

      `[(group ~(compile-plan ?ra)
               ~(compile-lambda
                  [?ra]
                  `(let [~@(for [[sym expr] ?bindings
                                 form [sym expr]]
                             form)
                         arr# (doto (object-array ~(count ?bindings))
                                ~@(for [[i [sym]] (map-indexed vector ?bindings)]
                                    `(aset ~i ~sym)))]
                     (new ArrayKey (CinqUtil/hashArray arr#) arr#))))
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
   (binding
     [; columns must be boxed in array-seq
      plan/*specialise-group-column-types* false
      ]
     (let [lp (parse/parse (list 'q ra expr))
           lp' (plan/rewrite lp)
           [src xforms] (compile-plan* lp')]
       (if (seq xforms)
         `(sequence (comp ~@xforms) ~src)
         `(sequence ~src))))))

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

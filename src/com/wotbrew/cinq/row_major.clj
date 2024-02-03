(ns com.wotbrew.cinq.row-major
  (:require [com.wotbrew.cinq.column :as col]
            [com.wotbrew.cinq.expr :as expr]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.vector-seq :as vseq]
            [meander.epsilon :as m])
  (:import (clojure.lang IRecord Seqable Sequential)
           (com.wotbrew.cinq ArrayIterator)
           (java.lang.reflect Field)
           (java.util ArrayList HashMap Iterator LinkedHashMap$Entry Map$Entry)))

(set! *warn-on-reflection* true)

(definterface Iter
  (nextRow ^boolean []))

(def oarr (Class/forName "[Ljava.lang.Object;"))

(defn iterator ^Iterator [src]
  (if (instance? oarr src)
    (ArrayIterator. src)
    (.iterator ^Iterable src)))

(def scan-src-field (with-meta 'cinq__scan-src {:tag `Iterator}))

(def src-field 'cinq__src)
(def src-field-accessor '.-cinq__src)

(defn col-field [i col] (with-meta (symbol (str "f" i)) (merge {:unsynchronized-mutable true, :public true} (select-keys (meta col) [:tag]))))
(defn col-fields [cols] (map-indexed col-field cols))

(defn field-sym [i] (symbol (str "f" i)))
(defn field-accessor [i] (symbol (str ".-" (field-sym i))))

(defn has-kw-field? [^Class class kw]
  (and (isa? class IRecord)
       (let [munged-name (munge (name kw))]
         (->> (.getFields class)
              (some (fn [^Field f] (= munged-name (.getName f))))))))

(defn getter-sym [i] (symbol (str "getF" i)))

(defn tuple-interface* [types]
  (let [s (gensym "ITuple")]
    `(definterface ~s
       ~@(for [[i t] (map-indexed vector types)]
           (list (getter-sym i) (with-meta [] {:tag t}))))))

(defn tuple-interface [cols]
  (tuple-interface* (mapv (fn [col] (:tag (meta col) `Object)) cols)))

(defn tuple-impl [interface-sym cols]
  (into [interface-sym] (for [i (range (count cols))] (list (getter-sym i) ['_] (field-sym i)))))

(defmacro add-hash [hash1 hash2]
  `(unchecked-multiply-int 31 (unchecked-add-int ~hash1 ~hash2)))

(defn tuple-standalone [interface-sym cols]
  (let [t (gensym "Tuple")
        osym (gensym "o")
        tosym (with-meta osym {:tag interface-sym})
        hsym (with-meta '__hashcode {:tag 'int, :unsynchronized-mutable true})]
    `(deftype ~t [~hsym ~@(col-fields cols)]
       ~@(tuple-impl interface-sym cols)
       Object
       (equals [_# ~osym]
         (and (instance? ~interface-sym ~osym)
              (let [~tosym ~osym]
                (and ~@(for [i (range (count cols))]
                         `(= ~(field-sym i) (. ~tosym ~(getter-sym i))))))))
       (hashCode [_#]
         (if (= 0 ~hsym)
           (let [hc# ~(case (count cols)
                        0 42
                        (reduce
                          (fn [form i] `(add-hash ~form (vseq/safe-hash ~(field-sym i))))
                          1
                          (range (count cols))))]
             (set! ~hsym hc#)
             hc#)
           ~hsym)))))

(defn scan-iter
  ([tuple-sym cols bindings] (scan-iter tuple-sym cols bindings ::no-pred))
  ([tuple-sym cols bindings pred]
   (let [t (gensym "ScanIter")
         this (gensym "this")
         self-binding (last (keep #(when (= :cinq/self (second %)) (first %)) bindings))
         self-tag (:tag (meta self-binding))
         self-class (if (symbol? self-tag) (resolve self-tag) self-tag)
         o (with-meta (gensym "o") {:tag self-tag})]
     `(deftype ~t [~(with-meta scan-src-field {:tag `Iterator}) ~@(col-fields cols)]
        ~@(tuple-impl tuple-sym cols)
        Iter
        (nextRow [~this]
          (if (.hasNext ~scan-src-field)
            (let [~o (.next ~scan-src-field)
                  ~@(for [[sym k] bindings
                          form [sym (cond
                                      (= :cinq/self k) o
                                      (and self-class (class? self-class) (has-kw-field? self-class k))
                                      (list (symbol (str ".-" (munge (name k)))) o)
                                      (keyword? k) (list k o)
                                      :else (list `get o k))]]
                      form)]
              ~(if (= ::no-pred pred)
                 `(if ~pred
                    (do
                      ~@(for [[i [local]] (map-indexed vector bindings)]
                          `(set! (~(field-accessor i) ~this) ~local))
                      true)
                    (recur))
                 `(do ~@(for [[i [local]] (map-indexed vector bindings)]
                          `(set! (~(field-accessor i) ~this) ~local))
                      true)))
            false))))))

(defn where-iter [src-acc src-type-sym cols pred]
  (let [t (gensym "WhereIter")
        this (gensym "this")]
    `(deftype ~t [~(with-meta src-field {:tag src-type-sym})]
       Iter
       (nextRow [~this]
         (if (.nextRow ~src-field)
           (let [~@(for [[i col] (map-indexed vector cols)
                         form [col (list '. (src-acc src-field) (getter-sym i))]]
                     form)]
             (if ~pred
               true
               (recur)))
           false)))))

(defn group-by-iter
  [tuple-sym key-cols cols key-tuple-sym val-tuple-sym]
  (let [t (gensym "GroupByIter")
        this (gensym "this")
        entries (with-meta (gensym "entries") {:tag `Iterator})
        entry (with-meta (gensym "entry") {:tag `Map$Entry})
        key (with-meta (gensym "key") {:tag key-tuple-sym})
        al (with-meta (gensym "al") {:tag `ArrayList})
        value (with-meta (gensym "value") {:tag val-tuple-sym})
        all-cols (vec (concat cols key-cols '[%count]))
        %count '%count]
    `(deftype ~t [~entries ~@(col-fields all-cols)]
       ~@(tuple-impl tuple-sym all-cols)
       Iter
       (nextRow [~this]
         (if (.hasNext ~entries)
           (let [~entry (.next ~entries)
                 ~key (.getKey ~entry)
                 ~@(for [[i col] (map-indexed vector key-cols)
                         form [col (list '. key (getter-sym i))]]
                     form)
                 ~al (.getValue ~entry)
                 ~%count (.size ~al)
                 ~@(for [[i col] (map-indexed vector cols)
                         :let [thunk `(let [arr# (object-array ~%count)]
                                        (dotimes [i# ~%count]
                                          (let [~value (.get ~al i#)]
                                            (aset arr# i# (clojure.lang.RT/box ~(list '. value (getter-sym i))))))
                                        arr#)]
                         form [col `(col/->Column nil (fn [] ~thunk))]]
                     form)]
             ~@(for [[i col] (map-indexed vector all-cols)]
                `(set! ~(field-sym i) ~col))
             true)
           false)))))

(defn join-iter [tuple-sym
                 left-acc left-type-sym left-cols
                 right-acc right-type-sym right-cols
                 joined-cols
                 pred]
  (let [t (gensym "JoinIter")
        this (gensym "this")
        left-field (with-meta (gensym "left-iter") {:tag left-type-sym})
        right-field (with-meta (gensym "right-iter") {:tag right-type-sym})
        right-field-acc (with-meta (list (symbol (str ".-" right-field)) this) {:tag right-type-sym})
        right-src-field (gensym "right-src-field")]
    `(deftype ~t [~left-field ~right-field ~right-src-field ~@(map-indexed col-field joined-cols)]
       ~@(tuple-impl tuple-sym joined-cols)
       Iter
       (nextRow [~this]
         (if-not ~right-field-acc
           (if (.nextRow ~left-field)
             (do (set! ~right-field-acc (~right-src-field))
                 (recur))
             false)
           (if (.nextRow ~right-field-acc)
             (let [~@(for [[i col] (map-indexed vector left-cols)
                           form [col (list '. (left-acc left-field) (getter-sym i))]]
                       form)
                   ~@(for [[i col] (map-indexed vector right-cols)
                           form [col (list '. (right-acc right-field-acc) (getter-sym i))]]
                       form)]
               ~(if (= ::no-pred pred)
                  `(do
                     ~@(for [[i col] (map-indexed vector joined-cols)]
                         `(set! ~(field-sym i) ~col))
                     true)
                  `(if ~pred
                     (do
                       ~@(for [[i col] (map-indexed vector joined-cols)]
                           `(set! ~(field-sym i) ~col))
                       true)
                     (recur))))
             (do (set! ~right-field-acc nil)
                 (recur))))))))

(defn equi-join-iter [tuple-sym
                      left-tuple-sym left-cols
                      right-acc right-type-sym right-cols right-key
                      joined-cols
                      theta-pred]
  (let [t (gensym "EquiJoinIter")
        this (gensym "this")
        hm (with-meta (gensym "hm") {:tag `HashMap})
        matches (with-meta (gensym "matches") {:tag `Iterator, :unsynchronized-mutable true})
        matches-acc (with-meta (list (symbol (str ".-" matches)) this) {:tag `Iterator})
        right-field (with-meta (gensym "right-iter") {:tag right-type-sym})
        used-in-right-key? (set (expr/possible-dependencies right-cols right-key))
        o (with-meta (gensym "o") {:tag left-tuple-sym})
        al (with-meta (gensym "al") {:tag `ArrayList})]
    `(deftype ~t [~hm ~matches ~right-field ~@(map-indexed col-field joined-cols)]
       ~@(tuple-impl tuple-sym joined-cols)
       Iter
       (nextRow [~this]
         (cond
           ~matches-acc
           (if (.hasNext ~matches-acc)
             (let [~o (.next ~matches-acc)
                   ~@(for [[i col] (map-indexed vector left-cols)
                           form [col (list '. o (getter-sym i))]]
                       form)
                   ~@(for [[i col] (map-indexed vector right-cols)
                           form [col (list '. (right-acc right-field) (getter-sym i))]]
                       form)]
               ~(if (= ::no-pred theta-pred)
                  `(do
                     ~@(for [[i col] (map-indexed vector joined-cols)]
                         `(set! ~(field-sym i) ~col))
                     true)
                  `(if ~theta-pred
                     (do
                       ~@(for [[i col] (map-indexed vector joined-cols)]
                           `(set! ~(field-sym i) ~col))
                       true)
                     (recur))))
             (do (set! ~matches-acc nil)
                 (recur)))

           (.nextRow ~right-field)
           (let [~@(for [[i col] (map-indexed vector right-cols)
                         :when (used-in-right-key? col)
                         form [col (list '. (right-acc right-field) (getter-sym i))]]
                     form)
                 key# ~right-key
                 ~al (.get ~hm key#)]
             (if ~al
               (do (set! ~matches-acc (.iterator ~al))
                   (recur))
               (recur)))
           :else false)))))

(defn semi-join-iter
  [left-acc left-type-sym left-cols
   right-acc right-type-sym right-cols
   pred]
  (let [t (gensym "SemiJoinIter")
        this (gensym "this")
        left-field (with-meta (gensym "left-iter") {:tag left-type-sym})
        right (with-meta (gensym "right-iter") {:tag right-type-sym})
        right-src-field (gensym "right-src-field")]
    `(deftype ~t [~left-field ~right-src-field]
       Iter
       (nextRow [~this]
         (if (.nextRow ~left-field)
           (let [~right (~right-src-field)]
             (if (.nextRow ~right)
               (let [~@(for [[i col] (map-indexed vector left-cols)
                             form [col (list '. (left-acc left-field) (getter-sym i))]]
                         form)
                     ~@(for [[i col] (map-indexed vector right-cols)
                             form [col (list '. (right-acc right) (getter-sym i))]]
                         form)]
                 ~(if (= ::no-pred pred)
                    true
                    `(let [~@(for [[i col] (map-indexed vector left-cols)
                                   form [col (list '. (left-acc left-field) (getter-sym i))]]
                               form)
                           ~@(for [[i col] (map-indexed vector right-cols)
                                   form [col (list '. (right-acc right) (getter-sym i))]]
                               form)]
                       (if ~pred
                         true
                         (recur)))))
               (recur))))))))

(declare compile-iterable)

(def debug false)

(defn- eval-definition ^Class [form]
  (when debug (clojure.pprint/pprint form))
  (eval
    `(binding [*warn-on-reflection* true]
       ~form)))

(defn compile-plan* [plan]
  (m/match plan
    [::plan/scan ?src ?bindings]
    (let [cols (plan/columns plan)
          tuple-type (eval-definition (tuple-interface cols))
          tuple-sym (symbol (.getName tuple-type))

          iter-def (scan-iter tuple-sym cols ?bindings)
          iter-type (eval-definition iter-def)
          iter-sym (symbol (.getName iter-type))]
      {
       ;; the symbol for the Iter type
       :type-sym iter-sym
       ;; the symbol for the ITupleXXX interface for the Iter
       :tuple-sym tuple-sym
       ;; a function that returns a clojure form for the tuple (having fields f0 ... fN
       :type-acc identity
       ;; a clojure form that will construct the Iter
       ;; todo if ?src is not dependent we want to define it as a preamble, otherwise it might be re-evaluated every step of a loop
       ;; note: is this the same as const-expr elimination?
       :form `(new ~iter-sym (iterator ~(expr/rewrite [] ?src compile-iterable)) ~@(repeat (count cols) nil))})

    [::plan/group-by ?ra ?bindings]
    (let [all-cols (plan/columns plan)
          cols (plan/columns ?ra)
          key-cols (mapv first ?bindings)
          ra-col-map (plan/column-map ?ra)
          key-exprs (mapv (fn [[_ expr]] (expr/rewrite [ra-col-map] expr compile-iterable)) ?bindings)

          key-tuple-interface-type (eval-definition (tuple-interface key-cols))
          key-tuple-interface-sym (symbol (.getName key-tuple-interface-type))
          key-tuple-type (eval-definition (tuple-standalone key-tuple-interface-sym key-cols))
          key-tuple-sym (symbol (.getName key-tuple-type))

          {ra-tuple-sym :tuple-sym
           ra-type-sym :type-sym
           ra-acc :type-acc
           ra-form :form}
          (compile-plan* ?ra)

          ra-cols (plan/columns ?ra)
          val-tuple-type (eval-definition (tuple-standalone ra-tuple-sym ra-cols))
          val-tuple-sym (symbol (.getName val-tuple-type))

          tuple-type (eval-definition (tuple-interface all-cols))
          tuple-sym (symbol (.getName tuple-type))

          iter-def (group-by-iter tuple-sym key-cols cols key-tuple-sym val-tuple-sym)
          iter-type (eval-definition iter-def)
          iter-sym (symbol (.getName iter-type))

          build (with-meta (gensym "build") {:tag ra-type-sym})
          al (with-meta (gensym "al") {:tag `ArrayList})]
      {:type-sym iter-sym
       :tuple-sym tuple-sym
       :type-acc identity
       :form `(new ~iter-sym
                   ((fn []
                      (let [~build ~ra-form
                            hm# (HashMap.)]
                        (while (.nextRow ~build)
                          (let [~@(for [[i col] (map-indexed vector ra-cols)
                                        form [col (list '. (ra-acc build) (getter-sym i))]]
                                    form)
                                key# (new ~key-tuple-sym 0 ~@key-exprs)
                                ~al (.get hm# key#)]
                            (if ~al
                              (.add ~al (new ~val-tuple-sym 0 ~@ra-cols))
                              (let [~al (ArrayList.)]
                                (.add ~al (new ~val-tuple-sym 0 ~@ra-cols))
                                (.put hm# key# ~al)))))
                        (.iterator (.entrySet hm#)))))
                   ~@(repeat (count all-cols) nil))})

    [::plan/where [::plan/scan ?src ?bindings] ?pred]
    (let [cols (plan/columns plan)
          tuple-type (eval-definition (tuple-interface cols))
          tuple-sym (symbol (.getName tuple-type))
          pred (expr/rewrite [(plan/column-map plan)] ?pred compile-iterable)
          iter-def (scan-iter tuple-sym cols ?bindings pred)
          iter-type (eval-definition iter-def)
          iter-sym (symbol (.getName iter-type))]
      {:type-sym iter-sym
       :tuple-sym tuple-sym
       :type-acc identity
       :form `(new ~iter-sym (iterator ~(expr/rewrite [] ?src compile-iterable)) ~@(repeat (count cols) nil))})

    [::plan/where ?src ?pred]
    (let [{src-sym :type-sym
           tuple-sym :tuple-sym
           src-acc :type-acc
           src-form :form} (compile-plan* ?src)
          cols (plan/columns ?src)
          iter-def (where-iter src-acc src-sym cols (expr/rewrite [(plan/column-map ?src)] ?pred compile-iterable))
          iter-type (eval-definition iter-def)
          iter-sym (symbol (.getName iter-type))]
      {:type-sym iter-sym
       :tuple-sym tuple-sym
       :type-acc (comp (fn [s] (list src-field-accessor s)) src-acc)
       :form `(new ~iter-sym ~src-form)})


    [::plan/cross-join ?left ?right]
    (let [{left-sym :type-sym
           left-acc :type-acc
           left-form :form} (compile-plan* ?left)
          {right-sym :type-sym
           right-acc :type-acc
           right-form :form} (compile-plan* ?right)

          left-cols (plan/columns ?left)
          right-cols (plan/columns ?right)
          joined-cols (plan/columns plan)

          tuple-def (tuple-interface joined-cols)
          tuple-type (eval-definition tuple-def)
          tuple-sym (symbol (.getName tuple-type))

          iter-def (join-iter tuple-sym left-acc left-sym left-cols right-acc right-sym right-cols joined-cols ::no-pred)
          iter-type (eval-definition iter-def)
          iter-sym (symbol (.getName iter-type))]
      {:type-sym iter-sym
       :tuple-sym tuple-sym
       :type-acc identity
       :form `(new ~iter-sym ~left-form nil (fn [] ~right-form) ~@(repeat (count joined-cols) nil))})

    [::plan/join ?left ?right ?pred]
    (let [{left-sym :type-sym
           left-tuple-sym :tuple-sym
           left-acc :type-acc
           left-form :form} (compile-plan* ?left)
          {right-sym :type-sym
           right-acc :type-acc
           right-form :form} (compile-plan* ?right)

          left-cols (plan/columns ?left)
          right-cols (plan/columns ?right)
          joined-cols (plan/columns plan)

          tuple-def (tuple-interface joined-cols)
          tuple-type (eval-definition tuple-def)
          tuple-sym (symbol (.getName tuple-type))

          {:keys [left-key right-key theta]
           :or {theta [true]}}
          (plan/equi-theta ?left ?right ?pred)]
      (if (seq left-key)
        (let [left-key (expr/rewrite [(plan/column-map ?left)] left-key compile-iterable)
              right-key (expr/rewrite [(plan/column-map ?right)] right-key compile-iterable)
              theta (if (= [true] theta)
                      ::no-pred
                      (expr/rewrite [(plan/column-map ?left) (plan/column-map ?right)] `(and ~@theta) compile-iterable))
              iter-def (equi-join-iter tuple-sym left-tuple-sym left-cols right-acc right-sym right-cols right-key joined-cols theta)
              iter-type (eval-definition iter-def)
              iter-sym (symbol (.getName iter-type))
              build (gensym "build")
              al (with-meta (gensym "al") {:tag `ArrayList})
              ts-type (eval-definition (tuple-standalone left-tuple-sym left-cols))
              left-tuple-standalone-sym (symbol (.getName ts-type))]
          {:type-sym iter-sym
           :tuple-sym tuple-sym
           :type-acc identity
           :form `(new ~iter-sym
                       ((fn []
                          (let [hm# (HashMap.)
                                ~build ~left-form]
                            (while (.nextRow ~build)
                              (let [~@(for [[i col] (map-indexed vector left-cols)
                                            form [col (list '. (left-acc build) (getter-sym i))]]
                                        form)
                                    key# ~left-key
                                    ~al (.get hm# key#)]
                                (if ~al
                                  (.add ~al (new ~left-tuple-standalone-sym 0 ~@left-cols))
                                  (let [~al (ArrayList.)]
                                    (.add ~al (new ~left-tuple-standalone-sym 0 ~@left-cols))
                                    (.put hm# key# ~al)))))
                            hm#)
                          ))
                       nil ~right-form ~@(repeat (count joined-cols) nil))})
        (let [iter-def (join-iter tuple-sym left-acc left-sym left-cols right-acc right-sym right-cols joined-cols ::no-pred)
              iter-type (eval-definition iter-def)
              iter-sym (symbol (.getName iter-type))]
          {:type-sym iter-sym
           :tuple-sym tuple-sym
           :type-acc identity
           :form `(new ~iter-sym ~left-form nil (fn [] ~right-form) ~@(repeat (count joined-cols) nil))})))

    ))

(defn compile-loop [plan cont]
  (let [{:keys [type-sym, type-acc, form]} (compile-plan* plan)
        cols (plan/columns plan)
        iter (with-meta (gensym "iter") {:tag type-sym})]
    `(let [~iter ~form]
       (loop []
         (when (.nextRow ~iter)
           (let [~@(for [[i col] (map-indexed vector cols)
                         form [col (list '. (type-acc iter) (getter-sym i))]]
                     form)]
             ~cont
             (recur)))))))

(defn compile-iterable
  ([plan]
   (m/match plan
     [::plan/project ?plan ?projection]
     (compile-iterable ?plan ?projection)
     _
     (compile-iterable plan nil)))
  ([plan projection]
   (let [{:keys [type-sym, tuple-sym, type-acc, form]} (compile-plan* plan)
         cols (plan/columns plan)
         iter (with-meta (gensym "iter") {:tag type-sym})]
     `(reify
        Sequential
        Seqable
        (seq [this#] (iterator-seq (.iterator this#)))
        Iterable
        (iterator [_#]
          (let [~iter ~form]
            (reify Iterator
              (hasNext [_#] (.nextRow ~iter))
              (next [_#]
                (let [~@(for [[i col] (map-indexed vector cols)
                              form [col (list '. (with-meta (type-acc iter) {:tag tuple-sym}) (getter-sym i))]]
                          form)
                      ~@(for [[sym expr] projection
                              form [sym (expr/rewrite [(plan/column-map plan)] expr compile-iterable)]]
                          form)]
                  ~(if-not projection
                     (case (count cols)
                       1 (first cols)
                       (vec cols))
                     (case (count projection)
                       1 (ffirst projection)
                       (mapv first projection))))))))))))

(defmacro q
  ([ra] `(q ~ra :cinq/*))
  ([ra expr]
   (let [lp (parse/parse (list 'q ra expr))
         lp' (plan/rewrite lp)]
     (binding [*warn-on-reflection* true]
       (compile-iterable lp')))))

(comment

  (vec (q [a [1, 2, 3]]))
  (macroexpand '(q [a [1, 2, 3]]))

  (vec (q [a [1, 2, 3] :when (= a 2)]))
  (macroexpand '(q [a [1, 2, 3] :when (= a 2)]))

  )

;; spliterator

'(deftype JoinIter2344 [left-iter, right-src, right-iter, side, f0, f1, f2]
   Iter
   (nextRow [s]
     (if (.nextRow left-iter)
       (let [left-bind-a (.-f0 left-iter)]
         (when-not right-iter (set! right-iter (.iter right-src)))
         (loop []
           (do (if (.nextRow right this)
                 (let [right-bind-a (.f0 right-iter)
                       right-bind-b (.f1 right-iter)]
                   (if (= left-bind-a right-bind-a)
                     (do
                       (set! f0 left-bind-a)
                       (set! f1 right-bind-a)
                       (set! f1 right-bind-b)
                       true)
                     (recur)))
                 (set! right (.iter right-src))
                 (recur)))))
       false)))

'(deftype EquiJoinIter2344 [left-ht, right-iter, pred, f0, f1, f2]
   Iter
   (nextRow [s]
     (if (.nextRow right-iter)
       (let [right-bind-a (.-f0 right-iter)
             right-bind-b (.-f1 right-iter)
             right-key (new K3323 right-bind-a)
             left-matches (.get left-ht right-key)]
         (set! matches left-matches)
         (set! match-idx left-matches)
         (set! f1 right-bind-a)
         (set! f2 right-bind-b)
         (recur))
       false)))

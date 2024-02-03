(ns com.wotbrew.cinq.row-major
  (:require [com.wotbrew.cinq.column :as col]
            [com.wotbrew.cinq.expr :as expr]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.plan2 :as plan]
            [meander.epsilon :as m])
  (:import (clojure.lang ArrayIter IRecord RT Seqable Sequential)
           (java.lang.reflect Field)
           (java.util ArrayList HashMap Iterator Map$Entry)
           (java.util.function Consumer)))

(defn untag [sym] (vary-meta sym dissoc :tag))

(set! *warn-on-reflection* true)

(definterface Iter
  (nextRow ^boolean []))

(def oarr (Class/forName "[Ljava.lang.Object;"))

(defn iterator ^Iterator [src]
  (if (instance? oarr src)
    (ArrayIter/create src)
    (.iterator ^Iterable src)))

(def scan-src-field (with-meta 'cinq__scan-src {:tag `Iterator}))

(def src-field 'cinq__src)
(def src-field-accessor '.-cinq__src)

(defn col-field [i col]
  (with-meta
    (symbol (str "f" i))
    (merge {:unsynchronized-mutable true}
           (select-keys (meta col) [:tag]))))

(defn col-fields [cols] (map-indexed col-field cols))

(defn field-sym [i] (symbol (str "f" i)))
(defn field-accessor [i] (symbol (str ".-" (field-sym i))))

(defn kw-field [^Class class kw]
  (when (isa? class IRecord)
    (let [munged-name (munge (name kw))]
      (->> (.getFields class)
           (some (fn [^Field f] (when (= munged-name (.getName f)) f)))))))

(defn getter-sym [i] (symbol (str "getF" i)))

(defn tuple-interface* [types]
  (let [s (gensym "ITuple")]
    `(definterface ~s
       ~@(for [[i t] (map-indexed vector types)]
           (list (with-meta (getter-sym i) {:tag t}) [])))))

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
        hsym (with-meta '__hashcode {:tag 'int, :unsynchronized-mutable true})
        hsymu (with-meta hsym {})]
    `(deftype ~t [~hsym ~@(col-fields cols)]
       ~@(tuple-impl interface-sym cols)
       ;; todo hasheq by default, java eq might want to be opt-in?
       Object
       (equals [_# ~osym]
         (and (instance? ~interface-sym ~osym)
              (let [~tosym ~osym]
                (and ~@(for [i (range (count cols))]
                         `(clojure.lang.Util/equals ~(field-sym i) (. ~tosym ~(getter-sym i))))))))
       (hashCode [_#]
         (if (= 0 ~hsymu)
           (let [hc# ~(case (count cols)
                        0 42
                        (reduce
                          (fn [form i] `(add-hash ~form (.hashCode (RT/box ~(field-sym i)))))
                          1
                          (range (count cols))))]
             (set! ~hsymu hc#)
             hc#)
           ~hsymu)))))

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
                          form [(untag sym)
                                (cond
                                  (= :cinq/self k) o
                                  (and self-class (class? self-class) (kw-field self-class k))
                                  (list (symbol (str ".-" (munge (name k)))) o)
                                  (keyword? k) (list k o)
                                  :else (list `get o k))]]
                      form)]
              ~(if-not (= ::no-pred pred)
                 `(if ~pred
                    (do
                      ~@(for [[i [local]] (map-indexed vector bindings)]
                          `(set! (~(field-accessor i) ~this) ~(untag local)))
                      true)
                    (recur))
                 `(do ~@(for [[i [local]] (map-indexed vector bindings)]
                          `(set! (~(field-accessor i) ~this) ~(untag local)))
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
                         form [(untag col) (list '. (src-acc src-field) (getter-sym i))]]
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
        all-cols (vec (concat (mapv plan/group-column-tag cols) key-cols [plan/%count-sym]))
        %count '%count]
    `(deftype ~t [~entries ~@(col-fields all-cols)]
       ~@(tuple-impl tuple-sym all-cols)
       Iter
       (nextRow [~this]
         (if (.hasNext ~entries)
           (let [~entry (.next ~entries)
                 ~key (.getKey ~entry)
                 ~@(for [[i col] (map-indexed vector key-cols)
                         form [(untag col) (list '. key (getter-sym i))]]
                     form)
                 ~al (.getValue ~entry)
                 ~%count (unchecked-long (.size ~al))
                 ~@(for [[i col] (map-indexed vector cols)
                         :let [[array box column]
                               (condp = (:tag (meta col))
                                 'double [`double-array false `col/->DoubleColumn]
                                 'long [`long-array false `col/->LongColumn]
                                 [`object-array true `col/->Column])

                               thunk `(let [arr# (~array ~%count)]
                                        (dotimes [i# ~%count]
                                          (let [~value (.get ~al i#)]
                                            (aset arr# i# ~(let [getf (list '. value (getter-sym i))]
                                                             (if box
                                                               `(RT/box ~getf)
                                                               getf)))))
                                        arr#)]
                         form [(untag col) `(~column nil (fn [] ~thunk))]]
                     form)]
             ~@(for [[i col] (map-indexed vector all-cols)]
                 `(set! ~(field-sym i) ~(untag col)))
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
                           form [(untag col) (list '. (left-acc left-field) (getter-sym i))]]
                       form)
                   ~@(for [[i col] (map-indexed vector right-cols)
                           form [(untag col) (list '. (right-acc right-field-acc) (getter-sym i))]]
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
                           form [(untag col) (list '. o (getter-sym i))]]
                       form)
                   ~@(for [[i col] (map-indexed vector right-cols)
                           form [(untag col) (list '. (right-acc right-field) (getter-sym i))]]
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
                         form [(untag col) (list '. (right-acc right-field) (getter-sym i))]]
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
                             form [(untag col) (list '. (left-acc left-field) (getter-sym i))]]
                         form)
                     ~@(for [[i col] (map-indexed vector right-cols)
                             form [(untag col) (list '. (right-acc right) (getter-sym i))]]
                         form)]
                 ~(if (= ::no-pred pred)
                    true
                    `(let [~@(for [[i col] (map-indexed vector left-cols)
                                   form [(untag col) (list '. (left-acc left-field) (getter-sym i))]]
                               form)
                           ~@(for [[i col] (map-indexed vector right-cols)
                                   form [(untag col) (list '. (right-acc right) (getter-sym i))]]
                               form)]
                       (if ~pred
                         true
                         (recur)))))
               (recur))))))))

(declare compile-iterable)

(def debug false)

(defn- eval-definition ^Class [form]
  (when debug (binding [*print-meta* true] (clojure.pprint/pprint form)))
  (eval
    `(binding [*warn-on-reflection* true]
       ~form)))

(defn column-defaults [cols]
  (map
    (fn [col] (let [tag (:tag (meta col))]
                (condp = tag
                  'boolean false
                  'long 0
                  'int 0
                  'short 0
                  'byte 0
                  'double 0.0
                  'float 0.0
                  nil)))
    cols))

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
       :form `(new ~iter-sym (iterator ~(expr/rewrite [] ?src compile-iterable)) ~@(column-defaults cols))})

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
          al (with-meta (gensym "al") {:tag `ArrayList})
          value (with-meta (gensym "value") {:tag val-tuple-sym})
          used-in-key? (set (expr/possible-dependencies ra-cols key-exprs))]
      {:type-sym iter-sym
       :tuple-sym tuple-sym
       :type-acc identity
       :form `(new ~iter-sym
                   ((fn build-groups# []
                      (let [~build ~ra-form
                            al# (ArrayList.)]
                        (while (.nextRow ~build)
                          (let [~@(for [[i col] (map-indexed vector ra-cols)
                                        form [(untag col) (list '. (ra-acc build) (getter-sym i))]]
                                    form)
                                val# (new ~val-tuple-sym (unchecked-int 0) ~@(map untag ra-cols))]
                            (.add al# val#)))
                        ;; todo hasheq by default, java eq might want to be opt-in?
                        (let [hm# (HashMap. 32)]
                          (.forEach
                            al#
                            (reify Consumer
                              (accept [_# val#]
                                (let [~value val#
                                      ~@(for [[i col] (map-indexed vector ra-cols)
                                              :when (used-in-key? col)
                                              form [(untag col) (list '. value (getter-sym i))]]
                                          form)
                                      key# (new ~key-tuple-sym (unchecked-int 0) ~@key-exprs)
                                      ~al (.get hm# key#)]
                                  (if ~al
                                    (.add ~al ~value)
                                    (let [~al (ArrayList.)]
                                      (.add ~al ~value)
                                      (.put hm# key# ~al)))))))
                          (.iterator (.entrySet hm#))))))
                   ~@(column-defaults all-cols))})

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
       :form `(new ~iter-sym (iterator ~(expr/rewrite [] ?src compile-iterable)) ~@(column-defaults cols))})

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
       :form `(new ~iter-sym ~left-form nil (fn [] ~right-form) ~@(column-defaults joined-cols))})

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
                       ((fn build-join-hm# []
                          (let [hm# (HashMap.)
                                ~build ~left-form]
                            (while (.nextRow ~build)
                              (let [~@(for [[i col] (map-indexed vector left-cols)
                                            form [(untag col) (list '. (left-acc build) (getter-sym i))]]
                                        form)
                                    key# ~left-key
                                    ~al (.get hm# key#)]
                                (if ~al
                                  (.add ~al (new ~left-tuple-standalone-sym 0 ~@(map untag left-cols)))
                                  (let [~al (ArrayList.)]
                                    (.add ~al (new ~left-tuple-standalone-sym 0 ~@(map untag left-cols)))
                                    (.put hm# key# ~al)))))
                            hm#)
                          ))
                       nil ~right-form ~@(column-defaults joined-cols))})
        (let [iter-def (join-iter tuple-sym left-acc left-sym left-cols right-acc right-sym right-cols joined-cols ::no-pred)
              iter-type (eval-definition iter-def)
              iter-sym (symbol (.getName iter-type))]
          {:type-sym iter-sym
           :tuple-sym tuple-sym
           :type-acc identity
           :form `(new ~iter-sym ~left-form nil (fn [] ~right-form) ~@(column-defaults joined-cols))})))

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
                              form [(untag col) (list '. (with-meta (type-acc iter) {:tag tuple-sym}) (getter-sym i))]]
                          form)
                      ~@(for [[sym expr] projection
                              form [(untag sym) (expr/rewrite [(plan/column-map plan)] expr compile-iterable)]]
                          form)]
                  ~(let [output (if projection (mapv (comp untag first) projection) (mapv untag cols))]
                     (case (count output)
                       1 (first output)
                       (vec output))))))))))))

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

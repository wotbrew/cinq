(ns com.wotbrew.cinq.eager-loop
  (:require [com.wotbrew.cinq.column :as col]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.plan2 :as plan]
            [meander.epsilon :as m]
            [com.wotbrew.cinq.array-seq :as aseq])
  (:import (clojure.lang RT Util)
           (java.lang.reflect Field)
           (java.util ArrayList HashMap Iterator Spliterator)
           (java.util.function BiConsumer Consumer)))

;; todo push type tags in plan
;; todo primitive aggregations for different array types
;; todo PROBLEM: ::plan/limit,
;; loop-ify should take an additional arg, each operator should try to be lazy up to the limit
;; this may cause different loop strategies to be used to accommodate the additional counter in the loop (e.g iterator instead of .forEach)

(defn tuple-field-sym [i] (symbol (str "field" i)))
(defn tuple-get-field-list [i expr] (list (symbol (str ".-field" i)) expr))

(defmacro add-hash [hash1 hash2]
  `(unchecked-multiply-int 31 (unchecked-add-int ~hash1 ~hash2)))

(def tuple-type*
  (memoize
    (fn tt* [k]
      (let [s (gensym "TT")
            osym (gensym "o")
            tosym (with-meta osym {:tag s})
            hsym (with-meta '__hashcode {:tag 'int, :unsynchronized-mutable true})]
        (eval `(deftype ~s [~hsym ~@(for [[i tag] (map-indexed vector k)] (with-meta (tuple-field-sym i) {:type tag}))]
                 Object
                 (equals [_# ~osym]
                   (and (instance? ~s ~osym)
                        (let [~tosym ~osym]
                          (and ~@(for [i (range (count k))]
                                   `(= ~(tuple-field-sym i) ~(tuple-get-field-list i tosym)))))))
                 (hashCode [_#]
                   (if (= 0 ~hsym)
                     (let [hc# ~(case (count k)
                                  0 42
                                  (reduce
                                    (fn [form i] `(add-hash ~form (Util/hash ~(tuple-field-sym i))))
                                    1
                                    (range (count k))))]
                       (set! ~hsym hc#)
                       hc#)
                     ~hsym))))))))

(defn tuple-type ^Class [cols]
  (let [k (mapv (fn [s] (:tag (meta s) `Object)) cols)]
    (tuple-type* k)))

(defn tuple-type-sym [cols] (symbol (.getName (tuple-type cols))))

(defn construct-tuple [cols]
  `(new ~(tuple-type-sym cols) 0 ~@cols))

(defn key-lambda [key-syms]
  `(fn key-function# [~@key-syms]
     ~(case (count key-syms)
        1 (first key-syms)
        (construct-tuple key-syms))))

(defn rt-add-to-hm-group [^HashMap hm ^Object k tt]
  (let [^ArrayList l (.get hm k)]
    (if l
      (.add l tt)
      (.put hm k (doto (ArrayList.) (.add tt))))))

(defn add-to-group [ht-sym keyfn-sym key-syms tt-expr]
  `(rt-add-to-hm-group ~ht-sym (~keyfn-sym ~@key-syms) ~tt-expr))

(defn create-array [column-syms]
  (let [arr-sym (gensym "arr")]
    `(let [~arr-sym (object-array ~(count column-syms))]
       ~@(for [[i csym] (map-indexed vector column-syms)]
           `(aset ~arr-sym ~i (RT/box ~csym)))
       ~arr-sym)))

(defn destructure-group-bindings-from-key [key-sym binding-syms]
  (assert (pos? (count binding-syms)))
  (case (count binding-syms)
    1 [(first binding-syms) key-sym]
    (for [[i sym] (map-indexed vector binding-syms)
          form [sym (tuple-get-field-list i key-sym)]]
      form)))

(defn create-grouped-column-bindings [column-syms list-sym]
  (case (count column-syms)
    1
    (let [col-sym (first column-syms)]
      [col-sym `(col/->Column nil (fn [] (.toArray ~list-sym)))])

    (let [t (with-meta (gensym "t") {:tag (tuple-type-sym column-syms)})]
      (for [[i sym] (map-indexed vector column-syms)
            form [sym `(col/->Column nil
                                     (fn build-col# []
                                       (let [arr# (object-array (.size ~list-sym))]
                                         (dotimes [j# (.size ~list-sym)]
                                           (let [~t (.get ~list-sym j#)]
                                             (aset arr# j# (RT/box ~(tuple-get-field-list i t)))))
                                         arr#)))]]
        form))))

(declare to-array-list)

(defn has-kw-field? [^Class class kw]
  (and (isa? class clojure.lang.IRecord)
       (let [munged-name (munge (name kw))]
         (->> (.getFields class)
              (some (fn [^Field f] (= munged-name (.getName f))))))))

(defn iterator ^Iterator [obj]
  (cond
    (instance? Iterable obj) (.iterator ^Iterable obj)
    (nil? obj) (.iterator ())
    :else (.iterator ^Iterable (seq obj))))

'
(deftype ScanSpliterator [src-spliterator
                          ^:unsynchronized-mutable ^boolean match
                          ^:unsynchronized-mutable col0
                          ^:unsynchronized-mutable col1]
  Consumer
  (accept [_ o]
    (let [o o
          foo (.-foo o)]
      (if ~pred
        (do (set! col0 row-count o)
            (set! col1 row-count foo)
            (set! match true))
        (set! match false))))
  Spliterator
  (tryAdvance [this c]
    (loop []
      (if (.tryAdvance src-spliterator this)
        (if (.-match this)
          (do (.accept c this) true)
          (recur))
        false))))

'(deftype NestedLoop [left-spliterator
                      right-spliterable
                      ^:unsynchronized-mutable right-spliterator
                      ^:unsynchronized-mutable ^boolean match
                      ^:unsynchronized-mutable col0
                      ^:unsynchronized-mutable col1
                      ^:unsynchronized-mutable col2]
   Consumer
   ;; accept left
   (accept [_ o]
     (let [t
           o (.getCol0 t)
           foo (.getCol1 t)]
       (if ~pred
         (do (set! col0 row-count o)
             (set! col1 row-count foo)
             (set! match true))
         (set! match false))))
   Spliterator
   (tryAdvance [this c]
     (if right-spliterator

       (loop []
         (if (.tryAdvance left-spliterator this)
           (let [spliterator (.spliterator right-spliterable)]
             )
           (if (.-match this)
             (do (.accept c this) true)
             (recur))
           false)))))

;; consider ::plan/limit feedback
;; extra args limit (if nil no limit), cont (if nil, return an iterable?).
;; we should know whether the return is materialized and how?
(defn loop-ify
  [ra cont]
  (letfn [(rewrite-expr [expr & ra] (aseq/rewrite-expr (mapv #(plan/dependent-cols % expr) ra) expr))]
    (m/match ra
      [::plan/scan ?src ?bindings]
      (let [self-binding (last (keep #(when (= :cinq/self (second %)) (first %)) ?bindings))
            self-tag (:tag (meta self-binding))
            self-class (if (symbol? self-tag) (resolve self-tag) self-tag)
            osym (with-meta (gensym "o") {:tag self-tag})]
        `(let [run-step# (fn [~osym]
                           (let [~@(for [[sym k] ?bindings
                                         form [sym (cond
                                                     (= :cinq/self k) osym
                                                     ;; specialisation for records
                                                     (and self-class (class? self-class) (has-kw-field? self-class k))
                                                     (list (symbol (str ".-" (munge (name k)))) osym)
                                                     ;; this gives you a KeywordLookupSite, which can sometimes be faster than RT.get
                                                     ;; needs testing as to whether worth given the IRecord specialisation above
                                                     (keyword? k) (list k osym)
                                                     :else (list `get osym k))]]
                                     form)]
                             ~cont))]
           (run! run-step# ~?src)))

      [::plan/project ?ra [[?sym ?expr]]]
      (loop-ify ?ra `(let [~?sym ~(rewrite-expr ?expr ?ra)] ~cont))

      [::plan/where ?ra ?pred]
      (loop-ify ?ra `(when ~(rewrite-expr ?pred ?ra) ~cont))

      [::plan/cross-join ?ra1 ?ra2]
      (loop-ify [::plan/join ?ra1 ?ra2 ::no-pred] cont)

      ;; todo a block nested loop, we would need to be able to control the start/stop from the underlying iterator
      ;; again like limit there would be an iterator-control requirement.
      [::plan/join ?ra1 ?ra2 ?pred]
      (if (= :no-pred ?pred)
        (loop-ify ?ra1 (loop-ify ?ra2 cont))
        (loop-ify ?ra1 (loop-ify ?ra2 `(when (rewrite-expr ?pred ?ra1 ?ra2) ~cont))))

      [::plan/apply :cross-join ?ra1 ?ra2]
      (loop-ify ?ra1 (loop-ify ?ra2 cont))


      [::plan/group-by ?ra ?bindings]
      (let [bindings (vec (for [[sym e] ?bindings]
                            [sym (aseq/rewrite-expr [(plan/dependent-cols ?ra e)] e)]))
            cols (plan/columns ?ra)]
        (case (count ?bindings)
          0
          (let [list-sym (gensym "list")]
            `(let [~list-sym ((fn [] ~(to-array-list ?ra)))
                   ~@(create-grouped-column-bindings cols list-sym)]
               ~cont))
          (let [ht-sym (gensym "ht")
                keyfn-sym (gensym "kfn")
                key-syms (mapv first bindings)
                key-sym (with-meta (gensym "key") (if (= 1 (count ?bindings)) {} {:tag (tuple-type-sym key-syms)}))
                val-sym (with-meta (gensym "val") {:tag `ArrayList})]
            `(let [~ht-sym (HashMap.)
                   ~keyfn-sym ~(key-lambda key-syms)
                   run-entry-step#
                   (reify BiConsumer
                     (accept [_# k# v#]
                       (let [~key-sym k#
                             ~val-sym v#
                             ~@(destructure-group-bindings-from-key key-sym (map first ?bindings))
                             ~@(create-grouped-column-bindings (plan/columns ?ra) val-sym)
                             ~'%count (.size ~val-sym)]
                         ~cont)))]
               ((fn [] ~(loop-ify ?ra `(let [~@(mapcat identity bindings)] ~(add-to-group ht-sym keyfn-sym key-syms (construct-tuple cols))))))
               (.forEach ~ht-sym run-entry-step#)))))
      _
      (throw (ex-info "Unknown ra" {:ra ra})))))

(defn to-array-list [ra]
  (let [lsym (gensym "l")
        cols (plan/columns ra)]
    `(let [~lsym (ArrayList.)]
       ~(loop-ify ra `(.add ~lsym ~(case (count cols) 1 (first cols) (create-array cols))))
       ~lsym)))


(defmacro q
  ([ra] `(q ~ra :cinq/*))
  ([ra expr]
   (let [lp (parse/parse (list 'q ra expr))
         lp' (plan/rewrite lp)]
     (to-array-list lp'))))

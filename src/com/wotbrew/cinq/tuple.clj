(ns com.wotbrew.cinq.tuple
  (:require [com.wotbrew.cinq.expr :as expr]
            [com.wotbrew.cinq.plan2 :as plan])
  (:import (com.wotbrew.cinq CinqUtil)))

(defn sig
  "Returns a vector of type tags for each of the columns. This is the structural signature of the tuple."
  [ra]
  (mapv (comp (some-fn :tag `Object) meta) (plan/columns ra)))

(defn field-sym [i]
  (symbol (str "field" i)))

(definterface CinqMark
  (getCinqMark ^Boolean [])
  (setCinqMark ^void []))

(def ^{:arglists '([sig])} compile-tuple-type
  (memoize
    (fn [sig]
      (let [s (gensym "Tuple")
            fields (for [[i tag] (map-indexed vector sig)]
                     (with-meta (field-sym i) {:tag (or tag `Object)}))
            o (gensym "o")
            mark (gensym "mark")
            t (eval
                `(deftype ~s [~(with-meta mark {:unsynchronized-mutable true, :tag `Boolean}) ~@fields]
                   CinqMark
                   (getCinqMark [_#] ~mark)
                   (setCinqMark [_#] (set! ~mark true))
                   Object
                   (equals [_# ~o]
                     (and (instance? ~s ~o)
                          ~@(for [i (range (count sig))]
                              `(CinqUtil/eq ~(field-sym i) (. ~(with-meta o {:tag s}) ~(field-sym i))))))
                   (hashCode [_#]
                     ~((fn nh
                         ([] (nh 1 0))
                         ([h i]
                          (if (< i (count fields))
                            (nh `(unchecked-add-int (unchecked-multiply-int 31 ~h) (clojure.lang.Util/hash ~(field-sym i))) (inc i))
                            h)))))))]
        {:type t
         :sym (symbol (str (munge (name (.getName *ns*))) "." (name s)))
         :sig sig
         :fields (vec fields)}))))

;; compile tuple type and return ctor form
(defn emit-tuple [ra]
  (let [{:keys [sym]} (compile-tuple-type (sig ra))]
    `(new ~sym false ~@(for [col (plan/columns ra)] col))))

;; return a hinted local for the ra tuple
(defn tuple-local [ra]
  (with-meta (gensym "t") {:tag (:sym (compile-tuple-type (sig ra)))}))

(defn null-tuple-local [ra]
  (with-meta (gensym "t") {:tag 'objects}))

(defn get-field [t i]
  (if (= 'objects (:tag (meta t)))
    `(aget ~t ~i)
    `(~(symbol (str ".-" (field-sym i))) ~t)))

(defn get-mark [t] `(.getCinqMark ~t))
(defn set-mark [t] `(.setCinqMark ~t))

(defn last-index-of [x xs]
  (loop [i 0
         j nil
         xs xs]
    (if (seq xs)
      (if (= (first xs) x)
        (recur (inc i) i (rest xs))
        (recur (inc i) j (rest xs)))
      j)))

;; return as seq of get-col exprs against t in for the cols in ra/cols
(defn emit-tuple-arg-list [t cols & {:keys [optional]}]
  (for [i (range (count cols))]
    (if optional `(when (some? ~t) ~(get-field t i)) (get-field t i))))

(defn infer-type [expr]
  (:tag (meta expr) `Object))

;; compile key tuple type and return ctor form
(defn emit-key [key-exprs]
  (if (= 1 (count key-exprs))
    (first key-exprs)
    (let [key-sig (mapv infer-type key-exprs)
          {:keys [sym]} (compile-tuple-type key-sig)]
      `(new ~sym false ~@(for [expr key-exprs] (if (= `Object (infer-type expr)) (clojure.lang.RT/box expr) expr))))))

;; return a seq of get-col exprs against k for the (key) bindings
(defn emit-key-args [k cols]
  (if (= 1 (count cols))
    [k]
    (for [i (range (count cols))] (get-field k i))))

(defn key-local [key-exprs]
  (if (= 1 (count key-exprs))
    (gensym "k")
    (let [key-sig (mapv infer-type key-exprs)
          {:keys [sym]} (compile-tuple-type key-sig)]
      (with-meta (gensym "k") {:tag sym}))))

(defn emit-tuple-column-binding
  ([t cols]
   (vec (interleave (map #(with-meta % {}) cols) (emit-tuple-arg-list t cols))))
  ([t cols expr] (emit-tuple-column-binding t cols expr #{}))
  ([t cols expr remove-set]
   (let [used-in-expr (set (expr/possible-dependencies cols expr))]
     (vec (for [[i col] (map-indexed vector cols)
                :when (and (used-in-expr col) (not (contains? remove-set col)))
                form [(with-meta col {}) (get-field t i)]]
            form)))))

(defn emit-optional-column-binding
  ([t cols]
   (vec (interleave (map #(with-meta % {}) cols) (emit-tuple-arg-list t cols :optional true))))
  ([t cols expr]
   (let [used-in-expr (set (expr/possible-dependencies cols expr))]
     (vec (for [[i col] (map-indexed vector cols)
                :when (used-in-expr col)
                form [(with-meta col {}) `(when (some? ~t) ~(get-field t i))]]
            form)))))

(defn emit-key-bindings [k cols]
  (vec (interleave (map #(with-meta % {}) cols) (emit-key-args k cols))))


(ns com.wotbrew.cinq.eager-loop
  (:require [clojure.set :as set]
            [com.wotbrew.cinq.expr :as expr]
            [com.wotbrew.cinq.nio-codec :as codec]
            [com.wotbrew.cinq.plan :as plan]
            [com.wotbrew.cinq.column :as col]
            [com.wotbrew.cinq.protocols :as p]
            [com.wotbrew.cinq.tuple :as t]
            [meander.epsilon :as m])
  (:import (clojure.lang IFn IReduceInit RT)
           (com.wotbrew.cinq CinqMultimap CinqScanFunction CinqScanFunction$NativeFilter CinqUtil)
           (com.wotbrew.cinq.protocols Scannable)
           (java.util ArrayList Comparator HashMap)
           (java.util.function BiFunction Function)))

(set! *warn-on-reflection* true)

(declare emit-loop emit-array emit-iterator emit-iterable)

(defn rewrite-expr [dependent-relations clj-expr]
  (let [col-maps (mapv #(plan/dependent-cols % clj-expr) dependent-relations)]
    (expr/rewrite col-maps clj-expr #(emit-iterable % 0))))

(defn emit-list
  ([ra]
   (let [list-sym (gensym "list")]
     `(let [~list-sym (ArrayList.)]
        ~(emit-loop ra `(do (.add ~list-sym ~(t/emit-tuple ra)) nil))
        ~list-sym)))
  ([ra col-idx]
   (let [list-sym (gensym "list")
         cols (plan/columns ra)
         col (nth cols col-idx)]
     `(let [~list-sym (ArrayList.)]
        ~(emit-loop ra `(do (.add ~list-sym (clojure.lang.RT/box ~col)) nil))
        ~list-sym))))

(defn emit-array [ra] `(.toArray ~(emit-list ra)))

(defn emit-iterable ([ra] (emit-list ra)) ([ra col-idx] (emit-list ra col-idx)))
(defn emit-iterator [ra] `(.iterator ~(emit-list ra)))

(defn first-or-null [rel]
  (reduce (fn [_ x] (reduced x)) nil rel))

(defn need-rsn? [scan-bindings] (some (comp #{:cinq/rsn} second) scan-bindings))

(defn get-numeric [k o]
  (if (associative? o)
    (get o k)
    (nth o k nil)))

(defn sort-scan-bindings [scan-bindings]
  (-> (group-by first scan-bindings)
      vals
      (->> (map peek)
           (sort-by (juxt (fn [[_ _ pred]]
                            (if (true? pred)
                              1
                              0))
                          first))
           vec)))

(defn emit-scan-binding-expr [k o rsn rv self-class]
  (cond
    (= :cinq/self k) o
    (= :cinq/rsn k) rsn
    (= :cinq/relvar k) rv
    (and self-class (class? self-class) (plan/kw-field self-class k))
    (list (symbol (str ".-" (munge (name k)))) o)
    (keyword? k) (list k o)
    ;; todo should we get a special numeric key here from vec destructures to avoid
    ;; ambiguity between get/nth
    (number? k) `(get-numeric ~k ~o)
    :else (list `get o k)))

(defn scan-reduce-rsn [src ^CinqScanFunction f]
  (let [a (long-array 1)]
    (aset a 0 -1)
    (reduce (fn [acc x] (f acc nil (aset a 0 (unchecked-inc (aget a 0))) x)) nil src)))

(defn run-scan-rsn [f src]
  (if (instance? Scannable src)
    (p/scan src f nil)
    (scan-reduce-rsn src f)))

(defn run-scan-no-rsn [f src]
  (if (instance? Scannable src)
    (p/scan src f nil)
    (reduce (fn [acc x] (f acc nil -1 x)) nil src)))

(defn scan-binding-used? [sym] (not (:not-used (meta sym))))

(defn native-pred-expr [[sym k pred] bound-syms]
  (when (and (keyword? k) (not (#{:cinq/self :cinq/rsn} k)))
    (m/match pred

      (m/and [::plan/= ?a ?b]
             (m/guard (and (bound-syms ?a) (not (bound-syms ?b)))))
      {:test ?b
       :k k
       :cmp :=}

      (m/and [::plan/= ?b ?a]
             (m/guard (and (bound-syms ?a) (not (bound-syms ?b)))))
      {:test ?b
       :k k
       :cmp :=}

      (m/and [::plan/< ?a ?b]
             (m/guard (and (bound-syms ?a) (not (bound-syms ?b)))))
      {:test ?b
       :k k
       :cmp :>}
      (m/and [::plan/< ?b ?a]
             (m/guard (and (bound-syms ?a) (not (bound-syms ?b)))))
      {:test ?b
       :k k
       :cmp :<}

      (m/and [::plan/<= ?a ?b]
             (m/guard (and (bound-syms ?a) (not (bound-syms ?b)))))
      {:test ?b
       :k k
       :cmp :>=}
      (m/and [::plan/<= ?b ?a]
             (m/guard (and (bound-syms ?a) (not (bound-syms ?b)))))
      {:test ?b
       :k k
       :cmp :<=}

      (m/and [::plan/> ?a ?b]
             (m/guard (and (bound-syms ?a) (not (bound-syms ?b)))))
      {:test ?b
       :k k
       :cmp :<}
      (m/and [::plan/> ?b ?a]
             (m/guard (and (bound-syms ?a) (not (bound-syms ?b)))))
      {:test ?b
       :k k
       :cmp :>}

      (m/and [::plan/>= ?a ?b]
             (m/guard (and (bound-syms ?a) (not (bound-syms ?b)))))
      {:test ?b
       :k k
       :cmp :<=}
      (m/and [::plan/>= ?b ?a]
             (m/guard (and (bound-syms ?a) (not (bound-syms ?b)))))
      {:test ?b
       :k k
       :cmp :>=}

      _ nil)))

(defn classify-scan-binding [[sym _ pred :as binding] bound-syms]
  (let [used (scan-binding-used? sym)]
    (cond
      (true? pred) [:bind used]
      (native-pred-expr binding bound-syms) [:filter used]
      :else [:condition used])))

(defn emit-scan [src bindings body]
  (let [bindings (sort-scan-bindings bindings)
        self-binding (last (keep #(when (= :cinq/self (second %)) (first %)) bindings))
        self-not-used (:not-used (meta self-binding))
        self-tag (:tag (meta self-binding))
        self-class (if (symbol? self-tag) (resolve self-tag) self-tag)
        o (if self-tag (with-meta (gensym "o") {:tag self-tag}) (gensym "o"))
        rsn (gensym "rsn")
        rv (gensym "rv")

        bound-syms (set (map first bindings))

        {top-level-bindings [:bind true]
         filter-only [:filter false]
         filter-bind [:filter true]
         cond-only [:condition false]
         cond-bind [:condition true]}
        (group-by #(classify-scan-binding % bound-syms) bindings)

        filter-bindings (concat filter-only filter-bind)
        condition-bindings (concat cond-only cond-bind)

        lambda `(reify
                  IFn
                  ~(if (seq filter-bindings)
                     `(invoke [f# agg# rv# rsn# o#]
                              (if (.filter f# rsn# o#)
                                (.apply f# agg# rv# rsn# o#)
                                agg#))
                     `(invoke [f# agg# rv# rsn# o#] (.apply f# agg# rv# rsn# o#)))

                  CinqScanFunction
                  ;; todo
                  ~(if (seq filter-bindings)
                     `(filter [_# ~rsn o#]
                              (let [~o o#]
                                ~((fn emit-next-pred [bindings]
                                    (if (empty? bindings)
                                      true
                                      (let [[sym k pred] (first bindings)]
                                        `(let [~sym ~(emit-scan-binding-expr k o rsn rv self-class)]
                                           (and ~(rewrite-expr [] pred)
                                                ~(emit-next-pred (rest bindings)))))))
                                  filter-bindings)))
                     `(filter [_# _# _#] true))

                  ~(if (seq filter-bindings)
                     (let [bufsym (memoize gensym)
                           keysym (memoize gensym)
                           valbuf (gensym "valbuf")
                           symbol-table (gensym "symbol-table")
                           symbol-list (gensym "symbol-list")
                           retexpr (fn [i expr]
                                     (if (= 0 i)
                                       expr
                                       `(do (.clear ~valbuf)
                                            ~expr)))]
                       `(nativeFilter
                          [_# ~symbol-table]
                          (let [~symbol-list (codec/symbol-list ~symbol-table)
                                ~@(for [[sym :as binding] filter-bindings
                                        :let [{:keys [k test]} (native-pred-expr binding bound-syms)]
                                        form [(bufsym sym) `(codec/encode-heap ~test ~symbol-table false)
                                              (keysym sym) `(codec/intern-symbol ~symbol-table ~k false)]]
                                    form)]
                            (when
                              ;; if any buf is nil, cannot possibly pass pred
                              (and ~@(for [[sym] filter-bindings form [(bufsym sym) (keysym sym)]] form))
                              (reify CinqScanFunction$NativeFilter
                                (apply [_# rsn# buf#]
                                  (let [~valbuf (.slice buf#)]
                                    (and ~@(for [[i [sym :as binding]] (map-indexed vector filter-bindings)
                                                 :let [{:keys [cmp]} (native-pred-expr binding bound-syms)
                                                       bufcmp-expr `(codec/bufcmp-ksv ~(bufsym sym) ~valbuf ~(keysym sym) ~symbol-list)]]
                                             (retexpr
                                               i
                                               (case cmp
                                                 := `(CinqUtil/eq 0 ~bufcmp-expr)
                                                 :< `(CinqUtil/lt ~bufcmp-expr 0)
                                                 :<= `(CinqUtil/lte ~bufcmp-expr 0)
                                                 :> `(CinqUtil/gt ~bufcmp-expr 0)
                                                 :>= `(CinqUtil/gte ~bufcmp-expr 0))))))))))))
                     `(nativeFilter
                        [_# _#]
                        (reify CinqScanFunction$NativeFilter (apply [_# _rsn# _buf#] true))))

                  (apply [_# _# ~rv ~rsn o#]
                    (when (Thread/interrupted) (throw (InterruptedException.)))
                    (let [~o o#]
                      ;; with pred
                      ~((fn emit-next-binding [bindings]
                          (if (empty? bindings)
                            (if (and (empty? top-level-bindings)
                                     (empty? filter-bindings))
                              `(some-> ~body reduced)
                              `(let [~@(for [[sym k] (concat top-level-bindings filter-bindings)
                                             form [sym (emit-scan-binding-expr k o rsn rv self-class)]]
                                         form)]
                                 (some-> ~body reduced)))
                            (let [[sym k pred] (first bindings)]
                              `(let [~sym ~(emit-scan-binding-expr k o rsn rv self-class)]
                                 (when ~(rewrite-expr [] pred)
                                   ~(emit-next-binding (rest bindings)))))))
                        condition-bindings)))

                  (rootDoesNotEscape [_#] ~(boolean self-not-used)))]
    (if (need-rsn? bindings)
      `(run-scan-rsn ~lambda ~(rewrite-expr [] src))
      `(run-scan-no-rsn ~lambda ~(rewrite-expr [] src)))))

(defn emit-where [ra pred body]
  (emit-loop ra `(when ~(rewrite-expr [ra] pred) ~body)))

(defn emit-rel
  ([ra] (emit-rel ra false))
  ([ra tuple]
   (let [box (gensym "box")
         rsn (gensym "rsn")
         f (gensym "f")
         cols (plan/columns ra)
         col (nth cols 0 nil)]
     `(reify p/Scannable
        (scan [this# f# init#]
          (let [~rsn (long-array 1)]
            (aset ~rsn 0 -1)
            (.reduce this#
                     (fn [acc# x#]
                       (f# acc# nil (aset ~rsn 0 (unchecked-inc (aget ~rsn 0))) x#)) init#)))
        IReduceInit
        (reduce [_ ~f init#]
          (let [~box (object-array 1)]
            (aset ~box 0 init#)
            ((fn [~(with-meta box {:tag 'objects})]
               ~(emit-loop ra `(let [r# (~f (aget ~box 0) ~(if tuple (t/emit-tuple ra) `(clojure.lang.RT/box ~col)))]
                                 (if (reduced? r#)
                                   (aset ~box 0 @r#)
                                   (do (aset ~box 0 r#) nil)))))
             ~box)
            (aget ~box 0)))))))

(defn emit-cross-join [left right body]
  (emit-loop left (emit-loop right body)))

(defn build-side-function [ra key-expr]
  (let [ht (gensym "ht")]
    `(fn build-side# []
       (let [~ht (CinqMultimap.)]
         ~(emit-loop
            ra
            `(let [t# ~(t/emit-tuple ra)]
               (.put ~ht ~(t/emit-key (rewrite-expr [ra] key-expr)) t#)))
         ~ht))))

(defn emit-join-theta [theta body]
  (case (count theta)
    0 body
    1 `(when ~(first theta) ~body)
    `(when (and ~@theta) ~body)))

(defn add-theta-where [ra theta-expressions]
  (if (seq theta-expressions)
    [::plan/where ra (into [::plan/and] theta-expressions)]
    ra))

(defn emit-apply-left-join [left right theta body]
  (let [left-t (t/tuple-local left)
        left-cols (plan/columns left)
        right-cols (plan/columns right)
        cont-lambda (gensym "cont-lambda")
        theta-expressions (rewrite-expr [left right] theta)
        right-t (t/tuple-local right)]
    `(let [~cont-lambda
           (fn [~left-t ~right-t]
             (let [~@(t/emit-tuple-column-binding left-t left-cols (vec (remove (set right-cols) left-cols)))
                   ~@(t/emit-optional-column-binding right-t right-cols)]
               ~body))]
       ~(emit-loop
          left
          `(let [~left-t ~(t/emit-tuple left)]
             (or ~(emit-loop
                    (add-theta-where right theta-expressions)
                    `(do (t/set-mark ~left-t)
                         (~cont-lambda ~left-t ~(t/emit-tuple right))))
                 (when-not (t/get-mark ~left-t)
                   (~cont-lambda ~left-t nil))))))))

(defn emit-const-single-join [left right body]
  (let [right-t (t/tuple-local right)
        right-cols (plan/columns right)]
    `(let [~right-t (first-or-null ~(emit-rel right true))]
       ~(emit-loop
          left
          `(let [~@(t/emit-optional-column-binding right-t right-cols)]
             ~body)))))

(defn emit-apply-single-join [left right theta body]
  (let [left-t (t/tuple-local left)
        left-cols (plan/columns left)
        right-cols (plan/columns right)
        cont-lambda (gensym "cont-lambda")
        theta-expressions (rewrite-expr [left right] theta)
        right-t (t/tuple-local right)
        limit-sym (gensym "limit")]
    `(let [~cont-lambda
           (fn [~left-t ~right-t]
             (let [~@(t/emit-tuple-column-binding left-t left-cols (vec (remove (set right-cols) left-cols)))
                   ~@(t/emit-optional-column-binding right-t right-cols)]
               ~body))]
       ~(emit-loop
          left
          `(let [~left-t ~(t/emit-tuple left)
                 ~limit-sym (int-array 1)
                 stop# ~(emit-loop
                          [::plan/limit (add-theta-where right theta-expressions) 1 limit-sym]
                          `(do (t/set-mark ~left-t)
                               (~cont-lambda ~left-t ~(t/emit-tuple right))))]
             (cond
               (not (identical? stop# ~limit-sym)) stop#
               (not (t/get-mark ~left-t)) (~cont-lambda ~left-t nil)
               :else nil))))))

(defn emit-apply-semi-join [left right theta body]
  (let [theta-expressions (rewrite-expr [left right] theta)
        limit (gensym "limit")]
    (emit-loop
      left
      `(let [~limit (int-array 1)
             stop# ~(emit-loop [::plan/limit (add-theta-where right theta-expressions) 1 limit] body)]
         (when-not (identical? stop# ~limit)
           stop#)))))

(defn emit-apply-anti-join [left right theta body]
  (let [theta-expressions (rewrite-expr [left right] theta)
        limit (gensym "limit")]
    (emit-loop
      left
      `(let [~limit (int-array 1)
             stop# ~(emit-loop [::plan/limit (add-theta-where right theta-expressions) 1 limit] ::matched)]
         (cond
           (identical? stop# ~limit) nil
           (identical? stop# ::matched) nil
           stop# stop#
           :else ~body)))))

(defn emit-equi-join [left right left-key-expr right-key-expr theta body]
  (let [al (with-meta (gensym "al") {:tag `ArrayList})
        ht (with-meta (gensym "ht") {:tag `CinqMultimap})
        t (t/tuple-local left)
        left-cols (plan/columns left)
        right-cols (plan/columns right)]
    `(let [build-fn# ~(build-side-function left left-key-expr)
           ~ht (build-fn#)]
       ~(emit-loop
          right
          `(when-some [~al (.get ~ht ~(t/emit-key (rewrite-expr [right] right-key-expr)))]
             (loop [i# 0]
               (when (< i# (.size ~al))
                 (let [~t (.get ~al i#)
                       ~@(t/emit-tuple-column-binding
                           t
                           left-cols
                           ;; right shadows left if there is a collision
                           ;; todo planner should ensure this cannot happen by auto gensym
                           (set/difference (set left-cols)
                                           (set right-cols)))]
                   (or ~(emit-join-theta (rewrite-expr [left right] theta) body)
                       (recur (unchecked-inc i#)))))))))))

(defn emit-equi-left-join [left right left-key-expr right-key-expr theta body]
  (let [al (with-meta (gensym "al") {:tag `ArrayList})
        ht (with-meta (gensym "ht") {:tag `CinqMultimap})
        left-t (t/tuple-local left)
        left-cols (plan/columns left)
        right-cols (plan/columns right)
        cont-lambda (gensym "cont-lambda")
        theta-expressions (rewrite-expr [left right] theta)
        right-t (t/tuple-local right)]
    `(let [build-fn# ~(build-side-function left left-key-expr)
           ~ht (build-fn#)
           ~cont-lambda
           (fn [~left-t ~right-t]
             (let [~@(t/emit-tuple-column-binding left-t left-cols (vec (remove (set right-cols) left-cols)))
                   ~@(t/emit-optional-column-binding right-t right-cols)]
               ~body))]
       (or ~(emit-loop
              right
              `(when-some [~al (.get ~ht ~(t/emit-key (rewrite-expr [right] right-key-expr)))]
                 (loop [i# 0]
                   (when (< i# (.size ~al))
                     (let [~left-t (.get ~al i#)
                           ~@(t/emit-tuple-column-binding
                               left-t
                               left-cols
                               ;; right shadows left if there is a collision
                               ;; todo planner should ensure this cannot happen by auto gensym
                               (set/difference (set left-cols)
                                               (set right-cols)))]
                       (or ~(emit-join-theta theta-expressions `(do ~(t/set-mark left-t) (~cont-lambda ~left-t ~(t/emit-tuple right))))
                           (recur (unchecked-inc i#))))))))
           (.forEach ~ht (reify Function
                           (apply [_# t#]
                             (let [~left-t t#]
                               (when-not ~(t/get-mark left-t)
                                 (~cont-lambda ~left-t nil))))))))))

(defn emit-equi-single-join [left right left-key-expr right-key-expr theta body]
  (let [al (with-meta (gensym "al") {:tag `ArrayList})
        ht (with-meta (gensym "ht") {:tag `CinqMultimap})
        left-t (t/tuple-local left)
        right-t (t/tuple-local right)
        left-cols (plan/columns left)
        right-cols (plan/columns right)
        cont-lambda (gensym "cont-lambda")
        theta-expressions (rewrite-expr [left right] theta)
        i (gensym "i")]
    `(let [build-fn# ~(build-side-function left left-key-expr)
           ~ht (build-fn#)
           ~cont-lambda
           (fn [~left-t ~right-t]
             ~(t/set-mark left-t)
             (let [~@(t/emit-tuple-column-binding left-t left-cols (vec (remove (set right-cols) left-cols)))
                   ~@(t/emit-optional-column-binding right-t right-cols)]
               ~body))]
       (or ~(emit-loop
              right
              `(when-some [~al (.get ~ht ~(t/emit-key (rewrite-expr [right] right-key-expr)))]
                 (loop [~i 0]
                   (when (< ~i (.size ~al))
                     (let [~left-t (.get ~al ~i)]
                       (if ~(t/get-mark left-t)
                         (recur (unchecked-inc ~i))
                         ~(case (count theta-expressions)
                            0 `(or (~cont-lambda ~left-t ~(t/emit-tuple right))
                                   (recur (unchecked-inc ~i)))
                            `(let [~@(t/emit-tuple-column-binding left-t left-cols theta (set right-cols))]
                               (if (and ~@theta-expressions)
                                 `(or (~cont-lambda ~left-t ~(t/emit-tuple right))
                                      (recur (unchecked-inc ~i)))
                                 (recur (unchecked-inc ~i)))))))))))
           (.forEach ~ht (reify Function
                           (apply [_# t#]
                             (let [~left-t t#]
                               (when-not ~(t/get-mark left-t)
                                 (~cont-lambda ~left-t nil))))))))))

(defn emit-equi-semi-join [left right left-key-expr right-key-expr theta body]
  (let [al (with-meta (gensym "al") {:tag `ArrayList})
        ht (with-meta (gensym "ht") {:tag `CinqMultimap})
        left-t (t/tuple-local left)
        left-cols (plan/columns left)
        right-cols (plan/columns right)
        theta-expressions (rewrite-expr [left right] theta)]
    `(let [build-fn# ~(build-side-function left left-key-expr)
           ~ht (build-fn#)]
       ~(emit-loop
          right
          `(when-some [~al (.get ~ht ~(t/emit-key (rewrite-expr [right] right-key-expr)))]
             (loop [i# 0]
               (when (< i# (.size ~al))
                 (let [~left-t (.get ~al i#)]
                   (if ~(t/get-mark left-t)
                     (recur (unchecked-inc i#))
                     (let [~@(t/emit-tuple-column-binding
                               left-t
                               left-cols
                               ;; right shadows left if there is a collision
                               ;; todo planner should ensure this cannot happen by auto gensym
                               (set/difference (set left-cols)
                                               (set right-cols)))]
                       (or ~(emit-join-theta theta-expressions `(do ~(t/set-mark left-t) ~body))
                           (recur (unchecked-inc i#)))))))))))))

(defn emit-equi-anti-join [left right left-key-expr right-key-expr theta body]
  (let [al (with-meta (gensym "al") {:tag `ArrayList})
        ht (with-meta (gensym "ht") {:tag `CinqMultimap})
        left-t (t/tuple-local left)
        left-cols (plan/columns left)
        right-cols (plan/columns right)
        theta-expressions (rewrite-expr [left right] theta)]
    `(let [build-fn# ~(build-side-function left left-key-expr)
           ~ht (build-fn#)]
       ~(emit-loop
          right
          `(when-some [~al (.get ~ht ~(t/emit-key (rewrite-expr [right] right-key-expr)))]
             (loop [i# 0]
               (when (< i# (.size ~al))
                 (let [~left-t (.get ~al i#)]
                   (if ~(t/get-mark left-t)
                     (recur (unchecked-inc i#))
                     (let [~@(t/emit-tuple-column-binding
                               left-t
                               left-cols
                               ;; right shadows left if there is a collision
                               ;; todo planner should ensure this cannot happen by auto gensym
                               (set/difference (set left-cols)
                                               (set right-cols)))]
                       ~(emit-join-theta theta-expressions (t/set-mark left-t))
                       (recur (unchecked-inc i#)))))))))
       (.forEach ~ht (reify Function
                       (apply [_# t#]
                         (let [~left-t t#]
                           (when-not ~(t/get-mark left-t)
                             (let [~@(t/emit-tuple-column-binding left-t left-cols)]
                               ~body)))))))))

(defn emit-distinct [ra exprs body]
  (let [ht (with-meta (gensym "ht") {:tag `HashMap})
        t (t/tuple-local ra)
        build-side-fn `(fn []
                         (let [~ht (HashMap.)]
                           ~(emit-loop ra `(do (.putIfAbsent ~ht ~(t/emit-key (rewrite-expr [ra] exprs)) ~(t/emit-tuple ra)) nil))
                           ~ht))
        left-cols (plan/columns ra)]
    `(let [~ht (~build-side-fn)
           ret# (reduce
                  (fn [_# ~t]
                    (let [~@(t/emit-tuple-column-binding t left-cols)
                          r# ~body]
                      (when (some? r#)
                        (reduced r#))))
                  nil
                  (vals ~ht))]
       ret#)))

(defn emit-column [al o col i]
  (let [[col-ctor a-ctor prim-conv]
        (condp = (:tag (meta col))
          'double [`col/->DoubleColumn `double-array `double]
          'long [`col/->LongColumn `long-array `long]
          [`col/->Column `object-array 'clojure.lang.RT/box])]
    `(~col-ctor
       nil
       (fn []
         (let [arr# (~a-ctor (.size ~al))]
           (dotimes [j# (.size ~al)]
             (let [~o (.get ~al j#)]
               (aset arr# j# (~prim-conv ~(t/get-field o i)))))
           arr#)))))

(defn emit-group-all [ra body]
  (let [o (t/tuple-local ra)
        al (with-meta (gensym "al") {:tag `ArrayList})]
    `(let [~al ~(emit-list ra)
           ~@(for [[i col] (map-indexed vector (plan/columns ra))
                   form
                   [(with-meta col {})
                    (emit-column al o col i)]]
               form)
           ~(with-meta plan/%count-sym {}) (.size ~al)]
       ~body)))

(defn emit-group-by [ra bindings body]
  (let [o (t/tuple-local ra)
        cols (plan/columns ra)
        k (t/key-local (mapv second bindings))
        al (with-meta (gensym "al") {:tag `ArrayList})]
    `(let [rs# ~(emit-list ra)
           ht# (HashMap. 64)]
       ;; agg
       (run!
         (fn [~o]
           (let [~@(t/emit-tuple-column-binding o cols (mapv second bindings))
                 ~k ~(t/emit-key (rewrite-expr [ra] (map second bindings)))
                 f# (reify BiFunction
                      (apply [_# _# ~(with-meta al {})]
                        (let [~al (or ~al (ArrayList.))]
                          (.add ~al ~o)
                          ~al)))]
             (.compute ht# ~k f#)))
         rs#)
       ;; emit results
       (reduce
         (fn [_# [~k ~al]]
           (let [~@(for [[i col] (map-indexed vector (plan/columns ra))
                         form
                         [(with-meta col {})
                          (emit-column al o col i)]]
                     form)
                 ~@(t/emit-key-bindings k (map first bindings))
                 ~(with-meta plan/%count-sym {}) (.size ~al)]
             (some-> ~body reduced)))
         nil
         ht#))))

(defn- assign-agg-binding-indexes [agg-bindings]
  (let [acc-index (volatile! -1)]
    (for [v agg-bindings]
      (update v 1 (partial mapv #(into [(vswap! acc-index inc)] %))))))

(defn emit-group-let-all [ra agg-bindings new-projection body]
  (let [arr (gensym "arr")
        agg-bindings (assign-agg-binding-indexes agg-bindings)
        acc-count (inc (reduce + 0 (map (fn [[_ agg]] (count agg)) agg-bindings)))
        cnt-index (dec acc-count)
        acc-bindings (for [[_ agg] agg-bindings
                           [i sym _] agg
                           form [sym `(aget ~arr ~i)]]
                       form)]
    `(let [~arr (object-array ~acc-count)]
       ~@(for [[_ agg] agg-bindings
               [i _ init _] agg]
           `(aset ~arr ~i (RT/box ~init)))
       (aset ~arr ~cnt-index (RT/box 0))
       ~(emit-loop ra `(let [~@acc-bindings]
                         ~@(for [[_ agg] agg-bindings
                                 [i _ _ expr] agg]
                             `(aset ~arr ~i (RT/box ~(rewrite-expr [ra] expr))))
                         (aset ~arr ~cnt-index (unchecked-inc (aget ~arr ~cnt-index)))
                         nil))
       (let [~@acc-bindings
             ~plan/%count-sym (aget ~arr ~cnt-index)
             ~@(for [[sym _ completion] agg-bindings
                     form [sym (rewrite-expr [] completion)]]
                 form)
             ~@(for [[sym expr] new-projection
                     form [sym (rewrite-expr [] expr)]]
                 form)]
         ~body))))

(defn emit-group-let [ra bindings agg-bindings new-projection body]
  (if (empty? bindings)
    (emit-group-let-all ra agg-bindings new-projection body)
    ;; group bindings
    (let [k (t/key-local (mapv second bindings))
          arr (with-meta (gensym "arr") {:tag 'objects})
          ht (gensym "ht")
          agg-bindings (assign-agg-binding-indexes agg-bindings)
          acc-count (inc (reduce + 0 (map (fn [[_ agg]] (count agg)) agg-bindings)))
          cnt-index (dec acc-count)
          acc-bindings (for [[_ agg] agg-bindings
                             [i sym _] agg
                             form [sym `(aget ~arr ~i)]]
                         form)]
      `(let [~ht (HashMap.)]
         ~(emit-loop
            ra
            `(let [~k ~(t/emit-key (rewrite-expr [ra] (map second bindings)))
                   f# (reify BiFunction
                        (apply [_# _# arr#]
                          (let [~arr (or arr# (doto (object-array ~acc-count)
                                                ~@(for [[_ agg] agg-bindings
                                                        [i _ init] agg]
                                                    `(aset ~i (RT/box ~(rewrite-expr [] init))))
                                                (aset ~cnt-index (RT/box 0))))
                                ~@acc-bindings]
                            ~@(for [[_ agg] agg-bindings
                                    [i _ _ expr] agg]
                              `(aset ~arr ~i (RT/box ~(rewrite-expr [ra] expr))))
                            (aset ~arr ~cnt-index (unchecked-inc (aget ~arr ~cnt-index)))
                            ~arr)))]
               (.compute ~ht ~k f#)
               nil))
         (reduce
           (fn [_# [~k ~arr]]
             (let [~@(t/emit-key-bindings k (map first bindings))
                   ~@acc-bindings
                   ~plan/%count-sym (aget ~arr ~cnt-index)
                   ~@(for [[sym _ completion] agg-bindings
                           form [sym (rewrite-expr [] completion)]]
                       form)
                   ~@(for [[sym expr] new-projection
                           form [sym (rewrite-expr [] expr)]]
                       form)]
               (some-> ~body reduced)))
           nil
           ~ht)))))

(defn emit-order-by [ra order-clauses body]
  (let [o (t/tuple-local ra)
        a (t/tuple-local ra)
        b (t/tuple-local ra)
        cols (plan/columns ra)
        comparator `(reify Comparator
                      (compare [_# a# b#]
                        (let [~a a#
                              ~b b#]
                          ~((fn ! [[[expr dir] & more]]
                              `(let [res# (compare (let ~(t/emit-tuple-column-binding a cols expr) ~(rewrite-expr [ra] expr))
                                                   (let ~(t/emit-tuple-column-binding b cols expr) ~(rewrite-expr [ra] expr)))]
                                 (if (= 0 res#)
                                   ~(if (seq more) (! more) 0)
                                   (* res# ~(if (= :desc dir) -1 1)))))
                            order-clauses))))]
    `(let [rs# ~(emit-array ra)]
       (java.util.Arrays/sort rs# ~comparator)
       (loop [i# 0]
         (when (< i# (alength rs#))
           (let [~o (aget rs# i#)
                 ~@(t/emit-tuple-column-binding o cols)]
             (or ~body (recur (unchecked-inc i#)))))))))

(defn emit-limit [ra n box-expr body]
  (let [ctr (gensym "ctr")]
    `(let [~ctr ~box-expr]
       ~(emit-loop ra `(if (< (aget ~ctr 0) ~n)
                         (do (aset ~ctr 0 (unchecked-inc (aget ~ctr 0))) ~body)
                         ~ctr)))))

(defn emit-cte [bindings expr body]
  (let [variables (distinct (map (fn [[sym]] sym) bindings))
        union-sym (memoize (fn [s] (with-meta (gensym (str "union-" s)) {:tag `ArrayList})))
        new-sym (memoize (fn [s] (with-meta (gensym (str "new-" s)) {:tag `ArrayList})))]
    `(let [~@(for [sym variables
                   form [(union-sym sym) `(ArrayList.)]]
               form)]
       ~((fn ! [[[sym ra] & bindings :as xs]]
           (if (empty? xs)
             nil
             (if-not (seq (expr/possible-dependencies [sym] ra))
               `(do ~(emit-loop ra `(do (.add ~(union-sym sym) ~(first (plan/columns ra))) nil))
                    (let [~sym ~(union-sym sym)]
                      ~(! bindings)))
               `(loop [~sym ~sym
                       ~(new-sym sym) (ArrayList.)]
                  ~(emit-loop ra `(do (.add ~(new-sym sym) ~(first (plan/columns ra))) nil))
                  (if (= 0 (.size ~(new-sym sym)))
                    (let [~sym ~(union-sym sym)]
                      ~(! bindings))
                    (do (.addAll ~(union-sym sym) ~(new-sym sym))
                        (recur ~(new-sym sym) (ArrayList.))))))))
         bindings)
       (let [~@(for [sym variables form [sym (union-sym sym)]] form)]
         ~(emit-loop expr body)))))

(defn emit-union [ras body]
  (let [cont-lambda (gensym "cont")]
    `(let [~cont-lambda (fn [~plan/union-out-col] ~body)]
       (or ~@(for [ra ras] (emit-loop ra `(~cont-lambda ~(first (plan/columns ra)))))))))

(defn emit-loop
  [ra body]
  (m/match ra
    [::plan/scan ?src ?bindings]
    (emit-scan ?src ?bindings body)

    [::plan/where ?ra ?pred]
    (emit-where ?ra ?pred body)

    ;; joins
    [::plan/cross-join ?left ?right]
    (emit-cross-join ?left ?right body)

    [::plan/apply :cross-join ?left ?right]
    (emit-cross-join ?left ?right body)

    [::plan/apply :left-join ?left ?right]
    (emit-apply-left-join ?left ?right [] body)

    [::plan/apply :single-join ?left ?right]
    (emit-apply-single-join ?left ?right [] body)

    [::plan/join ?left ?right ?pred]
    (if (plan/equi-join? ?left ?right ?pred)
      (let [{:keys [left-key right-key theta]} (plan/equi-theta ?left ?right ?pred)]
        (emit-equi-join ?left ?right left-key right-key theta body))
      (emit-loop [::plan/apply :cross-join ?left [::plan/where ?right ?pred]] body))

    [::plan/left-join ?left ?right ?pred]
    (if (plan/equi-join? ?left ?right ?pred)
      (let [{:keys [left-key right-key theta]} (plan/equi-theta ?left ?right ?pred)]
        (emit-equi-left-join ?left ?right left-key right-key theta body))
      (emit-apply-left-join ?left ?right [?pred] body))

    [::plan/single-join ?left ?right ?pred]
    (if (plan/equi-join? ?left ?right ?pred)
      (let [{:keys [left-key right-key theta]} (plan/equi-theta ?left ?right ?pred)]
        (emit-equi-single-join ?left ?right left-key right-key theta body))
      (if (= true ?pred)
        (emit-const-single-join ?left ?right body)
        (emit-apply-single-join ?left ?right [?pred] body)))

    [::plan/semi-join ?left ?right ?pred]
    (if (plan/equi-join? ?left ?right ?pred)
      (let [{:keys [left-key right-key theta]} (plan/equi-theta ?left ?right ?pred)]
        (emit-equi-semi-join ?left ?right left-key right-key theta body))
      (emit-apply-semi-join ?left ?right [?pred] body))

    [::plan/anti-join ?left ?right ?pred]
    (if (plan/equi-join? ?left ?right ?pred)
      (let [{:keys [left-key right-key theta]} (plan/equi-theta ?left ?right ?pred)]
        (emit-equi-anti-join ?left ?right left-key right-key theta body))
      (emit-apply-anti-join ?left ?right [?pred] body))

    [::plan/let ?ra ?bindings]
    (emit-loop ?ra `(let [~@(for [[col expr] ?bindings
                                  form [(with-meta col {}) (rewrite-expr [?ra] expr)]]
                              form)]
                      ~body))

    [::plan/project ?ra ?bindings]
    (emit-loop [::plan/let ?ra ?bindings] body)

    [::plan/group-by ?ra ?bindings]
    (if (empty? ?bindings)
      (emit-group-all ?ra body)
      (emit-group-by ?ra ?bindings body))

    [::plan/group-let ?ra ?bindings ?aggs ?new-projection]
    (emit-group-let ?ra ?bindings ?aggs ?new-projection body)

    [::plan/order-by ?ra ?order-clauses]
    (emit-order-by ?ra ?order-clauses body)

    [::plan/limit ?ra ?n ?box]
    (emit-limit ?ra ?n ?box body)

    [::plan/without ?ra _]
    (emit-loop ?ra body)

    [::plan/cte ?bindings ?ra]
    (emit-cte ?bindings ?ra body)

    [::plan/union ?ras]
    (emit-union ?ras body)

    [::plan/distinct ?ra ?exprs]
    (emit-distinct ?ra ?exprs body)

    _
    (do
      (clojure.pprint/pprint (plan/stack-view ra))
      (throw (ex-info (format "Unknown plan %s" (first ra)) {:ra ra})))))

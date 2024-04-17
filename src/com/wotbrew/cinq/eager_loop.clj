(ns com.wotbrew.cinq.eager-loop
  (:require [clojure.set :as set]
            [com.wotbrew.cinq.expr :as expr]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.column :as col]
            [com.wotbrew.cinq.tuple :as t]
            [meander.epsilon :as m])
  (:import (java.util ArrayList Comparator HashMap Iterator)
           (java.util.function BiConsumer BiFunction)))

(set! *warn-on-reflection* true)

(declare emit-loop emit-array emit-iterator emit-iterable)

(defn rewrite-expr [dependent-relations clj-expr]
  (let [col-maps (mapv #(plan/dependent-cols % clj-expr) dependent-relations)]
    (expr/rewrite col-maps clj-expr #(emit-iterable % 0))))

(defn emit-list
  ([ra]
   (let [list-sym (gensym "list")]
     `(let [~list-sym (ArrayList.)]
        ~(emit-loop ra `(.add ~list-sym ~(t/emit-tuple ra)))
        ~list-sym)))
  ([ra col-idx]
   (let [list-sym (gensym "list")
         cols (plan/columns ra)
         col (nth cols col-idx)]
     `(let [~list-sym (ArrayList.)]
        ~(emit-loop ra `(.add ~list-sym (clojure.lang.RT/box ~col)))
        ~list-sym))))

(defn emit-array [ra] `(.toArray ~(emit-list ra)))

;; todo use row-major.clj for the below, both namespaces should be symbiotic
(defn emit-iterable ([ra] (emit-list ra)) ([ra col-idx] (emit-list ra col-idx)))
(defn emit-iterator [ra] `(.iterator ~(emit-list ra)))

(defn emit-first [ra]
  `(let [iter# ~(emit-iterator ra)]
     (when (.hasNext iter#)
       (.next iter#))))

(defn emit-first-or-null [ra]
  `(let [iter# ~(emit-iterator ra)]
     (when (.hasNext iter#)
       (.next iter#))))

(defn emit-scan [src bindings body]
  (let [self-binding (last (keep #(when (= :cinq/self (second %)) (first %)) bindings))
        self-tag (:tag (meta self-binding))
        self-class (if (symbol? self-tag) (resolve self-tag) self-tag)
        o (if self-tag (with-meta (gensym "o") {:tag self-tag}) (gensym "o"))
        lambda `(fn scan-fn# [_# ~o]
                  (let [~@(for [[sym k] bindings
                                form [(with-meta sym {})
                                      (cond
                                        (= :cinq/self k) o
                                        (and self-class (class? self-class) (plan/kw-field self-class k))
                                        (list (symbol (str ".-" (munge (name k)))) o)
                                        (keyword? k) (list k o)
                                        :else (list `get o k))]]
                            form)]
                    ~body))]
    `(reduce ~lambda nil ~(rewrite-expr [] src))))

(defn emit-where [ra pred body]
  (emit-loop ra `(when ~(rewrite-expr [ra] pred) ~body)))

(defn emit-cross-join [left right body]
  (emit-loop left (emit-loop right body)))

(defn build-side-function [ra key-expr]
  (let [ht (gensym "ht")
        al (with-meta (gensym "al") {:tag `ArrayList})]
    `(fn build-side# []
       (let [~ht (HashMap.)]
         ~(emit-loop
            ra
            `(let [t# ~(t/emit-tuple ra)]
               (.compute ~ht
                         ~(t/emit-key (rewrite-expr [ra] key-expr))
                         (reify BiFunction
                           (apply [_ _ al#]
                             (let [~al (or al# (ArrayList.))]
                               (.add ~al t#)
                               ~al))))))
         ~ht))))

(defn emit-join-theta [theta body]
  (case (count theta)
    0 body
    1 `(when ~(first theta) ~body)
    `(when (and ~@theta) ~body)))

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
          `(let [~left-t ~(t/emit-tuple left)
                 iter# (emit-iterator ~right)]
             (loop [matched# false]
               (if (.hasNext iter#)
                 (let [~right-t (.next iter#)]
                   ~(case (count theta-expressions)
                      0 `(do (~cont-lambda ~left-t ~right-t) (recur true))
                      `(let [~@(t/emit-tuple-column-binding right-t right-cols theta)]
                         (if ~(and ~@theta-expressions)
                           (do (~cont-lambda ~left-t ~right-t) (recur true))
                           (recur matched#)))))
                 (when-not matched#
                   (~cont-lambda ~left-t nil)))))))))

(defn emit-const-single-join [left right body]
  (let [right-t (t/tuple-local right)
        right-cols (plan/columns right)]
    `(let [~right-t ~(emit-first-or-null right)]
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
        right-t (t/tuple-local right)]
    `(let [~cont-lambda
           (fn [~left-t ~right-t]
             (let [~@(t/emit-tuple-column-binding left-t left-cols (vec (remove (set right-cols) left-cols)))
                   ~@(t/emit-optional-column-binding right-t right-cols)]
               ~body))]
       ~(emit-loop
          left
          `(let [~left-t ~(t/emit-tuple left)
                 iter# ~(emit-iterator right)]
             (loop []
               (if (.hasNext iter#)
                 (let [~right-t (.next iter#)]
                   ~(case (count theta-expressions)
                      0 `(~cont-lambda ~left-t ~right-t)
                      `(let [~@(t/emit-tuple-column-binding right-t right-cols theta)]
                         (if (and ~@theta-expressions)
                           (~cont-lambda ~left-t ~right-t)
                           (recur)))))
                 (~cont-lambda ~left-t nil))))))))

(defn emit-apply-semi-join [left right theta body]
  (let [right-t (t/tuple-local right)
        right-cols (plan/columns right)
        theta-expressions (rewrite-expr [left right] theta)
        iter (with-meta (gensym "iter") {:tag `Iterator})]
    (emit-loop
      left
      `(let [~iter ~(emit-iterator right)]
         (loop []
           (when (.hasNext ~iter)
             ~(case (count theta-expressions)
                0 body
                `(if (let [~right-t (.next ~iter)
                           ~@(t/emit-tuple-column-binding right-t right-cols theta)]
                       (and ~@theta-expressions))
                   ~body
                   (recur)))))))))

(defn emit-apply-anti-join [left right theta body]
  (let [right-t (t/tuple-local right)
        right-cols (plan/columns right)
        theta-expressions (rewrite-expr [left right] theta)
        iter (with-meta (gensym "iter") {:tag `Iterator})]
    (emit-loop
      left
      `(let [~iter ~(emit-iterator right)]
         (loop []
           (if-not (.hasNext ~iter)
             ~body
             ~(case (count theta-expressions)
                0 nil
                `(if (let [~right-t (.next ~iter)
                           ~@(t/emit-tuple-column-binding right-t right-cols theta)]
                       (and ~@theta-expressions))
                   nil
                   (recur)))))))))

(defn emit-equi-join [left right left-key-expr right-key-expr theta body]
  (let [al (with-meta (gensym "al") {:tag `ArrayList})
        ht (with-meta (gensym "ht") {:tag `HashMap})
        t (t/tuple-local left)
        left-cols (plan/columns left)
        right-cols (plan/columns right)]
    `(let [build-fn# ~(build-side-function left left-key-expr)
           ~ht (build-fn#)]
       ~(emit-loop
          right
          `(when-some [~al (.get ~ht ~(t/emit-key (rewrite-expr [right] right-key-expr)))]
             (dotimes [i# (.size ~al)]
               (let [~t (.get ~al i#)
                     ~@(t/emit-tuple-column-binding
                         t
                         left-cols
                         ;; right shadows left if there is a collision
                         ;; todo planner should ensure this cannot happen by auto gensym
                         (set/difference (set left-cols)
                                         (set right-cols)))]
                 ~(emit-join-theta (rewrite-expr [left right] theta) body))))))))

(defn emit-equi-left-join [left right left-key-expr right-key-expr theta body]
  (let [al (with-meta (gensym "al") {:tag `ArrayList})
        ht (with-meta (gensym "ht") {:tag `HashMap})
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
       ~(emit-loop
          right
          `(when-some [~al (.get ~ht ~(t/emit-key (rewrite-expr [right] right-key-expr)))]
             (dotimes [i# (.size ~al)]
               (let [~left-t (.get ~al i#)
                     ~@(t/emit-tuple-column-binding
                         left-t
                         left-cols
                         ;; right shadows left if there is a collision
                         ;; todo planner should ensure this cannot happen by auto gensym
                         (set/difference (set left-cols)
                                         (set right-cols)))]
                 ~(emit-join-theta theta-expressions `(do ~(t/set-mark left-t) (~cont-lambda ~left-t ~(t/emit-tuple right))))))))
       (.forEach ~ht (reify BiConsumer
                       (accept [_# _# al#]
                         (let [~al al#]
                           (dotimes [i# (.size ~al)]
                             (let [~left-t (.get ~al i#)]
                               (when-not ~(t/get-mark left-t)
                                 (~cont-lambda ~left-t nil)))))))))))

(defn emit-equi-single-join [left right left-key-expr right-key-expr theta body]
  (let [al (with-meta (gensym "al") {:tag `ArrayList})
        ht (with-meta (gensym "ht") {:tag `HashMap})
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
       ~(emit-loop
          right
          `(when-some [~al (.get ~ht ~(t/emit-key (rewrite-expr [right] right-key-expr)))]
             (loop [~i 0]
               (when (< ~i (.size ~al))
                 ~(case (count theta-expressions)
                    0 `(~cont-lambda (.get ~al ~i) ~(t/emit-tuple right))
                    `(let [~left-t (.get ~al ~i)
                           ~@(t/emit-tuple-column-binding left-t left-cols theta (set right-cols))]
                       (if (and ~@theta-expressions)
                         (~cont-lambda ~left-t ~(t/emit-tuple right))
                         (recur (unchecked-inc-int ~i)))))))
             (~cont-lambda ~left-t nil)))
       (.forEach ~ht (reify BiConsumer
                       (accept [_# _# al#]
                         (let [~al al#]
                           (dotimes [i# (.size ~al)]
                             (let [~left-t (.get ~al i#)]
                               (when-not ~(t/get-mark left-t)
                                 (~cont-lambda ~left-t nil)))))))))))

(defn emit-equi-semi-join [left right left-key-expr right-key-expr theta body]
  (let [al (with-meta (gensym "al") {:tag `ArrayList})
        ht (with-meta (gensym "ht") {:tag `HashMap})
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
       ~(emit-loop
          right
          `(when-some [~al (.get ~ht ~(t/emit-key (rewrite-expr [right] right-key-expr)))]
             (dotimes [i# (.size ~al)]
               (let [~left-t (.get ~al i#)]
                 (when-not ~(t/get-mark left-t)
                   (let [~@(t/emit-tuple-column-binding
                             left-t
                             left-cols
                             ;; right shadows left if there is a collision
                             ;; todo planner should ensure this cannot happen by auto gensym
                             (set/difference (set left-cols)
                                             (set right-cols)))]
                     ~(emit-join-theta theta-expressions (t/set-mark left-t))))))))
       (.forEach ~ht (reify BiConsumer
                       (accept [_# _# al#]
                         (let [~al al#]
                           (dotimes [i# (.size ~al)]
                             (let [~left-t (.get ~al i#)]
                               (when ~(t/get-mark left-t)
                                 (~cont-lambda ~left-t nil)))))))))))

(defn emit-equi-anti-join [left right left-key-expr right-key-expr theta body]
  (let [al (with-meta (gensym "al") {:tag `ArrayList})
        ht (with-meta (gensym "ht") {:tag `HashMap})
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
       ~(emit-loop
          right
          `(when-some [~al (.get ~ht ~(t/emit-key (rewrite-expr [right] right-key-expr)))]
             (dotimes [i# (.size ~al)]
               (let [~left-t (.get ~al i#)]
                 (when-not ~(t/get-mark left-t)
                   (let [~@(t/emit-tuple-column-binding
                             left-t
                             left-cols
                             ;; right shadows left if there is a collision
                             ;; todo planner should ensure this cannot happen by auto gensym
                             (set/difference (set left-cols)
                                             (set right-cols)))]
                     ~(emit-join-theta theta-expressions (t/set-mark left-t))))))))
       (.forEach ~ht (reify BiConsumer
                       (accept [_# _# al#]
                         (let [~al al#]
                           (dotimes [i# (.size ~al)]
                             (let [~left-t (.get ~al i#)]
                               (when-not ~(t/get-mark left-t)
                                 (~cont-lambda ~left-t nil)))))))))))

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
           ht# (HashMap.)]
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
       (run!
         (fn [[~k ~al]]
           (let [~@(for [[i col] (map-indexed vector (plan/columns ra))
                         form
                         [(with-meta col {})
                          (emit-column al o col i)]]
                     form)
                 ~@(t/emit-key-bindings k (map first bindings))
                 ~(with-meta plan/%count-sym {}) (.size ~al)]
             ~body))
         ht#))))

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
       (dotimes [i# (alength rs#)]
         (let [~o (aget rs# i#)
               ~@(t/emit-tuple-column-binding o cols)]
           ~body)))))

(defn emit-limit [ra n body]
  (let [o (t/tuple-local ra)
        cols (plan/columns ra)]
    `(let [rs# ~(emit-iterator ra)]
       (loop [n# ~n]
         (when (pos? n#)
           (when (.hasNext rs#)
             (let [~o (.next rs#)
                   ~@(t/emit-tuple-column-binding o cols)]
               ~body
               (recur (dec n#)))))))))

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

    [::plan/order-by ?ra ?order-clauses]
    (emit-order-by ?ra ?order-clauses body)

    [::plan/limit ?ra ?n]
    (emit-limit ?ra ?n body)

    [::plan/without ?ra _]
    (emit-loop ?ra body)

    _
    (throw (ex-info (format "Unknown plan %s" (first ra)) {:ra ra}))))

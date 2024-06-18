(ns com.wotbrew.cinq.eager-loop
  (:require [clojure.set :as set]
            [com.wotbrew.cinq.expr :as expr]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.column :as col]
            [com.wotbrew.cinq.protocols :as p]
            [com.wotbrew.cinq.tuple :as t]
            [meander.epsilon :as m])
  (:import (clojure.lang IReduceInit)
           (com.wotbrew.cinq CinqLongBox CinqMultimap)
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

(defn need-rsn? [bindings] (some (comp #{:cinq/rsn} second) bindings))

(defn emit-scan [src bindings body]
  (let [self-binding (last (keep #(when (= :cinq/self (second %)) (first %)) bindings))
        self-tag (:tag (meta self-binding))
        self-class (if (symbol? self-tag) (resolve self-tag) self-tag)
        o (if self-tag (with-meta (gensym "o") {:tag self-tag}) (gensym "o"))
        rsn (gensym "rsn")
        need-rsn (need-rsn? bindings)
        lambda (if need-rsn
                 `(fn scan-fn# [_# ~rsn ~o]
                    (let [~@(for [[sym k] bindings
                                  form [sym
                                        (cond
                                          (= :cinq/self k) o
                                          (= :cinq/rsn k) rsn
                                          (and self-class (class? self-class) (plan/kw-field self-class k))
                                          (list (symbol (str ".-" (munge (name k)))) o)
                                          (keyword? k) (list k o)
                                          :else (list `get o k))]]
                              form)]
                      (some-> ~body reduced)))
                 `(fn scan-fn# [_# ~o]
                    (let [~@(for [[sym k] bindings
                                  form [sym
                                        (cond
                                          (= :cinq/self k) o
                                          (and self-class (class? self-class) (plan/kw-field self-class k))
                                          (list (symbol (str ".-" (munge (name k)))) o)
                                          (keyword? k) (list k o)
                                          :else (list `get o k))]]
                              form)]
                      (some-> ~body reduced))))]
    #_`(let [step# (fn [~cursor]
                     (let [~o (.val ~cursor)
                           ~rsn (.rsn ~cursor)
                           ~@(for [[sym k] bindings
                                   form [(with-meta sym {})
                                         (cond
                                           (= :cinq/self k) o
                                           (= :cinq/rsn k) rsn
                                           (and self-class (class? self-class) (plan/kw-field self-class k))
                                           (list (symbol (str ".-" (munge (name k)))) o)
                                           (keyword? k) (list k o)
                                           :else (list `get o k))]]
                               form)]
                       ~body))]
         (with-open [cursor# (open-cursor ~(rewrite-expr [] src))]
           (when (.first cursor#)
             (loop []
               (or (step# cursor#) (when (.next cursor#) (recur)))))))
    (if need-rsn
      `(p/scan ~(rewrite-expr [] src) ~lambda nil)
      `(reduce ~lambda nil ~(rewrite-expr [] src)))))

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
                       (f# acc# (aset ~rsn 0 (unchecked-inc (aget ~rsn 0))) x#)) init#)))
        IReduceInit
        (reduce [_ ~f init#]
          (let [~box (object-array 1)]
            (aset ~box 0 init#)
            ~(emit-loop ra `(let [r# (~f (aget ~box 0) ~(if tuple (t/emit-tuple ra) `(clojure.lang.RT/box ~col)))]
                              (if (reduced? r#)
                                (aset ~box 0 @r#)
                                (do (aset ~box 0 r#) nil))))
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
    [::plan/where ra (into [::plan/and]) theta-expressions]
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
                 ~limit-sym (CinqLongBox. 0)
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
      `(let [~limit (CinqLongBox. 0)
             stop# ~(emit-loop [::plan/limit (add-theta-where right theta-expressions) 1 ~limit] body)]
         (when-not (identical? stop# ~limit)
           stop#)))))

(defn emit-apply-anti-join [left right theta body]
  (let [theta-expressions (rewrite-expr [left right] theta)
        limit (gensym "limit")]
    (emit-loop
      left
      `(let [~limit (CinqLongBox. 0)
             stop# ~(emit-loop [::plan/limit (add-theta-where right theta-expressions) 1 ~limit] ::matched)]
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
       ~(emit-loop
          right
          `(when-some [~al (.get ~ht ~(t/emit-key (rewrite-expr [right] right-key-expr)))]
             (or (loop [~i 0]
                   (when (< ~i (.size ~al))
                     ~(case (count theta-expressions)
                        0 `(~cont-lambda (.get ~al ~i) ~(t/emit-tuple right))
                        `(let [~left-t (.get ~al ~i)
                               ~@(t/emit-tuple-column-binding left-t left-cols theta (set right-cols))]
                           (if (and ~@theta-expressions)
                             (~cont-lambda ~left-t ~(t/emit-tuple right))
                             (recur (unchecked-inc ~i)))))))
                 (.forEach ~ht (reify Function
                                 (apply [_# t#]
                                   (let [~left-t t#]
                                     (when-not ~(t/get-mark left-t)
                                       (~cont-lambda ~left-t nil))))))))))))

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

(defn emit-group-project [ra bindings agg-bindings new-projection body]
  ;; TODO broken
  (if (empty? bindings)
    ;; group all
    (let [t (t/tuple-local ra)]
      ;; a better form might be (emit-iter-loop ra variables loop-variables loop-body loop-finished)
      ;; would allow a totally eager pass without the emit-iterator here
      ;; could emit specialised loops for Vector etc as IReduce is often the fastest thing to do even with the virtual calls
      `(let [iter# ~(emit-iterator ra)]
         (loop [~@(for [[_ agg] agg-bindings
                        [acc init] agg
                        form [acc (rewrite-expr [] init)]]
                    form)]
           (if (.hasNext iter#)
             (let [~t (.next iter#)
                   ~@(t/emit-tuple-column-binding t
                                                  (plan/columns ra)
                                                  (rewrite-expr [ra]
                                                                (->> (mapcat second agg-bindings)
                                                                     (map (fn [[_acc _init expr]] expr))
                                                                     vec)))]
               (recur ~@(for [[_ agg] agg-bindings
                              [_ _ expr] agg]
                          (rewrite-expr [ra] expr))))
             (let [~@(for [[sym _ completion] agg-bindings
                           form [sym (rewrite-expr [] completion)]]
                       form)
                   ~@(mapcat (fn [[sym expr]] [sym (rewrite-expr [] expr)]) new-projection)]
               ~body)))))

    ;; group bindings
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
             (let [~@(t/emit-key-bindings k (map first bindings))]
               (loop [i# 0
                      ~@(for [[_ agg] agg-bindings
                              [acc init] agg
                              form [acc (rewrite-expr [] init)]]
                          form)]
                 (if (< i# (.size ~al))
                   (let [~o (.get ~al i#)
                         ~@(t/emit-tuple-column-binding o
                                                        (plan/columns ra)
                                                        (rewrite-expr [ra]
                                                                      (->> (mapcat second agg-bindings)
                                                                           (map (fn [[_acc _init expr]] expr))
                                                                           vec)))]
                     (recur (unchecked-inc i#)
                            ~@(for [[_ agg] agg-bindings
                                    [_ _ expr] agg]
                                (rewrite-expr [ra] expr))))
                   (let [~@(for [[sym _ completion] agg-bindings
                                 form [sym (rewrite-expr [] completion)]]
                             form)
                         ~@(mapcat (fn [[sym expr]] [sym (rewrite-expr [] expr)]) new-projection)]
                     ~body)))))
           ht#)))

    ))

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
       ~(emit-loop ra `(if (< (.-val ~ctr) ~n)
                         (do (set! (.-val ~ctr) (unchecked-inc (.-val ~ctr))) ~body)
                         ~ctr)))))

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

    [::plan/group-project ?ra ?bindings ?aggs ?new-projection]
    (emit-group-project ?ra ?bindings ?aggs ?new-projection body)

    [::plan/order-by ?ra ?order-clauses]
    (emit-order-by ?ra ?order-clauses body)

    [::plan/limit ?ra ?n ?box]
    (emit-limit ?ra ?n ?box body)

    [::plan/without ?ra _]
    (emit-loop ?ra body)

    _
    (throw (ex-info (format "Unknown plan %s" (first ra)) {:ra ra}))))

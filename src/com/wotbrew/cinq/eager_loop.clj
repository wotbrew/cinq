(ns com.wotbrew.cinq.eager-loop
  (:require [clojure.set :as set]
            [com.wotbrew.cinq.expr :as expr]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.column :as col]
            [meander.epsilon :as m])
  (:import (java.util ArrayList Comparator HashMap)
           (java.util.function BiFunction)))

(declare emit-loop emit-array emit-iterator emit-iterable)

(defn rewrite-expr [dependent-relations clj-expr]
  (let [col-maps (mapv #(plan/dependent-cols % clj-expr) dependent-relations)]
    (expr/rewrite col-maps clj-expr #(emit-iterable % 0))))

(defn tuple-local [ra]
  (with-meta (gensym "t") {:tag 'objects}))

(defn emit-tuple [ra]
  (let [cols (plan/columns ra)]
    `(doto (object-array ~(count cols))
       ~@(for [[i col] (map-indexed vector cols)]
           `(aset ~i (clojure.lang.RT/box ~col))))))

(defn emit-list
  ([ra]
   (let [list-sym (gensym "list")]
     `(let [~list-sym (ArrayList.)]
        ~(emit-loop ra `(.add ~list-sym ~(emit-tuple ra)))
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

(defn emit-tuple-arg-list [obj cols]
  (for [i (range (count cols))]
    `(aget ~(with-meta obj {:tag 'objects}) ~i)))

(defn emit-tuple-column-binding
  ([obj cols]
   (vec (interleave (map #(with-meta % {}) cols) (emit-tuple-arg-list obj cols))))
  ([obj cols expr]
   (let [used-in-expr (set (expr/possible-dependencies cols expr))]
     (vec (for [[i col] (map-indexed vector cols)
                :when (used-in-expr col)
                form [(with-meta col {}) `(aget ~(with-meta obj {:tag 'objects}) ~i)]]
            form)))))

(defn emit-key [expressions]
  (mapv (fn [expr] `(clojure.lang.RT/box ~expr)) expressions))

(defn emit-key-args [k cols]
  (for [i (range (count cols))]
    `(nth ~k ~i)))

(defn emit-key-bindings [k cols]
  (vec (interleave (map #(with-meta % {}) cols) (emit-key-args k cols))))

(defn emit-scan [src bindings body]
  (let [self-binding (last (keep #(when (= :cinq/self (second %)) (first %)) bindings))
        self-tag (:tag (meta self-binding))
        self-class (if (symbol? self-tag) (resolve self-tag) self-tag)
        o (if self-tag (with-meta (gensym "o") {:tag self-tag}) (gensym "o"))
        lambda `(fn scan-fn# [~o]
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
    `(run! ~lambda ~(rewrite-expr [] src))))

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
            `(let [t# ~(emit-tuple ra)]
               (.compute ~ht
                         ~(rewrite-expr [ra] key-expr)
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

(defn emit-equi-join [left right left-key-expr right-key-expr theta body]
  (let [al (with-meta (gensym "al") {:tag `ArrayList})
        ht (with-meta (gensym "ht") {:tag `HashMap})
        t (tuple-local left)
        left-cols (plan/columns left)
        right-cols (plan/columns right)]
    `(let [build-fn# ~(build-side-function left left-key-expr)
           ~ht (build-fn#)]
       ~(emit-loop
          right
          `(when-some [~al (.get ~ht ~(rewrite-expr [right] right-key-expr))]
             (dotimes [i# (.size ~al)]
               (let [~t (.get ~al i#)
                     ~@(emit-tuple-column-binding
                         t
                         (plan/columns left)
                         ;; right shadows left if there is a collision
                         ;; todo planner should ensure this cannot happen by auto gensym
                         (set/difference (set left-cols)
                                         (set right-cols)))]
                 ~(emit-join-theta (rewrite-expr [left right] theta) body))))))))

(defn emit-apply-left-join [left right body]
  (let [o (with-meta (gensym "o") {:tag 'objects})
        right-cols (plan/columns right)]
    (emit-loop
      left
      `(let [rs# ~(emit-iterator right)
             has-any# (.hasNext rs#)
             f# (fn [~@right-cols] ~body)]
         (if has-any#
           (loop [~o (.next rs#)]
             (f# ~@(emit-tuple-arg-list o right-cols))
             (when (.hasNext rs#) (recur (.next rs#))))
           (f# ~@(repeat (count right-cols) nil)))))))

(defn emit-single-join [left right body]
  (let [o (with-meta (gensym "o") {:tag 'objects})
        right-cols (plan/columns right)]
    (emit-loop
      left
      `(let [rs# ~(emit-iterator right)
             has-any# (.hasNext rs#)
             f# (fn [~@right-cols] ~body)]
         (if has-any#
           (let [~o (.next rs#)]
             (f# ~@(emit-tuple-arg-list o right-cols)))
           (f# ~@(repeat (count right-cols) nil)))))))

(defn emit-semi-join [left right pred body]
  (let [o (with-meta (gensym "o") {:tag 'objects})
        right-cols (plan/columns right)]
    (emit-loop
      left
      `(let [rs# ~(emit-iterator right)]
         (loop []
           (when (.hasNext rs#)
             (let [~o (.next rs#)]
               (if (let ~(emit-tuple-column-binding o right-cols)
                     ~(rewrite-expr [left right] pred))
                 ~body
                 (recur)))))))))

(defn emit-anti-join [left right pred body]
  (let [o (with-meta (gensym "o") {:tag 'objects})
        right-cols (plan/columns right)]
    (emit-loop
      left
      `(let [rs# ~(emit-iterator right)]
         (when-not
           (loop []
             (when (.hasNext rs#)
               (let [~o (.next rs#)
                     ~@(emit-tuple-column-binding o right-cols)]
                 (if ~(rewrite-expr [left right] pred) true (recur)))))
           ~body)))))

(defn emit-group-by [ra bindings body]
  (let [o (with-meta (gensym "o") {:tag 'objects})
        cols (plan/columns ra)
        k (gensym "k")
        al (with-meta (gensym "al") {:tag `ArrayList})]
    `(let [rs# ~(emit-list ra)
           ht# (HashMap.)]
       ;; agg
       (run!
         (fn [~o]
           (let [~@(emit-tuple-column-binding o cols (mapv second bindings))
                 ~k ~(emit-key (rewrite-expr [ra] (map second bindings)))
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
                          `(col/->Column
                             nil
                             (fn []
                               (let [arr# (object-array (.size ~al))]
                                 (dotimes [j# (.size ~al)]
                                   (let [~o (.get ~al j#)]
                                     (aset arr# j# (aget ~o ~i))))
                                 arr#)))]]
                     form)
                 ~@(emit-key-bindings k (map first bindings))
                 ~(with-meta plan/%count-sym {}) (.size ~al)]
             ~body))
         ht#))))

(defn emit-order-by [ra order-clauses body]
  (let [o (with-meta (gensym "o") {:tag 'objects})
        a (with-meta (gensym "a") {:tag 'objects})
        b (with-meta (gensym "b") {:tag 'objects})
        cols (plan/columns ra)
        comparator `(reify Comparator
                      (compare [_# a# b#]
                        (let [~a a#
                              ~b b#]
                          ~((fn ! [[[expr dir] & more]]
                              `(let [res# (compare (let ~(emit-tuple-column-binding a cols expr) ~(rewrite-expr [ra] expr))
                                                   (let ~(emit-tuple-column-binding b cols expr) ~(rewrite-expr [ra] expr)))]
                                 (if (= 0 res#)
                                   ~(if (seq more) (! more) 0)
                                   (* res# ~(if (= :desc dir) -1 1)))))
                            order-clauses))))]
    `(let [rs# ~(emit-array ra)]
       (java.util.Arrays/sort rs# ~comparator)
       (dotimes [i# (alength rs#)]
         (let [~o (aget rs# i#)
               ~@(emit-tuple-column-binding o cols)]
           ~body)))))

(defn emit-limit [ra n body]
  (let [o (with-meta (gensym "o") {:tag 'objects})
        cols (plan/columns ra)]
    `(let [rs# ~(emit-iterator ra)]
       (loop [n# ~n]
         (when (pos? n#)
           (when (.hasNext rs#)
             (let [~o (.next rs#)
                   ~@(emit-tuple-column-binding o cols)]
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
    (emit-apply-left-join ?left ?right body)

    [::plan/apply :single-join ?left ?right]
    (emit-single-join ?left ?right body)

    [::plan/join ?left ?right ?pred]
    (if (plan/equi-join? ?left ?right ?pred)
      (let [{:keys [left-key right-key theta]} (plan/equi-theta ?left ?right ?pred)]
        (emit-equi-join ?left ?right left-key right-key theta body))
      (emit-loop [::plan/apply :cross-join ?left [::plan/where ?right ?pred]] body))

    ;; todo equi-join
    [::plan/left-join ?left ?right ?pred]
    (emit-loop [::plan/apply :left-join ?left [::plan/where ?right ?pred]] body)

    [::plan/single-join ?left ?right ?pred]
    (emit-loop [::plan/apply :single-join ?left [::plan/where ?right ?pred]] body)

    [::plan/semi-join ?left ?right ?pred]
    (emit-semi-join ?left ?right ?pred body)

    [::plan/anti-join ?left ?right ?pred]
    (emit-anti-join ?left ?right ?pred body)

    [::plan/let ?ra ?bindings]
    (emit-loop ?ra `(let [~@(for [[col expr] ?bindings
                                  form [(with-meta col {}) (rewrite-expr [?ra] expr)]]
                              form)]
                      ~body))

    [::plan/project ?ra ?bindings]
    (emit-loop [::plan/let ?ra ?bindings] body)

    [::plan/group-by ?ra ?bindings]
    (emit-group-by ?ra ?bindings body)

    [::plan/order-by ?ra ?order-clauses]
    (emit-order-by ?ra ?order-clauses body)

    [::plan/limit ?ra ?n]
    (emit-limit ?ra ?n body)

    _
    (throw (ex-info (format "Unknown plan %s" (first ra)) {:ra ra}))))

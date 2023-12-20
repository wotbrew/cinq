(ns com.wotbrew.cinq
  (:import (java.util ArrayList HashMap)))

(defprotocol Relation
  :extend-via-metadata true
  (columns [rel])
  (tuple-seq [rel]))

(defn relation? [x]
  (or (and (contains? (meta x) `tuple-seq)
           (contains? (meta x) `columns))
      (satisfies? Relation x)))

(defn arity [rel] (count (columns rel)))

(defn- join-tuples [l r added-col-count]
  (into l (take added-col-count) (concat r (repeat nil))))

(defn nested-loop-join [rel added-col-count f]
  (with-meta
    (for [l (tuple-seq rel)
          r (tuple-seq (f l))]
      (join-tuples l r added-col-count))
    {`tuple-seq identity
     `columns (constantly (into (columns rel) (repeat added-col-count :cinq/anon)))}))

(defn hash-equi-join
  [build
   build-key-fn
   probe
   probe-key-fn
   added-col-count
   theta-condition]
  (-> (lazy-seq
        (let [hm (HashMap.)
              _ (doseq [b (tuple-seq build)]
                  (let [k (build-key-fn b)
                        nested-list (.get hm k)
                        empty-nested-list (when-not nested-list (ArrayList.))
                        _ (when empty-nested-list (.put hm k empty-nested-list))
                        nested-list (or nested-list empty-nested-list)]
                    (.add nested-list b)))]
          (for [p (tuple-seq probe)
                b (.get hm (probe-key-fn p))
                :when (theta-condition b p)]
            (join-tuples b p added-col-count))))
      (with-meta
        {`tuple-seq identity
         `columns (constantly (into (columns build) (repeat added-col-count :cinq/anon)))})))

(defn hash-equi-left-join
  [probe
   probe-key-fn
   build
   build-key-fn
   added-col-count
   theta-condition]
  (-> (lazy-seq
        (let [hm (HashMap.)
              _ (doseq [b (tuple-seq build)]
                  (let [k (build-key-fn b)
                        nested-list (.get hm k)
                        empty-nested-list (when-not nested-list (ArrayList.))
                        _ (when empty-nested-list (.put hm k empty-nested-list))
                        nested-list (or nested-list empty-nested-list)]
                    (.add nested-list b)))]
          (for [p (tuple-seq probe)
                :let [bs (filter #(theta-condition p %) (.get hm (probe-key-fn p)))]
                b (if (seq bs) bs (repeat added-col-count nil))]
            (join-tuples p b added-col-count))))
      (with-meta
        {`tuple-seq identity
         `columns (constantly (into (columns probe) (repeat added-col-count :cinq/anon)))})))

(defn nested-loop-left-join [rel added-col-count f]
  (with-meta
    (for [l (tuple-seq rel)
          :let [rs (tuple-seq (f l))]
          r (if (seq rs) rs [(repeat nil)])]
      (join-tuples l r added-col-count))
    {`tuple-seq identity
     `columns (constantly (into (columns rel) (repeat added-col-count :cinq/anon)))}))

(defn where [rel f]
  (with-meta (lazy-seq (filter f (tuple-seq rel))) (meta rel)))

(defn group [rel & group-keys]
  (let [group-key (if (seq group-keys) (apply juxt group-keys) (constantly []))
        arity (arity rel)]
    (with-meta
      (for [[gk ts] (group-by group-key (tuple-seq rel))]
        (into (mapv (fn [i] (map (fn [t] (nth t i)) ts)) (range arity)) gk))
      {`tuple-seq identity
       `columns (constantly (into (columns rel) (repeat (count group-keys) :cinq/anon)))})))

(defn order [rel & order-clauses]
  rel)

(defn limit [rel n]
  (with-meta (lazy-seq (take n (tuple-seq rel))) (meta rel)))

(defn add [rel & fns]
  (let [f (fn [t] (reduce (fn [t f] (conj t (f t))) t fns))]
    (with-meta
      (lazy-seq (map f (tuple-seq rel)))
      {`tuple-seq identity
       `columns (constantly (into (columns rel) (repeat (count fns) :cinq/anon)))})))

(defn select [rel & fns]
  (let [f (if (seq fns) (apply juxt fns) (constantly []))]
    (with-meta
      (lazy-seq (map f (tuple-seq rel)))
      {`tuple-seq identity
       `columns (constantly (vec (repeat (count fns) :cinq/anon)))})))

(defn scan [x cols]
  (let [tuple-f (if (seq cols) (apply juxt (map (fn [col] (if (= :cinq/self col) identity col)) cols)) (constantly []))]
    (with-meta
      (map tuple-f x)
      {`tuple-seq identity
       `columns (constantly (vec cols))})))

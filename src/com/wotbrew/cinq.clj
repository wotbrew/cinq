(ns com.wotbrew.cinq
  (:refer-clojure :exclude [use max min count set update replace read range var])
  (:require [com.wotbrew.cinq.eager-loop :as el]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.plan :as plan]
            [com.wotbrew.cinq.protocols :as p]
            [com.wotbrew.cinq.range-test :as range-test]
            [com.wotbrew.cinq.ref-var :as ref-var]
            [meander.epsilon :as m]
            [meander.strategy.epsilon :as r])
  (:import (com.wotbrew.cinq.protocols Index Relvar Scannable)))

(defn optimize-plan [ra] (plan/rewrite ra))

(defn compile-plan [ra] (el/emit-rel ra))

(defn- fix-env [env f & args]
  (let [ctr (atom -1)]
    (binding [parse/*env* env
              plan/*gensym* (fn [pref] (symbol (str pref "__" (swap! ctr inc))))]
      (apply f args))))

(defn parse* [rel-expr] (parse/parse rel-expr))

(defmacro parse [rel-expr] (list `quote (fix-env &env parse* rel-expr)))

(defn tree* [rel-expr]
  (-> (parse* rel-expr)
      optimize-plan
      (plan/prune-cols {})))

(defmacro tree [rel-expr] (list `quote (fix-env &env tree* rel-expr)))

(defmacro plan [rel-expr] (list `quote (plan/stack-view (fix-env &env tree* rel-expr))))

(defmacro q [query body]
  (binding [parse/*env* &env]
    (-> (parse/parse-query query body)
        optimize-plan
        (plan/prune-cols #{})
        compile-plan)))

(defmacro with [bindings rel-expr]
  (binding [parse/*env* &env]
    (-> (parse/parse-cte bindings rel-expr)
        optimize-plan
        (plan/prune-cols #{})
        compile-plan)))

(defmacro union [& rels]
  (binding [parse/*env* &env]
    (-> (parse/parse-union rels)
        optimize-plan
        (plan/prune-cols #{})
        compile-plan)))

(defn- throw-only-in-queries [v]
  (throw (ex-info (format "%s only supported in cinq queries" v) {})))

(defn rel-first
  ([rel] (reduce (fn [_ x] (reduced x)) nil rel))
  ([rel not-found] (reduce (fn [_ x] (reduced x)) not-found rel)))

(defmacro scalar [query selection] `(rel-first (q ~query ~selection)))

(defmacro exists? [query] `(boolean (scalar ~query true)))

(defn- throw-only-in-run [v]
  (throw (ex-info (format "%s only supported in cinq/run bodies" v) {})))

(defmacro delete [target] (throw-only-in-run #'delete))

(defmacro replace [target expr] (throw-only-in-run #'replace))

(defmacro update [target f & args] (throw-only-in-run #'update))

(defmacro agg [init init-sym query & body]
  {:pre [(symbol? init-sym)]}
  `(clojure.core/reduce (fn [~init-sym f#] (f# ~init-sym)) ~init (q ~query (fn [~init-sym] ~@body))))

(defmacro run [query & body]
  (let [srelvar (fn [?alias] (symbol (str ?alias ":cinq") "relvar"))
        srsn (fn [?alias] (symbol (str ?alias ":cinq") "rsn"))
        match
        #(m/match %
           (m/and (?f & ?args)
                  (m/guard (symbol? ?f)))
           (condp = (resolve &env ?f)
             #'replace
             (m/match ?args
               (m/and (?alias ?expr)
                      (m/guard (simple-symbol? ?alias)))
               `(do (p/delete ~(srelvar ?alias) ~(srsn ?alias))
                    (p/insert ~(srelvar ?alias) ~?expr))
               _
               (throw (ex-info "Invalid cinq/replace call" {:args ?args})))

             #'update
             (m/match ?args
               (m/and (?alias ?f & ?args)
                      (m/guard (simple-symbol? ?alias)))
               `(do (p/delete ~(srelvar ?alias) ~(srsn ?alias))
                    (p/insert ~(srelvar ?alias) (~?f ~?alias ~@?args)))
               _
               (throw (ex-info "Invalid cinq/update call" {:args ?args})))

             #'delete
             (m/match ?args
               (m/and (?alias)
                      (m/guard (simple-symbol? ?alias)))
               `(p/delete ~(srelvar ?alias) ~(srsn ?alias))
               _ (throw (ex-info "Invalid delete call" {:args ?args})))
             %)
           _ %)
        rw (r/top-down (r/attempt match))]
    `(agg nil _# ~query ~@(rw body) nil)))

(defmacro sum [expr] (#'throw-only-in-queries #'sum))

(defmacro max [expr] (#'throw-only-in-queries #'max))

(defmacro min [expr] (#'throw-only-in-queries #'min))

(defmacro avg [expr] (#'throw-only-in-queries #'avg))

(defmacro count
  ([] (#'throw-only-in-queries #'count))
  ([expr] (#'throw-only-in-queries #'count)))

(defmacro tuple [& kvs] (#'throw-only-in-queries #'tuple))

(defmacro write [binding & body] `(p/write-transaction ~(second binding) (fn [~(first binding)] ~@body)))

(defmacro read [binding & body] `(p/read-transaction ~(second binding) (fn [~(first binding)] ~@body)))

(defn create [db relvar-id]
  {:pre [(keyword? relvar-id)]}
  (p/create-relvar db relvar-id))

(defn index [relvar indexed-key]
  {:pre [(keyword? indexed-key)]}
  (p/index relvar indexed-key))

(defn rel-set [relvar rel] (p/rel-set relvar rel))

(defn insert [relvar row] (p/insert relvar row))

(declare rel-count)

(extend-protocol p/Scannable
  nil
  (scan [_ _f init] init)
  Object
  (scan [this f init]
    (let [ctr (long-array 1)]
      (reduce
        (fn [acc record]
          (let [i (aget ctr 0)]
            (aset ctr 0 (unchecked-inc i))
            (f acc nil i record)))
        init
        this))))

(extend-protocol clojure.core.protocols/CollReduce
  Scannable
  (coll-reduce
    ([scan f start] (p/scan scan (fn [acc _rv _rsn record] (f acc record)) start))
    ([scan f] (p/scan scan (fn [acc _rv _rsn record] (f acc record)) (f)))))

(extend-protocol p/BigCount
  nil
  (big-count [_] 0)
  Object
  (big-count [rel] (agg 0 n [_ rel] (unchecked-inc n))))

(defn rel-count [rel] (p/big-count rel))

(defn range
  "Index range scan.

  e.g

  (range index > 3) all values over 3.
  (range index < 3) all values below 3.

  (range index < 3 > 1) all values below 3, but above one.
  (range index > 3 <= 100) all values between 3-100 incl."
  ([index test a]
   (condp identical? test
     > (p/range-scan index range-test/gt a nil nil)
     >= (p/range-scan index range-test/gte a nil nil)
     (p/range-scan index nil nil (range-test/index-test test) a)))
  ([index start-test start-key end-test end-key]
   (condp identical? start-test
     < (p/range-scan index (range-test/index-test end-test) end-key (range-test/index-test start-test) start-key)
     <= (p/range-scan index (range-test/index-test end-test) end-key (range-test/index-test start-test) start-key)
     (p/range-scan index (range-test/index-test start-test) start-key (range-test/index-test end-test) end-key))))

(defn getn
  "Index lookup returning a relation of rows (cinq) eq to the key,
  same as (get idx k).

  Like (get idx k), always returns a relation, even on key miss."
  [index k]
  (p/getn index k))

(defn get1
  "Index unique-or-first lookup, returns the matching record or not-found (default nil)."
  ([index k] (p/get1 index k nil))
  ([index k not-found] (p/get1 index k not-found)))

(defmethod print-method Scannable [o w]
  (binding [*print-level* (and (not *print-dup*) *print-level* (dec *print-level*))]
    (do
      (.write w "#cinq/rel [")
      (let [ictr (volatile! -1)]
        (p/scan o (fn [print-length _ _ x]
                    (let [i (vswap! ictr inc)]
                      (if (and (not *print-dup*) print-length)
                        (if (<= print-length 0)
                          (do (.write w " ...") (reduced print-length))
                          (do
                            (when-not (= 0 i) (.write w " "))
                            (print-method x w)
                            (dec print-length)))
                        (do
                          (when-not (= 0 i) (.write w " "))
                          (print-method x w)
                          nil))))
                *print-length*))
      (.write w "] "))))

(defmethod print-method Relvar [o w]
  (binding [*print-level* (and (not *print-dup*) *print-level* (dec *print-level*))]
    (do
      (.write w "#cinq/var [")
      (let [ictr (volatile! -1)]
        (p/scan o (fn [print-length _ _ x]
                    (let [i (vswap! ictr inc)]
                      (if (and (not *print-dup*) print-length)
                        (if (<= print-length 0)
                          (do (.write w " ...") (reduced print-length))
                          (do
                            (when-not (= 0 i) (.write w " "))
                            (print-method x w)
                            (dec print-length)))
                        (do
                          (when-not (= 0 i) (.write w " "))
                          (print-method x w)
                          nil))))
                *print-length*))
      (.write w "] "))))

(defmethod print-method Index [o w]
  (binding [*print-level* (and (not *print-dup*) *print-level* (dec *print-level*))]
    (do
      (.write w "#cinq/idx [")
      (let [ictr (volatile! -1)
            index-key (p/indexed-key o)]
        (p/scan o (fn [print-length _ _ x]
                    (let [i (vswap! ictr inc)
                          k (get x index-key)]
                      (if (and (not *print-dup*) print-length)
                        (if (<= print-length 0)
                          (do (.write w " ...") (reduced print-length))
                          (do
                            (when-not (= 0 i) (.write w ", "))
                            (.write w "[")
                            (print-method k w)
                            (.write w " ")
                            (print-method x w)
                            (.write w "]")
                            (dec print-length)))
                        (do
                          (when-not (= 0 i) (.write w ", "))
                          (.write w "[")
                          (print-method k w)
                          (.write w " ")
                          (print-method x w)
                          (.write w "]")
                          nil))))
                *print-length*))
      (.write w "] "))))

(prefer-method print-method Relvar Scannable)

(prefer-method print-method Index Scannable)

(defn relvar
  "An in-memory relvar, for testing and experimentation at the repl.

  Participates in Clojure's STM, so inherits the transactional
  behaviour of clojure.lang.Ref.

  Will be quite slow compared to com.wotbrew.cinq.lmdb backed relvars."
  []
  (ref-var/->RefVariable (ref [(sorted-map) {}])))

(comment

  (let [a (relvar)
        b (relvar)]
    (dosync
      (rel-set a [1, 2, 3])
      (rel-set b [1, 2])
      (run [x a
            y b
            :when (= x y)]
        (replace x (inc x))
        (replace y (dec y))))
    (println "a:" a)
    (println "b:" b))

  (def x (relvar))
  x
  (index x :foo)
  (insert x {:foo 42})
  (insert x {:foo 43})
  (-> x :foo (get 42))
  (-> x :foo (range > 42))
  (-> x :foo (range > 41 < 43))
  (-> x :foo (get1 43))
  (-> x :foo (get1 44))

  )

(defn desc
  "Scans the index in descending order."
  [idx]
  (p/sorted-scan idx true))

(defn asc
  "Scans the index in ascending order. Scanning the index without specifying an ascending order
  does not guarantee an ascending sequence if keys are long - this function does."
  [idx]
  (p/sorted-scan idx false))

(defn top-k [idx n] (q [x (desc idx) :limit n] x))

(defn bottom-k [idx n] (q [x (asc idx) :limit n] x))

(defn lookup [rel indexed-key key]
  (if-some [idx (get rel indexed-key)]
    (getn idx key)
    (q [{k indexed-key :as r} :when (= k key)] r)))

(defn lookup1
  ([rel indexed-key key] (lookup1 rel indexed-key key nil))
  ([rel indexed-key key not-found]
   (if-some [idx (get rel indexed-key)]
     (get1 idx key not-found)
     (rel-first (q [{k indexed-key :as r} :when (= k key)] r) not-found))))

(defn swap
  "Updates rows where (= (indexed-key row) key), by applying a function to the existing row.

  Returns the result of the last application of the function. Often keys are unique, so in this case the function
  returns the new value of the row.

  Works if the key is not indexed, but will scan the relation to find the row.

  e.g
  (swap customers :id 42 assoc :name \"Bob\")
  ;; ^ sets a customers name to bob by id."
  [rel indexed-key key f & args]
  (let [ret (volatile! nil)]
    (run [r (lookup rel indexed-key key)] (replace r (vreset! ret (apply f r args))))
    @ret))

(defn put
  "Inserts the row if it does not exist otherwise replaces rows where (= (indexed-key row) key) with the supplied row value."
  [rel indexed-key key row]
  (let [ret (volatile! ::no-update)
        row (assoc row indexed-key key)]
    (run [r (lookup rel indexed-key key)]
      (vreset! ret r)
      (replace r row))
    (if (identical? ::no-update @ret)
      (do (insert rel row)
          nil)
      @ret)))

(defn del-key
  "Deletes rows where (= (indexed-key row) key). Returns the number of rows deleted."
  [rel indexed-key key]
  (let [ret (long-array 1)]
    (run [r (lookup rel indexed-key key)]
      (aset ret 0 (unchecked-inc (aget ret 0)))
      (delete r))
    (aget ret 0)))

(defn auto-id
  "Given a relation variable, sets its auto-incrementing key. If a previous key was set, it will be replaced with the one provided.

  When a relation variable has such a key, inserts will provide it (by assoc) before saving to storage.

  If an :id is supplied then the behaviour will be ignored - be aware collisions are possible this way. This behaviour is subject to change.

  The RSN will be used to source the id, this means the id is only unique for a given relvar, and you will have gaps.
  It also means that (insert relvar record) will return the assigned id."
  [relvar key]
  (assert (some? key) "nil cannot be used as an auto-id")
  (p/set-auto-increment relvar key))

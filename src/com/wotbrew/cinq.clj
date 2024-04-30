(ns com.wotbrew.cinq
  (:refer-clojure :exclude [use, for, max, min, count, set, update, replace, run!, read, write])
  (:require [com.wotbrew.cinq.eager-loop :as el]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.protocols :as p]))

(defn parse [query selection] (parse/parse (list 'q query selection)))

(defn optimize-plan [ra] (plan/rewrite ra))

(defn compile-plan [ra] (el/emit-list ra 0))

(defmacro p [query selection]
  (let [ctr (atom -1)]
    (binding [plan/*gensym* (fn [pref] (symbol (str pref "__" (swap! ctr inc))))]
      (-> (parse query selection)
          optimize-plan
          (plan/prune-cols #{})
          plan/stack-view
          (->> (list `quote))))))

(defmacro ptree [query selection]
  (-> (parse query selection)
      optimize-plan
      (plan/prune-cols #{})
      (->> (list `quote))))

(defmacro q [query selection]
  (let [code (binding [parse/*env* &env]
               (-> (parse query selection)
                   optimize-plan
                   (plan/prune-cols #{})
                   compile-plan))]
    ;; todo better representation
    `(vec ~code)))

(defn- throw-only-in-queries [v]
  (throw (ex-info (format "%s only supported in queries" v) {})))

(defmacro scalar [query selection] `(first (q ~query ~selection)))

(defmacro exists? [query] `(scalar ~query true))

(defmacro run! [query & body]
  `(doseq [f# (q ~query (fn [] ~@body))] (f#)))

(defmacro sum [expr] (#'throw-only-in-queries #'sum))

(defmacro max [expr] (#'throw-only-in-queries #'max))

(defmacro min [expr] (#'throw-only-in-queries #'min))

(defmacro avg [expr] (#'throw-only-in-queries #'avg))

(defmacro count
  ([] (#'throw-only-in-queries #'count))
  ([expr] (#'throw-only-in-queries #'count)))

(defmacro tuple [& kvs] (#'throw-only-in-queries #'tuple))

(defn write* [db f]
  (with-open [tx (p/open-write-tx db)]
    (let [r (f tx)]
      (p/commit tx)
      r)))

(defmacro write [binding & body] `(write* ~(second binding) (fn [~(first binding)] ~@body)))

(defn read* [db f] (with-open [tx (p/open-read-tx db)] (f tx)))

(defmacro read [binding & body] `(read* ~(second binding) (fn [~(first binding)] ~@body)))

(defn rel-set [relvar rel] (p/rel-set relvar rel))

(defn insert [relvar record] (p/insert relvar record))

(defmacro delete [relvar alias query-or-pred]
  {:pre [(symbol? relvar)
         (symbol? alias)]}
  (let [rsn (gensym "rsn")]
    `(run! ~(into [{rsn :cinq/rsn, :as alias} relvar] (if (vector? query-or-pred) query-or-pred [:when query-or-pred]))
           (p/delete ~relvar ~rsn))))

(defmacro update [relvar alias query-or-pred expr]
  {:pre [(symbol? relvar)
         (symbol? alias)]}
  (let [rsn (gensym "rsn")]
    `(run! ~(into [{rsn :cinq/rsn, :as alias} relvar] (if (vector? query-or-pred) query-or-pred [:when query-or-pred]))
           (p/replace ~relvar ~rsn ~expr))))

(extend-protocol p/Scannable
  nil
  (scan [_ _f init _rsn] init)
  Object
  (scan [this f init rsn]
    (let [ctr (volatile! -1)]
      (reduce
        (fn [acc record]
          (let [i (vswap! ctr inc)]
            (if (< i rsn)
              acc
              (f acc i record))))
        init
        this))))

(extend-protocol clojure.core.protocols/CollReduce
  com.wotbrew.cinq.protocols.Scannable
  (coll-reduce
    ([scan f start] (p/scan scan (fn [acc _rsn record] (f acc record)) start 0))
    ([scan f] (p/scan scan (fn [acc _rsn record] (f acc record)) (f) 0))))

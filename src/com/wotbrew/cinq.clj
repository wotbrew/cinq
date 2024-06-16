(ns com.wotbrew.cinq
  (:refer-clojure :exclude [use for max min count set update replace run! read])
  (:require [com.wotbrew.cinq.eager-loop :as el]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.protocols :as p])
  (:import (com.wotbrew.cinq.protocols Scannable)))

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
    code))

(defn- throw-only-in-queries [v]
  (throw (ex-info (format "%s only supported in queries" v) {})))

(defmacro scalar [query selection] `(first (q ~query ~selection)))

(defmacro exists? [query] `(scalar ~query true))

(defmacro run! [query & body]
  `(doseq [f# (q ~query (fn [] ~@body))] (f#)))

(defmacro agg [init init-sym query & body]
  {:pre [(symbol? init-sym)]}
  `(clojure.core/reduce (fn [~init-sym f#] (f# ~init-sym)) ~init (q ~query (fn [~init-sym] ~@body))))

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

(defn create
  ([db k] (create db k nil))
  ([db k init]
   (doto (p/create-relvar db k)
     (p/rel-set init))))

(defn rel-set [relvar rel] (p/rel-set relvar rel))

(defn insert [relvar record] (p/insert relvar record))

(defn- add-rsn-to-binding [binding rsn]
  (cond
    (symbol? binding) {rsn :cinq/rsn :as binding}
    (map? binding) (assoc binding rsn :cinq/rsn)
    (sequential? binding) {binding :cinq/self, rsn :cinq/rsn}
    :else (throw (ex-info "Unsupported binding form" {:binding binding}))))

(defmacro delete [query]
  {:pre [(<= 2 (clojure.core/count query))]}
  (let [rsn (gensym "rsn")
        rv (gensym "relvar")
        [binding relvar & query] query]
    `(let [~rv ~relvar]
       (agg
         0
         affected-records#
         ~(into [(add-rsn-to-binding binding rsn) rv] query)
         (if (p/delete ~rv ~rsn)
           (unchecked-inc affected-records#)
           affected-records#)))))

(defmacro update [query expr]
  {:pre [(<= 2 (clojure.core/count query))]}
  (let [rsn (gensym "rsn")
        rv (gensym "relvar")
        [binding relvar & query] query]
    `(let [~rv ~relvar]
       (agg
         0
         affected-records#
         ~(into [(add-rsn-to-binding binding rsn) rv] query)
         (if (p/replace ~rv ~rsn ~expr)
           (unchecked-inc affected-records#)
           affected-records#)))))

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
  Scannable
  (coll-reduce
    ([scan f start] (p/scan scan (fn [acc _rsn record] (f acc record)) start 0))
    ([scan f] (p/scan scan (fn [acc _rsn record] (f acc record)) (f) 0))))

(defmethod print-method Scannable
  [o w]
  (binding [*print-level* (and (not *print-dup*) *print-level* (dec *print-level*))]
    (do
      (.write w "#cinq/relvar [")
      (let [ictr (volatile! -1)]
        (p/scan o (fn [print-length _ x]
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
                *print-length* 0))
      (.write w "] "))))

(defn rel-count [rel]
  ;; TODO (big-count) proto for stats[count].
  (agg 0 n [_ rel] (inc n)))

(defn rel-first [rel]
  (scalar [x rel :limit 1] x))

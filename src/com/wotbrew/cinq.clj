(ns com.wotbrew.cinq
  (:refer-clojure :exclude [use, for, max, min, count, set, update, run!])
  (:require [com.wotbrew.cinq.array-seq :as aseq]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.plan2 :as plan]
            [com.wotbrew.cinq.db0 :as db0]
            [com.wotbrew.cinq.scope :as scope]
            [com.wotbrew.cinq.relvar :as relvar]))

(defn parse [query selection] (parse/parse (list 'q query selection)))
(defn optimize-plan [ra] (plan/rewrite ra))
(defn compile-plan [ra] (aseq/compile-plan ra))
(defmacro plan [query selection] (list `quote (plan/stack-view (optimize-plan (parse query selection)))))

(defmacro q [query selection]
  (let [code (binding [parse/*env* &env]
               (-> (parse query selection)
                   optimize-plan
                   compile-plan))]
    ;; better representation
    `(vec ~code)))

(defn- throw-only-in-queries [v]
  (throw (ex-info (format "%s only supported in queries" v) {})))

(defmacro scalar [query selection] `(first (q ~query ~selection)))

(defmacro run! [query & body]
  `(doseq [f# (q ~query (fn [] ~@body))] (f#)))

(defmacro sum [expr] (#'throw-only-in-queries #'sum))

(defmacro max [expr] (#'throw-only-in-queries #'max))

(defmacro min [expr] (#'throw-only-in-queries #'min))

(defmacro avg [expr] (#'throw-only-in-queries #'avg))

(defmacro count
  ([] (#'throw-only-in-queries #'count))
  ([expr] (#'throw-only-in-queries #'count)))

(defmacro use [db & program]
  `(binding [scope/*tx* (db0/read-transaction ~db)]
     ~@program))

(defmacro do! [db & program]
  `(binding [scope/*tx* (db0/read-write-transaction ~db)]
     (let [ret# (do ~@program)]
       (db0/commit! scope/*tx*)
       ret#)))

(defmacro insert [rv expr] `(relvar/insert! ~rv scope/*tx* ~expr))

(defmacro delete-tuple [rv tid] `(relvar/delete! ~rv scope/*tx* ~tid))

(defmacro assign [rv expr]
  `(let [rv# ~rv]
     (doseq [[tid# _# _#] (relvar/tuple-seq rv# scope/*tx*)]
       (delete-tuple rv# tid#))
     (doseq [m# ~expr]
       (insert rv# m#))))

(defn variable [name] (relvar/->Relvar name))

;; definitions
(defmacro def-var [sym & {:keys [name]}] `(def ~sym (variable ~(or name (str sym)))))
(defmacro def-view [sym query selection])
(defmacro def-query [sym params query selection])

;; database variables
(defmacro mem-db [& init] `(doto (db0/in-memory) (do! ~@init)))
(defmacro init-db [file & init])
(defn load-db [file])
(defn close-db [db-var])

;;
(defn materialize! [db view])
(defn de-materialize! [db view])

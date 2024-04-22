(ns com.wotbrew.cinq
  (:refer-clojure :exclude [use, for, max, min, count, set, update, run!])
  (:require [com.wotbrew.cinq.array-seq :as aseq]
            [com.wotbrew.cinq.eager-loop :as el]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.plan2 :as plan]))

(defn parse [query selection] (parse/parse (list 'q query selection)))
(defn optimize-plan [ra] (plan/rewrite ra))

(defn compile-plan [ra]
  #_(aseq/compile-plan ra)
  (el/emit-list ra 0))

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

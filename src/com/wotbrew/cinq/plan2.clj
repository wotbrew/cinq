(ns com.wotbrew.cinq.plan2
  (:require [meander.epsilon :as m]
            [meander.strategy.epsilon :as r]))

(declare arity column-map columns)

(defn arity [ra] (count (columns ra)))

(defn column-map [ra]
  (let [cols (columns ra)]
    (zipmap cols (range (count cols)))))

(defn columns [ra]
  (m/match ra
    [::scan _ ?cols]
    (mapv first ?cols)

    [::where ?ra _]
    (columns ?ra)

    [::select ?ra _]
    []

    [::product ?left ?right]
    (into (columns ?left) (columns ?right))

    [::dependent-join ?left ?right]
    (into (columns ?left) (columns ?right))

    [::dependent-left-join ?left ?right]
    (into (columns ?left) (columns ?right))

    [::join ?left ?right _]
    (into (columns ?left) (columns ?right))

    [::left-join ?left ?right _]
    (into (columns ?left) (columns ?right))

    [::equi-join ?left _ ?right _ _]
    (into (columns ?left) (columns ?right))

    [::equi-left-join ?left _ ?right _ _]
    (into (columns ?left) (columns ?right))

    [::group-by ?ra ?bindings]
    (let [cols (columns ?ra)]
      (into cols (mapv first ?bindings)))

    [::order-by ?ra _]
    (columns ?ra)

    [::limit ?ra _]
    (columns ?ra)

    [::let ?ra ?bindings]
    (let [cols (columns ?ra)]
      (into cols (map first ?bindings)))))

(defn dependent-cols [ra expr]
  (let [cmap (column-map ra)
        dmap (atom {})]
    ((fn ! [expr]
       (m/match expr
         [::scan ?expr ?bindings]
         (do
           (! ?expr)
           (dotimes [i (count ?bindings)]
             (! (nth (nth ?bindings i) 1))))
         _
         (cond
           (symbol? expr) (when (cmap expr) (swap! dmap conj (find cmap expr)))
           (seq? expr) (run! ! expr)
           (vector? expr) (run! ! expr)
           (map? expr) (run! ! expr)
           (set? expr) (run! ! expr)
           (map-entry? expr) (run! ! expr)
           :else nil))) expr)
    @dmap))

(defn not-dependent? [ra expr]
  (empty? (dependent-cols ra expr)))

(def rewrites
  (r/match
    ;; region decorrelate normal joins
    (m/and [?join ?left [::where ?right ?pred]]
           (m/guard (#{::dependent-join ::dependent-left-join} ?join))
           (m/guard (not-dependent? ?left ?right)))
    ;; =>
    [(case ?join ::dependent-join ::join, ::dependent-left-join :left-join)
     ?left
     ?right
     ?pred]
    ;; endregion

    ;; region join 2 product
    [::join ?left ?right true]
    ;; =>
    [::product ?left ?right]
    ;; endregion

    ;; region product + where 2 join
    [::where [::product ?left ?right] ?pred]
    ;; =>
    [::join ?left ?right ?pred]
    ;; endregion

    ;; region non dependent pred past join
    (m/and [::join ?left ?right ?pred]
           (m/guard (not-dependent? ?right ?pred)))
    ;; =>
    [::product [::where ?left ?pred] ?right]

    (m/and [::join ?left ?right ?pred]
           (m/guard (not-dependent? ?left ?pred)))
    ;; =>
    [::product ?left [::where ?right ?pred]]
    ;; endregion

    ))

(defn fix-max
  "Fixed point strategy combinator with a maximum iteration count.
  See r/fix."
  [s n]
  (fn [t]
    (loop [t t
           i 0]
      (let [t* (s t)]
        (cond
          (= t* t) t*
          (= i n) t*
          :else (recur t* (inc i)))))))

(defn rewrite [ra]
  ((-> #'rewrites
       r/attempt
       r/bottom-up
       (fix-max 100))
   ra))

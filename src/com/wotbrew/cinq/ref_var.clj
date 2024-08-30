(ns com.wotbrew.cinq.ref-var
  (:require [com.wotbrew.cinq.protocols :as p]
            [com.wotbrew.cinq.range-test :as range-test])
  (:import (clojure.lang IFn ILookup IReduceInit)
           (com.wotbrew.cinq.protocols AutoIncrementing BigCount IncrementalRelvar Indexable Relvar Scannable)))

(defn scan [rv [sm] f init] (reduce-kv (fn [acc rsn o] (f acc rv rsn o)) init sm))

(defn last-rsn [sm] (ffirst (reverse sm)))

(def set-conj (fnil conj #{}))

(defn assoc-if-absent [m k v] (if (contains? m k) m (assoc m k v)))

(defn insert [[sm indexes auto-key] record]
  (let [last-rsn (last-rsn sm)
        rsn (if last-rsn (inc last-rsn) 0)
        record (if (some? auto-key) (assoc-if-absent record auto-key rsn) record)
        sm2 (assoc sm rsn record)
        indexes2 (reduce-kv (fn [indexes indexed-key index]
                              (if-some [v (get record indexed-key)]
                                (assoc indexes indexed-key (update index v set-conj rsn))
                                indexes))
                            indexes
                            indexes)]
    [sm2 indexes2 auto-key]))

(defn delete [[sm indexes auto-key] rsn]
  (if-some [record (get sm rsn)]
    (let [sm2 (dissoc sm rsn)
          indexes2 (reduce-kv (fn [indexes indexed-key index]
                                (if-some [v (get record indexed-key)]
                                  (let [new-set (disj (index v rsn) rsn)
                                        new-index (if (seq new-set) (assoc index v new-set) (dissoc index v))]
                                    (assoc indexes indexed-key new-index))
                                  indexes))
                              indexes
                              indexes)]
      [sm2 indexes2 auto-key])
    [sm indexes auto-key]))

(defn rel-set [[_ indexes auto-key] rel]
  (reduce insert [(sorted-map) (update-vals indexes (constantly (sorted-map))) auto-key] rel))

(defn index-hit [rv ref get-rsns]
  (reify
    p/Scannable
    (scan [_ f init]
      (let [[sm] @ref]
        (reduce (fn [acc rsn] (f acc rv rsn (sm rsn))) init (get-rsns))))
    IReduceInit
    (reduce [_ f init] (let [[sm] @ref] (reduce (fn [acc rsn] (f acc (sm rsn))) init (get-rsns))))))

(defn get-index [ref rv indexed-key not-found]
  (let [[_ indexes] @ref]
    (if (indexes indexed-key)
      (let [get-sm (fn [] (first @ref))
            get-index (fn [] (let [[_ indexes] @ref] (get indexes indexed-key)))]
        (reify
          p/Scannable
          (scan [_ f init]
            (let [sm (get-sm)]
              (reduce
                (fn [acc rsns]
                  (reduce
                    (fn [acc2 rsn] (f acc2 rv rsn (sm rsn)))
                    acc
                    rsns))
                init
                (vals (get-index)))))
          IReduceInit
          (reduce [_ f init]
            (let [sm (get-sm)]
              (reduce
                (fn [acc rsns]
                  (reduce
                    (fn [acc2 rsn] (f acc2 (sm rsn)))
                    acc
                    rsns))
                init
                (vals (get-index)))))
          ILookup
          (valAt [idx key] (.valAt idx key nil))
          (valAt [idx key _] (index-hit rv ref (fn [] (get (get-index) key))))
          IFn
          (invoke [idx key] (.valAt idx key nil))
          (invoke [idx key _] (.valAt idx key nil))
          p/Index
          (indexed-key [idx] indexed-key)
          (getn [idx key] (.valAt idx key))
          (get1 [idx key not-found] (if-some [rsns (get (get-index) key)] (get (get-sm) (first rsns)) not-found))
          (range-scan [idx test-a a test-b b]
            (index-hit
              rv
              ref
              (fn []
                (let [index (get-index)
                      test-a (some-> test-a range-test/clj-test)
                      test-b (some-> test-b range-test/clj-test)]
                  (->> (cond
                         (and test-a test-b)
                         (subseq index test-a a test-b b)

                         test-a
                         (subseq index test-a a)

                         test-b
                         (take-while (fn [[key]] (test-b key b)) index))
                       (mapcat val))))))
          (sorted-scan [_ high]
            (index-hit
              rv
              ref
              (fn []
                (if high
                  (mapcat val (reverse (get-index)))
                  (mapcat val (get-index))))))))
      not-found)))

(deftype RefVariable [ref]
  ILookup
  (valAt [rv k] (.valAt rv k nil))
  (valAt [rv indexed-key not-found] (get-index ref rv indexed-key not-found))
  Indexable
  (index [rv indexed-key]
    (dosync
      (alter ref
             (fn [[sm indexes]]
               (if (contains? indexes indexed-key)
                 [sm indexes]
                 (let [index (reduce-kv (fn [idx rsn record]
                                          (if-some [v (get record indexed-key)]
                                            (update idx v set-conj rsn)
                                            idx))
                                        (sorted-map)
                                        sm)
                       indexes2 (assoc indexes indexed-key index)]
                   [sm indexes2]))))
      (.valAt rv indexed-key)))
  Scannable
  (scan [rv f init] (scan rv @ref f init))
  IReduceInit
  (reduce [_ f init] (reduce f init (vals (first @ref))))
  Relvar
  (rel-set [rv rel]
    (dosync (ref-set ref (rel-set @ref rel)))
    nil)
  IncrementalRelvar
  (insert [_ o]
    (dosync (last-rsn (first (alter ref insert o)))))
  (delete [_ rsn]
    (dosync
      (let [ret (volatile! false)]
        (alter ref
               (fn [[sm :as v]]
                 (if (contains? sm rsn)
                   (do (vreset! ret true)
                       (delete v rsn))
                   sm)))
        @ret)))
  BigCount
  (big-count [_] (count (first @ref)))
  AutoIncrementing
  (set-auto-increment [_ key]
    (dosync
      (alter ref (fn [[sm indexes _]] [sm indexes key])))
    nil))

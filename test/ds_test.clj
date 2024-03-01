(ns ds-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq.array-seq :as aseq]
            [com.wotbrew.cinq.row-major :as rm]))

(def next-eid (volatile! 0))

(defrecord Man [id name last-name sex ^long age ^long salary])

(defn random-man []
  (map->Man
    {:id (str (vswap! next-eid inc))
     :name (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
     :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
     :sex (rand-nth [:male :female])
     :age (long (rand-int 100))
     :salary (long (rand-int 100000))}))

(def people (repeatedly random-man))
(def people20k (vec (shuffle (take 20000 people))))

(defn q1 []
  (aseq/q* [p people20k
            :when (= "Ivan" p:name)]
           p:id))

(defn q1-rm []
  (rm/q [^Man p people20k
         :when (= "Ivan" p:name)]
        p:id))

(defn qpred1 []
  (aseq/q* [^Man p people20k
            :when (> p:salary 50000)]
           p:id))

(defn qpred1-rm []
  (rm/q [^Man p people20k
          :when (> p:salary 50000)]
         p:id))

(comment

  (require 'criterium.core)
  (criterium.core/quick-bench (com.wotbrew.cinq.tpch-test/iter-count (q1)))
  (criterium.core/quick-bench (com.wotbrew.cinq.tpch-test/iter-count (q1-rm)))
  (criterium.core/quick-bench (com.wotbrew.cinq.tpch-test/iter-count (qpred1)))
  (criterium.core/quick-bench (com.wotbrew.cinq.tpch-test/iter-count (qpred1-rm)))
  )

(ns ds-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(def next-eid (volatile! 0))

(defrecord Person [^long id name last-name sex ^long age ^long salary])

(defn random-man []
  (map->Person
    {:id (vswap! next-eid inc)
     :name (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
     :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
     :sex (rand-nth [:male :female])
     :age (long (rand-int 100))
     :salary (long (rand-int 100000))}))

(def people (repeatedly random-man))
(def people20k (vec (shuffle (take 20000 people))))

(defn q1 []
  (c/q [^Person p people20k
        :when (= "Ivan" p:name)]
       p:id))

(defn qpred1 []
  (c/q [^Person p people20k
        :when (> p:salary 50000)]
       p:id))

(defn q2 []
  (c/q [^Person p people20k
        :when (= "Ivan" p:name)]
    (c/tuple :id p:id :age p:age)))

(defn q3 []
  (c/q [^Person p people20k
        :when (and (= "Ivan" p:name) (= :male p:sex))]
    (c/tuple :id p:id :last-name p:last-name)))

(defn q4 []
  (c/q [^Person p people20k
        :when (and (= "Ivan" p:name) (= :male p:sex))]
    (c/tuple :id p:id :last-name p:last-name, :age p:age)))

(defn q5 []
  (c/q [^Person p people20k
        ^Person p1 people20k
        :when (and (= "Ivan" p:name)
                   (= p:age p1:age))]
    (c/tuple :id p1:id :last-name p1:last-name, :age p1:age)))

(comment

  (require 'criterium.core)
  (criterium.core/quick-bench (q1))
  (criterium.core/quick-bench (q2))
  (criterium.core/quick-bench (q3))
  (criterium.core/quick-bench (q4))
  (clj-async-profiler.core/profile
    (criterium.core/quick-bench (q5))
    )
  (criterium.core/quick-bench (qpred1))
  )

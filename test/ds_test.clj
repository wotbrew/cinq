(ns ds-test
  (:require [clojure.java.io]
            [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.lmdb :as lmdb]))

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

(defn q1 [{:keys [people]}]
  (c/q [p people
        :when (= "Ivan" p:name)]
       p:id))

(defn qpred1 [{:keys [people]}]
  (c/q [p people
        :when (> p:salary 50000)]
       p:id))

(defn q2 [{:keys [people]}]
  (c/q [p people
        :when (= "Ivan" p:name)]
    (c/tuple :id p:id :age p:age)))

(defn q3 [{:keys [people]}]
  (c/q [p people
        :when (and (= "Ivan" p:name) (= :male p:sex))]
    (c/tuple :id p:id :last-name p:last-name)))

(defn q4 [{:keys [people]}]
  (c/q [p people
        :when (and (= "Ivan" p:name) (= :male p:sex))]
    (c/tuple :id p:id :last-name p:last-name, :age p:age)))

(defn q5 [{:keys [people]}]
  (c/q [p people
        p1 people
        :when (and (= "Ivan" p:name)
                   (= p:age p1:age))]
    (c/tuple :id p1:id :last-name p1:last-name, :age p1:age)))

(comment

  (require 'criterium.core)
  (require 'clj-async-profiler.core)

  (def in-memory {:people people20k})
  (clojure.java.io/delete-file "tmp/ds-test.cinq" true)
  (def lmdb (lmdb/database "tmp/ds-test.cinq"))
  (c/create lmdb :people)
  (c/rel-set (:people lmdb) people20k)

  (defn bench-q [db q]
    (if (instance? com.wotbrew.cinq.protocols.Database db)
      (c/read [db db] (criterium.core/quick-bench (c/rel-count (q db))))
      (criterium.core/quick-bench (c/rel-count (q db)))))

  (clj-async-profiler.core/serve-ui "localhost" 5001)
  (clojure.java.browse/browse-url "http://127.0.0.1:5001")

  (let [db in-memory]
    (println "in memory")
    (println "q1")
    (bench-q db q1)
    (println "q2")
    (bench-q db q2)
    (println "q3")
    (bench-q db q3)
    (println "q4")
    (bench-q db q4)
    (println "q5")
    (bench-q db q5)
    (println "qpred1")
    (bench-q db qpred1))

  (let [db lmdb]
    (println "lmdb")
    (println "q1")
    (bench-q db q1)
    (println "q2")
    (bench-q db q2)
    (println "q3")
    (bench-q db q3)
    (println "q4")
    (bench-q db q4)
    (println "q5")
    (bench-q db q5)
    (println "qpred1")
    (bench-q db qpred1))


  (bench-q lmdb q1)

  )

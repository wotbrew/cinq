(ns com.wotbrew.cinq.lmdb-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.lmdb :as lmdb])
  (:import (java.io File)))

(defonce ^:redef db (lmdb/database (File/createTempFile "cinq-test" ".cinq")))

(defn- cleanup []
  (c/run! [{:keys [k, rsn]} (:lmdb/variables db)
           :when rsn]
    ;; later we might delete the database
    (c/rel-set (get db k) [])))

(use-fixtures :each (fn [f] (try (f) (finally (cleanup)))))

(deftest crud-test
  (c/create db :foo)
  (c/rel-set (:foo db) [1, 2, 3])

  (is (= [1, 2, 3] (vec (:foo db))))

  (c/insert (:foo db) 4)
  (is (= [1, 2, 3, 4] (vec (:foo db))))

  (c/insert (:foo db) 3)
  (is (= [1, 2, 3, 4, 3] (vec (:foo db))))

  (is (= 3 (c/delete [f (:foo db) :when (#{3, 1} f)])))
  (is (= [2, 4] (vec (:foo db))))

  (c/insert (:foo db) 3)
  (is (= [2, 4, 3] (vec (:foo db))))

  (c/update [f (:foo db) :when (even? f)] (inc f))
  (is (= [3, 3, 5] (vec (:foo db)))))

(deftest tx-test
  (c/create db :foo)
  (c/rel-set (:foo db) [0])

  (is (= nil (c/write [db db])))
  (is (= 42 (c/write [db db] 42)))

  (is (= nil (c/read [db db])))
  (is (= 42 (c/read [db db] 42)))

  (->> "returned rel is not valid"
       (is (thrown? Exception (vec (c/read [db db] (:foo db))))))

  (->> "computing in transaction is fine though"
       (is (= [0] (c/read [db db] (vec (:foo db))))))

  (is (thrown? Exception (c/write [db db] (c/insert (:foo db) 1) (throw (ex-info "" {})))))
  (is (= [0] (vec (:foo db))))

  (c/write [db db]
    (c/insert (:foo db) 1)
    (is (= [0, 1] (vec (:foo db))))
    (is (= 2 (c/rel-count (:foo db))))
    42)

  (is (= [0, 1] (vec (:foo db)))))

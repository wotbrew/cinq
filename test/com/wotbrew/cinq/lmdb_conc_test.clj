(ns com.wotbrew.cinq.lmdb-conc-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.lmdb :as lmdb]))

(defn- sleep [ms] (when (pos? ms) (Thread/sleep (int ms))))

(deftest counter-test
  (let [file "tmp/conc-test.cinq"
        _ (io/delete-file file true)]
    (with-open [db (lmdb/database file)]
      (let [ctr (c/create db :ctr)
            _ (c/rel-set ctr [0])
            spawn-read (fn [& _] (future (sleep (rand-int 3)) (c/read [_ db] (c/rel-first ctr))))
            spawn-write (fn [& _] (future (sleep (rand-int 3)) (c/write [_ db] (c/rel-set ctr [(inc (c/rel-first ctr))]))))
            ;; maxreaders if we increase these to 1k
            read-futs (mapv spawn-read (range 100))
            write-futs (mapv spawn-write (range 100))]
        (run! deref write-futs)
        (run! deref read-futs)
        (is (= 100 (c/rel-first ctr)))))))

(ns com.wotbrew.cinq.lmdb-test
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as tcg]
            [clojure.test.check.properties :as tcp]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.lmdb :as lmdb]
            [com.wotbrew.cinq.nio-codec :as codec])
  (:import (java.io File)))

(defn temp-db []
  (lmdb/database (File/createTempFile "cinq-test" ".cinq")))

(defonce ^:redef db (temp-db))

(defn- cleanup []
  (c/run [{:keys [k, rsn]} (:lmdb/variables db)
          :when rsn]
    ;; later we might delete the database
    (c/rel-set (get db k) []))
  (.close db)
  (.bindRoot #'db (temp-db)))

(use-fixtures :each (fn [f] (try (f) (finally (cleanup)))))

(deftest crud-test
  (c/create db :foo)
  (c/rel-set (:foo db) [1, 2, 3])

  (is (= [1, 2, 3] (vec (:foo db))))

  (c/insert (:foo db) 4)
  (is (= [1, 2, 3, 4] (vec (:foo db))))

  (c/insert (:foo db) 3)
  (is (= [1, 2, 3, 4, 3] (vec (:foo db))))

  (is (= nil (c/run [f (:foo db) :when (#{3, 1} f)] (c/delete f))))
  (is (= [2, 4] (vec (:foo db))))

  (c/insert (:foo db) 3)
  (is (= [2, 4, 3] (vec (:foo db))))

  (c/run [f (:foo db) :when (even? f)] (c/replace f (inc f)))
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

(deftest index-test
  (let [foo (c/create db :foo)
        idx (c/index foo :id)]

    (c/rel-set foo [])
    (is (= [] (vec (idx 42))))
    (is (= [] (vec (get idx 42))))

    (c/rel-set foo [{:id 42}])
    (is (= [{:id 42}] (vec (idx 42))))
    (is (= [{:id 42}] (vec (get idx 42))))

    (c/insert foo {:id 42})
    (is (= [{:id 42} {:id 42}] (vec (idx 42))))

    (c/insert foo {:id 43})
    (c/insert foo {:id 44})
    (c/insert foo {:id 45})

    (is (= [{:id 42} {:id 42}] (vec (idx 42))))
    (is (= [{:id 43}] (vec (idx 43))))
    (is (= [{:id 44} {:id 45}] (vec (c/range idx > 43))))
    (is (= [{:id 42} {:id 42} {:id 43}] (vec (c/range idx <= 43))))

    (is (= [{:id 43}] (vec (c/range idx > 42 < 44))))

    (c/run [f (idx 42) :limit 1] (c/delete f))

    (is (= [42] (vec (c/rel [f (get idx 42)] f:id))))
    (is (= [42] (vec (c/rel [f (c/range idx > 40) :when (= f:id 42)] f:id))))
    (is (= [42] (vec (c/rel [f (get idx 42) :when (< f:id 43)] f:id)))))
  )

(s/def ::value
  (s/or :int int?
        :float (s/with-gen float? #(gen/double* {:NaN? false, :infinite? false}))
        :string string?
        :keyword keyword?
        #_#_:symbol symbol?
        :list (s/every ::value :max-count 10)
        :map (s/map-of keyword? ::value :max-count 10)
        :inst (s/with-gen inst? (fn [] (tcg/fmap #(java.util.Date. %) (tcg/choose 0 1720780830887))))))

(s/def ::value-seq (s/every ::value :max-count 10))

(deftest round-trip-test
  (let [file (File/createTempFile "cinq-test" ".cinq")
        open-db (partial lmdb/database file)
        no-shrink true
        vgen ((if no-shrink tcg/no-shrink identity) (s/gen ::value-seq))
        rt (fn [vseq]
             (try
               (and (with-open [db (open-db)]
                      (c/rel-set (c/create db :foo) vseq)
                      (= (vec vseq) (vec (:foo db))))
                    (with-open [db (open-db)]
                      (= (vec vseq) (vec (:foo db)))))
               (finally
                 (io/delete-file file true))))
        prop (tcp/for-all [vseq vgen] (rt vseq))
        tr (if true
             {:pass? true}
             (tc/quick-check 100 prop #_#_:reporter-fn prn))]

    (is (:pass? tr))

    (when-not (:pass? tr)
      (prn (:seed tr))
      (prn (:smallest (:shrunk tr))))

    (is (rt []))
    (is (rt [[]]))
    (is (rt [[{} 0]]))
    (is (rt (map (fn [i] (keyword (str "i" i))) (range 512))))))

(deftest native-eq-test
  (let [foo (c/create db :foo)
        _ (c/rel-set foo [{:n 42}])]
    (is (= 42 (c/rel-first (c/rel [f foo :when (= f:n 42)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (= 42 f:n2)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (= f:n nil)] f:n) ::no-result)))))

(deftest native-cmp-test
  (let [foo (c/create db :foo)
        _ (c/rel-set foo [{:n 42, :n3 nil}])]

    (is (= ::no-result (c/rel-first (c/rel [f foo :when (< f:n 42)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (<= f:n 41)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (> f:n 42)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (>= f:n 43)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (> f:n 42)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (>= f:n 43)] f:n) ::no-result)))

    (is (= ::no-result (c/rel-first (c/rel [f foo :when (< f:n nil)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (< f:n2 Long/MIN_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (< f:n2 Long/MAX_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (< f:n3 Long/MIN_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (< f:n3 Long/MAX_VALUE)] f:n) ::no-result)))

    (is (= ::no-result (c/rel-first (c/rel [f foo :when (<= f:n nil)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (<= f:n2 Long/MIN_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (<= f:n2 Long/MAX_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (<= f:n3 Long/MIN_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (<= f:n3 Long/MAX_VALUE)] f:n) ::no-result)))

    (is (= ::no-result (c/rel-first (c/rel [f foo :when (> f:n nil)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (> f:n2 Long/MIN_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (> f:n2 Long/MAX_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (> f:n3 Long/MIN_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (> f:n3 Long/MAX_VALUE)] f:n) ::no-result)))

    (is (= ::no-result (c/rel-first (c/rel [f foo :when (>= f:n nil)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (>= f:n2 Long/MIN_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (>= f:n2 Long/MAX_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (>= f:n3 Long/MIN_VALUE)] f:n) ::no-result)))
    (is (= ::no-result (c/rel-first (c/rel [f foo :when (>= f:n3 Long/MAX_VALUE)] f:n) ::no-result)))

    (is (= 42 (c/rel-first (c/rel [f foo :when (= f:n 42)] f:n))))
    (is (= 42 (c/rel-first (c/rel [f foo :when (< f:n 43)] f:n))))
    (is (= 42 (c/rel-first (c/rel [f foo :when (> f:n 41)] f:n))))
    (is (= 42 (c/rel-first (c/rel [f foo :when (<= f:n 42)] f:n))))
    (is (= 42 (c/rel-first (c/rel [f foo :when (>= f:n 42)] f:n))))))

(deftest create-relvar-map-resize-test
  (with-open [db (lmdb/database (File/createTempFile "cinq-test" ".cinq") :map-size (* 128 1024))]
    (is (= [] (vec (c/create db :foo))))))

(deftest create-index-map-resize-test
  (with-open [db (lmdb/database (File/createTempFile "cinq-test" ".cinq") :map-size (* 200 1024))]
    (is (= [] (vec (c/create db :foo))))
    (is (= [] (vec (c/index (:foo db) :bar))))))

(deftest sorted-scan-test
  (let [foo (c/create db :foo)
        idx (c/index (:foo db) :a)]
    (c/rel-set foo [{:a 3} {:a 4} {:a 2}])

    (is (= [{:a 4} {:a 3} {:a 2}] (vec (c/desc idx))))
    (is (= [{:a 2} {:a 3} {:a 4}] (vec (c/asc idx))))

    (is (= [{:a 4}] (vec (c/top-k idx 1))))
    (is (= [{:a 4}, {:a 3}] (vec (c/top-k idx 2))))
    (is (= [{:a 4}, {:a 3}, {:a 2}] (vec (c/top-k idx 3))))
    (is (= [{:a 4}, {:a 3}, {:a 2}] (vec (c/top-k idx 4))))
    (is (= [{:a 2} {:a 3}] (vec (c/bottom-k idx 2))))

    (let [a-str (str/join "" (concat (repeat 512 "a") ["a"]))
          b-str (str/join "" (concat (repeat 512 "a") ["b"]))
          c-str (str/join "" (concat (repeat 512 "a") ["c"]))]

      (c/rel-set foo [{:a a-str}
                      {:a c-str}
                      {:a b-str}])

      (is (= [{:a a-str}] (vec (c/bottom-k idx 1))))
      (is (= [{:a a-str} {:a b-str} {:a c-str}] (vec (c/bottom-k idx 3))))
      (is (= [{:a a-str} {:a b-str} {:a c-str}] (vec (c/asc idx))))
      (is (= [{:a c-str} {:a b-str} {:a a-str}] (vec (c/desc idx))))
      (is (= [{:a c-str}] (vec (c/top-k idx 1)))))))

(deftest auto-id-test
  (let [x (c/create db :x)]
    (is (nil? (c/auto-id x :id)))
    (is (= 0 (c/insert x {})))
    (is (= {:id 0} (c/rel-first x)))
    (is (= 1 (c/insert x {})))
    (is (= [{:id 0} {:id 1}] (vec x)))
    (is (= 2 (c/insert x {:id 42})))
    (is (= [{:id 0} {:id 1} {:id 42}] (vec x)))))

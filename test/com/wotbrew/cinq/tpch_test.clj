(ns com.wotbrew.cinq.tpch-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.instant :as inst]
            [com.wotbrew.cinq.column :as col]
            [com.wotbrew.cinq.vector-seq :refer [q]])
  (:import (io.airlift.tpch GenerateUtils TpchColumn TpchColumnType$Base TpchEntity TpchTable)))

(set! *warn-on-reflection* true)

(comment
  (for [t (TpchTable/getTables)]
    (list 'defrecord (symbol (str/capitalize (.getTableName t)))
          (vec
            (for [^TpchColumn c (.getColumns t)]
              (symbol (str/join "_" (rest (str/split (.getColumnName c) #"_" 2))))))))

  )
(defrecord Customer [custkey name address nationkey phone acctbal mktsegment comment])
(defrecord Order [orderkey custkey orderstatus totalprice orderdate orderpriority clerk shippriority comment])
(defrecord Lineitem
  [orderkey
   partkey
   suppkey
   linenumber
   quantity
   extendedprice
   discount
   tax
   returnflag
   linestatus
   shipdate
   commitdate
   receiptdate
   shipinstruct
   shipmode
   comment])
(defrecord Part [partkey name mfgr brand type size container retailprice comment])
(defrecord Partsupp [partkey suppkey availqty supplycost comment])
(defrecord Supplier [suppkey name address nationkey phone acctbal comment])
(defrecord Nation [nationkey name regionkey comment])
(defrecord Region [regionkey name comment])

(def record-ctor
  {"customer" ->Customer
   "orders" ->Order
   "lineitem" ->Lineitem
   "part" ->Part
   "partsupp" ->Partsupp
   "supplier" ->Supplier
   "nation" ->Nation
   "region" ->Region})

(defn tpch-map [^TpchTable t ctor ^TpchEntity b]
  (->> (for [^TpchColumn c (.getColumns t)]
         (condp = (.getBase (.getType c))
           TpchColumnType$Base/IDENTIFIER
           (.getIdentifier c b)
           TpchColumnType$Base/INTEGER
           (.getInteger c b)
           TpchColumnType$Base/VARCHAR
           (.getString c b)
           TpchColumnType$Base/DOUBLE (.getDouble c b)
           TpchColumnType$Base/DATE
           (clojure.instant/read-instant-date
             (GenerateUtils/formatDate (.getDate c b)))))
       (apply ctor)))

(defn generate-maps
  [table-name scale-factor]
  (let [t (TpchTable/getTable table-name)
        ctor (record-ctor (.getTableName t))]
    (map (partial tpch-map t ctor) (seq (.createGenerator t scale-factor 1 1)))))

(defn all-tables [scale-factor]
  (into {} (for [^TpchTable t (TpchTable/getTables)]
             [(keyword (.getTableName t))
              (vec (generate-maps (.getTableName t) scale-factor))])))

(defn parse-result-file [in]
  (with-open [rdr (io/reader in)]
    (vec (for [s (rest (line-seq rdr))]
           (str/split s #"\|")))))

(defn check-answer [qvar all-tables]
  (let [result (parse-result-file (io/resource (format "io/airlift/tpch/queries/%s.result" (name (.toSymbol ^clojure.lang.Var qvar)))))
        rs (qvar all-tables)]
    (testing (str qvar)
      (->> "row counts should match"
           (is (= (count result) (count rs))))
      (doseq [[i row] (map-indexed vector rs)]
        (doseq [[relv tpch col] (map vector row (nth result i nil) (range))
                :let [parsed (try
                               (cond
                                 (int? relv) (Long/parseLong tpch)
                                 (double? relv) (Double/parseDouble tpch)
                                 (inst? relv) (inst/read-instant-date tpch)
                                 :else tpch)
                               (catch Throwable e
                                 tpch))]]
          (cond
            (and (some? relv) (nil? parsed))
            (is (= parsed relv) (str col ", row " i))
            (double? relv)
            (let [err 0.01
                  diff (Math/abs (double (- relv parsed)))]
              (when-not (<= diff err)
                (is (= parsed relv) (str col ", row " i))))
            :else (is (= parsed relv) (str col ", row " i))))))))

(def sf-00001 (delay (all-tables 0.0001)))
(def sf-0001 (delay (all-tables 0.001)))
(def sf-001 (delay (all-tables 0.01)))
(def sf-005 (delay (all-tables 0.05)))
(def sf-01 (delay (all-tables 0.1)))
(def sf-1 (delay (all-tables 1.0)))

(defn q1 [{:keys [lineitem]}]
  (q [l lineitem
      :where (<= l:shipdate #inst "1998-09-02")
      :group-by [returnflag l:returnflag, linestatus l:linestatus]
      :order-by [returnflag :asc, linestatus :asc]]
     ($select :l_returnflag returnflag
              :l_linestatus linestatus
              :sum_qty ($sum l:quantity)
              :sum_base_price ($sum l:extendedprice)
              :sum_disc_price ($sum (* l:extendedprice (- 1 l:discount)))
              :sum_charge ($sum (* l:extendedprice (- 1 l:discount) (+ 1 l:tax)))
              :avg_qty ($avg l:quantity)
              :avg_price ($avg l:extendedprice)
              :avg_disc ($avg l:discount)
              :count_order %count)))

(comment

  (time (count (q1 @sf-005)))
  (check-answer #'q1 @sf-001)
  )

(deftest q1-test (check-answer #'q1 @sf-001))

(defn q2 [{:keys [part, supplier, partsupp, nation, region]}]
  (q [p part
      s supplier
      ps partsupp
      n nation
      r region
      :where
      (and
        (= p:partkey ps:partkey)
        (= s:suppkey ps:suppkey)
        (= p:size 15)
        (str/ends-with? p:type "BRASS")
        (= s:nationkey n:nationkey)
        (= n:regionkey r:regionkey)
        (= r:name "EUROPE")
        (= ps:supplycost (S [ps partsupp
                             s supplier
                             n nation
                             r region
                             :where (and (= p:partkey ps:partkey)
                                         (= s:suppkey ps:suppkey)
                                         (= s:nationkey n:nationkey)
                                         (= n:regionkey r:regionkey)
                                         (= "EUROPE" r:name))
                             :group-by []]
                            ($min ps:supplycost))))
      :order-by [s:acctbal :desc
                 n:name :asc
                 s:name :asc
                 p:partkey :asc]]
     ($select :s_acctbal s:acctbal
              :s_name s:name
              :n_name n:name
              :p_partkey p:partkey
              :p_mfgr p:mfgr
              :s_address s:address
              :s_phone s:phone
              :s_comment s:comment)))

(comment
  (time (count (q2 @sf-001)))
  (check-answer #'q2 @sf-001)

  )

(deftest q2-test (check-answer #'q2 @sf-001))

(defn q3 [{:keys [customer orders lineitem]}]
  (q [c customer
      o orders
      l lineitem
      :where
      (and (= c:mktsegment "BUILDING")
           (= c:custkey o:custkey)
           (= l:orderkey o:orderkey)
           (< o:orderdate #inst "1995-03-15")
           (> l:shipdate #inst "1995-03-15"))
      :group-by [orderkey l:orderkey
                 orderdate o:orderdate
                 shippriority o:shippriority]
      :let [revenue ($sum (* l:extendedprice (- 1 l:discount)))]
      :order-by [revenue :desc, orderdate :asc, orderkey :asc]
      :limit 10]
     ($select
       :l_orderkey orderkey
       :revenue revenue
       :o_orderdate orderdate
       :o_shippriority shippriority)))

(comment
  (time (count (q3 @sf-001)))
  (check-answer #'q3 @sf-001)
  )

(deftest q3-test (check-answer #'q3 @sf-001))

(defn q4 [{:keys [orders, lineitem]}]
  ;; needs decor + semijoin
  (q [o orders
      :where (and (>= o:orderdate #inst "1993-07-01")
                  (< o:orderdate #inst "1993-10-01")
                  ;; todo $exists
                  (S [l lineitem
                      :where (and (= l:orderkey o:orderkey)
                                  (< l:commitdate l:receiptdate))]
                     true))
      :group-by [orderpriority o:orderpriority]
      :order-by [orderpriority :asc]]
     ($select
       :o_orderpriority orderpriority
       :order_count %count)))

(comment

  (time (count (q4 @sf-001)))
  (check-answer #'q4 @sf-001)

  )

(deftest q4-test (check-answer #'q4 @sf-001))

(defn q5 [{:keys [customer, orders, lineitem, supplier, nation, region]}]
  (q [c customer
      o orders
      l lineitem
      s supplier
      n nation
      r region
      :where (and (= c:custkey o:custkey)
                  (= l:orderkey o:orderkey)
                  (= l:suppkey s:suppkey)
                  (= c:nationkey s:nationkey)
                  (= s:nationkey n:nationkey)
                  (= n:regionkey r:regionkey)
                  (= r:name "ASIA")
                  (>= o:orderdate #inst "1994-01-01")
                  (< o:orderdate #inst "1995-01-01"))
      :group-by [nation-name n:name]
      :let [revenue ($sum (* l:extendedprice (- 1 l:discount)))]
      :order-by [revenue :desc]]
     ($select :n_name nation-name
              :revenue revenue)))

(comment

  (time (count (q5 @sf-005)))
  (check-answer #'q5 @sf-001)

  )

(comment
  ((requiring-resolve 'clj-async-profiler.core/serve-ui) 5000)
  ((requiring-resolve 'clojure.java.browse/browse-url) "http://localhost:5000")

  (def dataset @sf-1)
  (def dataset @sf-005)
  (update-vals dataset count)
  (type (first (:lineitem dataset)))

  (doseq [q [#'q1, #'q2, #'q3, #'q4, #'q5]]
    (println q)
    (time (count (q dataset))))

  (time (dotimes [x 1] (count (q1 dataset))))
  (time (dotimes [x 1] (count (q2 dataset))))
  (time (dotimes [x 1] (count (q3 dataset))))
  (time (dotimes [x 1] (count (q4 dataset))))
  (time (dotimes [x 1] (count (q5 dataset))))

  (time (dotimes [x 20] (count (q1 dataset))))
  (time (dotimes [x 100] (count (q2 dataset))))
  (time (dotimes [x 50] (count (q3 dataset))))

  )

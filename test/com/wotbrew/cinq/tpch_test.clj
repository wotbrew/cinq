(ns com.wotbrew.cinq.tpch-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.instant :as inst]
            [com.wotbrew.cinq.column :as col]
            [com.wotbrew.cinq.vector-seq :refer [q]])
  (:import (io.airlift.tpch GenerateUtils TpchColumn TpchColumnType$Base TpchEntity TpchTable)
           (java.util Date)))

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
              :sum_disc_price ($sum (* l:extendedprice (- 1.0 l:discount)))
              :sum_charge ($sum (* l:extendedprice (- 1.0 l:discount) (+ 1.0 l:tax)))
              :avg_qty ($avg l:quantity)
              :avg_price ($avg l:extendedprice)
              :avg_disc ($avg l:discount)
              :count_order %count)))
#_(defn q1-el [{:keys [lineitem]}]
    (com.wotbrew.cinq.eager-loop/q
      [l lineitem
       :where (<= l:shipdate #inst "1998-09-02")
       :group-by [returnflag l:returnflag, linestatus l:linestatus]]
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
  (time (count (q1-el @sf-005)))

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

(deftest q5-test (check-answer #'q5 @sf-001))

(comment

  (time (count (q5 @sf-005)))
  (check-answer #'q5 @sf-001)

  )

(defn q6 [{:keys [lineitem]}]
  (q [l lineitem
      :where (and (>= l:shipdate #inst "1994-01-01")
                  (< l:shipdate #inst "1995-01-01")
                  (>= l:discount 0.05)
                  (<= l:discount 0.07)
                  (< l:quantity 24.0))
      :group-by []]
     ($select :foo ($sum (* l:extendedprice l:discount)))))

(deftest q6-test (check-answer #'q6 @sf-001))

(comment

  (time (count (q6 @sf-005)))
  (check-answer #'q6 @sf-001)

  )

(defn get-year [^Date date]
  (+ 1900 (.getYear date)))

(defn q7 [{:keys [supplier lineitem orders customer nation]}]
  (q [s supplier
      l lineitem
      o orders
      c customer
      n1 nation
      n2 nation
      :where (and (= s:suppkey l:suppkey)
                  (= o:orderkey l:orderkey)
                  (= c:custkey o:custkey)
                  (= s:nationkey n1:nationkey)
                  (= c:nationkey n2:nationkey)
                  (or (and (= "FRANCE" n1:name)
                           (= "GERMANY" n2:name))
                      (and (= "GERMANY" n1:name)
                           (= "FRANCE" n2:name)))
                  (<= #inst "1995-01-01" l:shipdate)
                  (<= l:shipdate #inst "1996-12-31"))
      :let [year (get-year l:shipdate)
            volume (Double/valueOf (* l:extendedprice (- 1.0 l:discount)))
            supp_nation n1:name
            cust_nation n2:name]
      :group-by [supp_nation supp_nation
                 cust_nation cust_nation
                 year year]
      :order-by [supp_nation :asc
                 cust_nation :asc
                 year :asc]]
     ($select :supp_nation supp_nation
              :cust_nation cust_nation
              :l_year year
              :revenue ($sum volume))))

(deftest q7-test (check-answer #'q7 @sf-001))

(comment

  (time (count (q7 @sf-005)))
  (q7 @sf-001)
  (check-answer #'q7 @sf-001))

(defn q8 [{:keys [part supplier region lineitem orders customer nation]}]
  (q [p part
      s supplier
      l lineitem
      o orders
      c customer
      n1 nation
      n2 nation
      r region
      :where
      (and (= p:partkey l:partkey)
           (= s:suppkey l:suppkey)
           (= l:orderkey o:orderkey)
           (= o:custkey c:custkey)
           (= c:nationkey n1:nationkey)
           (= n1:regionkey r:regionkey)
           (= "AMERICA" r:name)
           (= s:nationkey n2:nationkey)
           (<= #inst "1995-01-01" o:orderdate)
           (<= o:orderdate #inst "1996-12-31")
           (= "ECONOMY ANODIZED STEEL" p:type))
      :let [o_year (get-year o:orderdate)
            volume (Double/valueOf (* l:extendedprice (- 1.0 l:discount)))
            nation n2:name]
      :group-by [o_year o_year]
      :order-by [o_year :asc]]
     ($select
       :o_year o_year
       :mkt_share (double (/ ($sum
                               (if (= "BRAZIL" nation)
                                 volume
                                 0.0))
                             ($sum volume))))))

(deftest q8-test (check-answer #'q8 @sf-001))

(comment
  (time (count (q8 @sf-005)))
  (q8 @sf-001)
  (check-answer #'q8 @sf-001))

(defn q9 [{:keys [part supplier lineitem partsupp orders nation]}]
  (q [p part
      s supplier
      l lineitem
      ps partsupp
      o orders
      n nation
      :where
      (and (= s:suppkey l:suppkey)
           (= ps:suppkey l:suppkey)
           (= ps:partkey l:partkey)
           (= p:partkey l:partkey)
           (= o:orderkey l:orderkey)
           (= s:nationkey n:nationkey)
           (str/includes? p:name "green"))
      ;; todo if I do not have the boxing here - the let aset causes reflection
      :let [amount (Double/valueOf (- (* l:extendedprice (- 1.0 l:discount))
                                      (* ps:supplycost l:quantity)))]
      :group-by [nation n:name
                 year (get-year o:orderdate)]
      :order-by [nation :asc, year :desc]]
     ($select :nation nation
              :o_year year
              :sum_profit ($sum amount))))

(deftest q9-test (check-answer #'q9 @sf-001))

(comment
  (time (count (q9 @sf-005)))
  (q9 @sf-001)
  (check-answer #'q9 @sf-001))

(defn q10 [{:keys [lineitem customer orders nation]}]
  (q [c customer
      o orders
      l lineitem
      n nation
      :where
      (and (= c:custkey o:custkey)
           (= l:orderkey o:orderkey)
           (>= o:orderdate #inst "1993-10-01")
           (< o:orderdate #inst "1994-01-01")
           (= "R" l:returnflag)
           (= c:nationkey n:nationkey))
      ;; todo shadowing names here cause issues
      :group-by [c_custkey c:custkey
                 c_name c:name
                 c_acctbal c:acctbal
                 c_phone c:phone
                 n_name n:name
                 c_address c:address
                 c_comment c:comment]
      :let [revenue ($sum (* l:extendedprice (- 1.0 l:discount)))]
      :order-by [revenue :desc, c_custkey :asc]
      :limit 20]
     ($select :c_custkey c_custkey
              :c_name c_name
              :revenue revenue
              :c_acctbal c_acctbal
              :n_name n_name
              :c_address c_address
              :c_phone c_phone
              :c_comment c_comment)))

(deftest q10-test (check-answer #'q10 @sf-001))

(comment
  (time (count (q10 @sf-005)))
  (q10 @sf-001)
  (check-answer #'q10 @sf-001)
  )

(defn q11 [{:keys [partsupp supplier nation]}]
  (q [ps partsupp
      s supplier
      n nation
      :where
      (and (= ps:suppkey s:suppkey)
           (= s:nationkey n:nationkey)
           (= n:name "GERMANY"))
      :group-by [ps_partkey ps:partkey]
      :let [value ($sum (* ps:supplycost ps:availqty))]
      :where (> value (S [ps partsupp
                          s supplier
                          n nation
                          :where
                          (and (= ps:suppkey s:suppkey)
                               (= s:nationkey n:nationkey)
                               (= n:name "GERMANY"))
                          :group-by []]
                         (* 0.0001 ($sum (* ps:supplycost ps:availqty)))))
      :order-by [value :desc]]
     ($select :ps_partkey ps_partkey
              :value value)))

(deftest q11-test (check-answer #'q11 @sf-001))

(comment
  (time (count (q11 @sf-005)))
  (q11 @sf-001)
  (check-answer #'q11 @sf-001)
  )

(comment
  ((requiring-resolve 'clj-async-profiler.core/serve-ui) 5000)
  ((requiring-resolve 'clojure.java.browse/browse-url) "http://localhost:5000")

  (System/gc)

  (def dataset @sf-001)
  (def dataset @sf-005)
  (def dataset @sf-1)

  (update-vals dataset count)
  (type (first (:lineitem dataset)))

  (time
    (do (doseq [q [#'q1, #'q2, #'q3,
                   #'q4, #'q5, #'q6,
                   #'q7, #'q8, #'q9
                   #'q10, #'q11]]
          (println q)
          (time (count (q dataset))))
        (println "done")))

  (time (dotimes [x 1] (count (q1 dataset))))
  (time (dotimes [x 1] (count (q2 dataset))))
  (time (dotimes [x 1] (count (q3 dataset))))
  (time (dotimes [x 1] (count (q4 dataset))))
  (time (dotimes [x 1] (count (q5 dataset))))
  (time (dotimes [x 1] (count (q6 dataset))))
  (time (dotimes [x 1] (count (q7 dataset))))
  (time (dotimes [x 1] (count (q8 dataset))))
  (time (dotimes [x 1] (count (q9 dataset))))
  (time (dotimes [x 1] (count (q10 dataset))))
  (time (dotimes [x 1] (count (q11 dataset))))

  (clj-async-profiler.core/profile
    {}
    (dotimes [x 20] (count (q1 dataset))))

  (clj-async-profiler.core/profile
    {}
    (dotimes [x 20] (count (q7 dataset))))

  (time (dotimes [x 20] (count (q1-el dataset))))
  (time (dotimes [x 100] (count (q2 dataset))))
  (time (dotimes [x 50] (count (q3 dataset))))

  )

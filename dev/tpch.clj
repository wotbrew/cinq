(ns tpch
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [com.wotbrew.cinq.lmdb :as lmdb]
            [com.wotbrew.cinq :as c])
  (:import (io.airlift.tpch GenerateUtils TpchColumn TpchColumnType$Base TpchEntity TpchTable)
           (java.util Date)))

(defn tpch-map [^TpchTable t ^TpchEntity b]
  (->> (for [^TpchColumn c (.getColumns t)]
         [(keyword (second (str/split (.getColumnName c) #"_")))
          (condp = (.getBase (.getType c))
            TpchColumnType$Base/IDENTIFIER
            (.getIdentifier c b)
            TpchColumnType$Base/INTEGER
            (long (.getInteger c b))
            TpchColumnType$Base/VARCHAR
            (.getString c b)
            TpchColumnType$Base/DOUBLE (.getDouble c b)
            TpchColumnType$Base/DATE
            (clojure.instant/read-instant-date
              (GenerateUtils/formatDate (.getDate c b))))])
       (into {})))

(defn generate-maps
  [table-name scale-factor]
  (let [t (TpchTable/getTable table-name)]
    (map (partial tpch-map t) (seq (.createGenerator t scale-factor 1 1)))))

(defn dataset [scale-factor]
  (into {} (for [^TpchTable t (TpchTable/getTables)]
             [(keyword (.getTableName t))
              (vec (generate-maps (.getTableName t) scale-factor))])))

(defn load-dataset [db sf]
  (doseq [^TpchTable t (TpchTable/getTables)
          :let [tname (keyword (.getTableName t))]]
    (c/create db tname))
  (doseq [^TpchTable t (TpchTable/getTables)
          :let [tname (keyword (.getTableName t))]]
    (let [start (System/nanoTime)
          _ (c/rel-set (tname db) (generate-maps (.getTableName t) sf))
          end (System/nanoTime)]
      (println (format "%s - %s records, %.2fms" tname (c/rel-count (tname db)) (* 1e-6 (- end start)))))))

(defn q1 [{:keys [lineitem]}]
  (c/q [l lineitem
        :when (<= l:shipdate #inst "1998-09-02")
        :group [returnflag l:returnflag, linestatus l:linestatus]
        :order [returnflag :asc, linestatus :asc]]
    (c/tuple :l_returnflag returnflag
             :l_linestatus linestatus
             :sum_qty (c/sum ^double l:quantity)
             :sum_base_price (c/sum ^double l:extendedprice)
             :sum_disc_price (c/sum (* ^double l:extendedprice (- 1.0 ^double l:discount)))
             :sum_charge (c/sum (* ^double l:extendedprice (- 1.0 ^double l:discount) (+ 1.0 ^double l:tax)))
             :avg_qty (c/avg ^double l:quantity)
             :avg_price (c/avg ^double l:extendedprice)
             :avg_disc (c/avg ^double l:discount)
             :count_order (c/count))))

(defn q2 [{:keys [part, supplier, partsupp, nation, region]}]
  (c/q [r region
        n nation
        s supplier
        p part
        ps partsupp
        :when
        (and
          (= p:partkey ps:partkey)
          (= s:suppkey ps:suppkey)
          (= p:size 15)
          (str/ends-with? p:type "BRASS")
          (= s:nationkey n:nationkey)
          (= n:regionkey r:regionkey)
          (= r:name "EUROPE")
          (= ps:supplycost (c/scalar [ps partsupp
                                      s supplier
                                      n nation
                                      r region
                                      :when (and (= p:partkey ps:partkey)
                                                 (= s:suppkey ps:suppkey)
                                                 (= s:nationkey n:nationkey)
                                                 (= n:regionkey r:regionkey)
                                                 (= "EUROPE" r:name))
                                      :group []]
                             (c/min ps:supplycost))))
        :order [s:acctbal :desc
                n:name :asc
                s:name :asc
                p:partkey :asc]]
    (c/tuple
      :s_acctbal s:acctbal
      :s_name s:name
      :n_name n:name
      :p_partkey p:partkey
      :p_mfgr p:mfgr
      :s_address s:address
      :s_phone s:phone
      :s_comment s:comment)))

(defn q3 [{:keys [customer orders lineitem]}]
  (c/q [c customer
        o orders
        l lineitem
        :when
        (and (= c:mktsegment "BUILDING")
             (= c:custkey o:custkey)
             (= l:orderkey o:orderkey)
             (< o:orderdate #inst "1995-03-15")
             (> l:shipdate #inst "1995-03-15"))
        :group [orderkey l:orderkey
                orderdate o:orderdate
                shippriority o:shippriority]
        :let [revenue (c/sum (* l:extendedprice (- 1 l:discount)))]
        :order [revenue :desc, orderdate :asc, orderkey :asc]
        :limit 10]
    (c/tuple
      :l_orderkey orderkey
      :revenue revenue
      :o_orderdate orderdate
      :o_shippriority shippriority)))

(defn q4 [{:keys [orders, lineitem]}]
  (c/q [o orders
        :when (and (>= o:orderdate #inst "1993-07-01")
                   (< o:orderdate #inst "1993-10-01")
                   (c/exists? [^Lineitem l lineitem
                               :when (and (= l:orderkey o:orderkey)
                                          (< l:commitdate l:receiptdate))]))
        :group [orderpriority o:orderpriority]
        :order [orderpriority :asc]]
    (c/tuple
      :o_orderpriority orderpriority
      :order_count (c/count))))

(defn q5 [{:keys [customer, orders, lineitem, supplier, nation, region]}]
  (c/q [o orders
        c customer
        l lineitem
        s supplier
        r region
        n nation
        :when (and (= c:custkey o:custkey)
                   (= l:orderkey o:orderkey)
                   (= l:suppkey s:suppkey)
                   (= c:nationkey s:nationkey)
                   (= s:nationkey n:nationkey)
                   (= n:regionkey r:regionkey)
                   (= r:name "ASIA")
                   (>= o:orderdate #inst "1994-01-01")
                   (< o:orderdate #inst "1995-01-01"))
        :group [nation-name n:name]
        :let [revenue (c/sum (* l:extendedprice (- 1 l:discount)))]
        :order [revenue :desc]]
    (c/tuple :n_name nation-name
             :revenue revenue)))

(defn q6 [{:keys [lineitem]}]
  (c/q [l lineitem
        :when (and (>= l:shipdate #inst "1994-01-01")
                   (< l:shipdate #inst "1995-01-01")
                   (>= l:discount 0.05)
                   (<= l:discount 0.07)
                   (< l:quantity 24.0))
        :group []]
    (c/tuple :foo (c/sum (* l:extendedprice l:discount)))))

(defn get-year [^Date date]
  (+ 1900 (.getYear date)))

(defn q7 [{:keys [supplier lineitem orders customer nation]}]
  (c/q [s supplier
        l lineitem
        o orders
        c customer
        n1 nation
        n2 nation
        :when (and (= s:suppkey l:suppkey)
                   (= o:orderkey l:orderkey)
                   (= c:custkey o:custkey)
                   (= s:nationkey n1:nationkey)
                   (= c:nationkey n2:nationkey)

                   ;; good speed up if we can push down preds like this
                   #_(or (= "FRANCE" n1:name) (= "GERMANY" n1:name))
                   #_(or (= "FRANCE" n2:name) (= "GERMANY" n2:name))

                   (or (and (= "FRANCE" n1:name)
                            (= "GERMANY" n2:name))
                       (and (= "GERMANY" n1:name)
                            (= "FRANCE" n2:name)))
                   (<= #inst "1995-01-01" l:shipdate)
                   (<= l:shipdate #inst "1996-12-31"))
        :let [year (get-year l:shipdate)
              volume (* l:extendedprice (- 1.0 l:discount))]
        :group [supp_nation n1:name
                cust_nation n2:name
                year year]
        :order [supp_nation :asc
                cust_nation :asc
                year :asc]]
    (c/tuple :supp_nation supp_nation
             :cust_nation cust_nation
             :l_year year
             :revenue (c/sum volume))))

(defn q8 [{:keys [part supplier region lineitem orders customer nation]}]
  (c/q [r region
        p part
        s supplier
        l lineitem
        o orders
        c customer
        n1 nation
        n2 nation
        :when
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
              volume (* l:extendedprice (- 1.0 l:discount))
              nation n2:name]
        :group [o_year o_year]
        :order [o_year :asc]]
    (c/tuple
      :o_year o_year
      :mkt_share (double (/ (c/sum
                              (if (= "BRAZIL" nation)
                                volume
                                0.0))
                            (c/sum volume))))))

(defn q9 [{:keys [part supplier lineitem partsupp orders nation]}]
  (c/q [p part
        l lineitem
        s supplier
        ps partsupp
        n nation
        o orders
        :when
        (and (= s:suppkey l:suppkey)
             (= ps:suppkey l:suppkey)
             (= ps:partkey l:partkey)
             (= p:partkey l:partkey)
             (= o:orderkey l:orderkey)
             (= s:nationkey n:nationkey)
             (str/includes? p:name "green"))
        :let [amount (- (* l:extendedprice (- 1.0 l:discount)) (* ps:supplycost l:quantity))]
        :group [nation n:name
                year (get-year o:orderdate)]
        :order [nation :asc, year :desc]]
    (c/tuple :nation nation
             :o_year year
             :sum_profit (c/sum amount))))

(defn q10 [{:keys [lineitem customer orders nation]}]
  (c/q [n nation
        c customer
        o orders
        l lineitem
        :when
        (and (= c:custkey o:custkey)
             (= l:orderkey o:orderkey)
             (>= o:orderdate #inst "1993-10-01")
             (< o:orderdate #inst "1994-01-01")
             (= "R" l:returnflag)
             (= c:nationkey n:nationkey))
        ;; todo shadowing names here cause issues
        :group [c_custkey c:custkey
                c_name c:name
                c_acctbal c:acctbal
                c_phone c:phone
                n_name n:name
                c_address c:address
                c_comment c:comment]
        :let [revenue (c/sum (* l:extendedprice (- 1.0 l:discount)))]
        :order [revenue :desc, c_custkey :asc]
        :limit 20]
    (c/tuple :c_custkey c_custkey
             :c_name c_name
             :revenue revenue
             :c_acctbal c_acctbal
             :n_name n_name
             :c_address c_address
             :c_phone c_phone
             :c_comment c_comment)))

(defn q11 [{:keys [partsupp supplier nation]}]
  (c/q [n nation
        s supplier
        ps partsupp
        :when
        (and (= ps:suppkey s:suppkey)
             (= s:nationkey n:nationkey)
             (= n:name "GERMANY"))
        :group [ps_partkey ps:partkey]
        :let [value (c/sum (* ps:supplycost ps:availqty))]
        :when (> value (c/scalar [ps partsupp
                                  s supplier
                                  n nation
                                  :when
                                  (and (= ps:suppkey s:suppkey)
                                       (= s:nationkey n:nationkey)
                                       (= n:name "GERMANY"))
                                  :group []]
                         (* 0.0001 (c/sum (* ps:supplycost ps:availqty)))))
        :order [value :desc]]
    (c/tuple :ps_partkey ps_partkey
             :value value)))

(defn q12 [{:keys [orders lineitem]}]
  (c/q [o orders
        l lineitem
        :when (and (= o:orderkey l:orderkey)
                   (contains? #{"MAIL", "SHIP"} l:shipmode)
                   (< l:commitdate l:receiptdate)
                   (< l:shipdate l:commitdate)
                   (>= l:receiptdate #inst "1994-01-01")
                   (< l:receiptdate #inst "1995-01-01"))
        :group [shipmode l:shipmode]
        :order [shipmode :asc]]
    (c/tuple
      :shipmode shipmode
      :high_line_count (c/sum (case o:orderpriority "1-URGENT" 1 "2-HIGH" 1 0))
      :low_line_count (c/sum (case o:orderpriority "1-URGENT" 0 "2-HIGH" 0 1)))))

(defn q13 [{:keys [customer orders]}]
  (c/q [c customer
        :left-join [o orders (and (= c:custkey o:custkey)
                                  (not (re-find #"special.*?requests" o:comment)))]
        :group [custkey c:custkey]
        :group [order-count (c/count o:orderkey)]
        :order [(c/count) :desc, order-count :desc]]
    (c/tuple :c_count order-count, :custdist (c/count))))

(defn q14 [{:keys [lineitem part]}]
  (c/q [l lineitem
        p part
        :when (and (= l:partkey p:partkey)
                   (>= l:shipdate #inst "1995-09-01")
                   (< l:shipdate #inst "1995-10-01"))
        :group []]
    (c/tuple
      :promo_revenue
      (* 100.0
         (/ (c/sum (if (str/starts-with? p:type "PROMO")
                     (* l:extendedprice (- 1.0 l:discount))
                     0.0))
            (c/sum (* l:extendedprice (- 1.0 l:discount))))))))

(defn q15 [{:keys [lineitem supplier]}]
  (let [revenue (c/q [l lineitem
                      :when (and (>= l:shipdate #inst "1996-01-01")
                                 (< l:shipdate #inst "1996-04-01"))
                      :group [suppkey l:suppkey]
                      :let [total_revenue (c/sum (* l:extendedprice (- 1.0 l:discount)))]]
                  {:suppkey suppkey
                   :total_revenue total_revenue})]
    (c/q [s supplier
          r revenue
          :when (and (= s:suppkey r:suppkey)
                     (= r:total_revenue (c/scalar [r revenue :group []] (c/max r:total_revenue))))
          :order [s:suppkey :asc]]
      (c/tuple :s_suppkey s:suppkey
               :s_name s:name
               :s_address s:address
               :s_phone s:phone
               :total_revenue r:total_revenue))))

(defn q16 [{:keys [partsupp part supplier]}]
  (c/q [p part
        ps partsupp
        :when (and (= p:partkey ps:partkey)
                   (not= p:brand "Brand#45")
                   (not (str/starts-with? p:type "MEDIUM POLISHED"))
                   (contains? #{49, 14, 23, 45, 19, 3, 36, 9} p:size)
                   (not (contains? (c/scalar [s supplier
                                              :when (re-find #".*?Customer.*?Complaints.*?" s:comment)
                                              :group []]
                                     (set s:suppkey))
                                   ps:suppkey)))
        :group [brand p:brand, type p:type, size p:size]
        :let [supplier-cnt (Long/valueOf (count (set ps:suppkey)))]
        :order [supplier-cnt :desc, brand :asc, type :asc, size :asc]]
    (c/tuple :p_brand brand
             :p_type type
             :p_size size
             :supplier_cnt supplier-cnt)))

(defn q17 [{:keys [lineitem part]}]
  (c/q [p part
        l lineitem
        :when (and (= p:partkey l:partkey)
                   (= p:brand "Brand#23")
                   (= p:container "MED BOX")
                   (< l:quantity
                      (c/scalar [l2 lineitem
                                 :when (= l2:partkey p:partkey)
                                 :group []]
                        (* 0.2 (c/avg l2:quantity)))))
        :group []]
    (c/tuple :avg_yearly (when (pos? %count) (/ (c/sum l:extendedprice) 7.0)))))

(defn q18 [{:keys [customer orders lineitem]}]
  ;; TODO make this a sub query without weirdness
  (let [orderkeys (set (c/q [l lineitem
                             :group [ok l:orderkey]
                             :when (> (c/sum l:quantity) 300)]
                         ok))]
    (c/q [o orders
          c customer
          l lineitem
          :when (and (contains? orderkeys o:orderkey)
                     (= c:custkey o:custkey)
                     (= o:orderkey l:orderkey))
          :group [name c:name
                  custkey c:custkey
                  orderkey o:orderkey
                  orderdate o:orderdate
                  totalprice o:totalprice]
          :order [totalprice :desc, orderdate :asc, orderkey :asc]
          :limit 100]
      (c/tuple :c_name name
               :c_custkey custkey
               :c_orderkey orderkey
               :c_orderdate orderdate
               :c_totalprice totalprice
               :quantity (c/sum l:quantity)))))

(defn q19 [{:keys [lineitem part]}]
  (c/q [l lineitem
        p part
        :when (and (= l:partkey p:partkey)
                   (= l:shipinstruct "DELIVER IN PERSON")
                   (contains? #{"AIR" "AIR REG"} l:shipmode)
                   (or (and (= p:brand "Brand#12")
                            (contains? #{"SM CASE" "SM BOX" "SM PACK" "SM PKG"} p:container)
                            (>= l:quantity 1)
                            (<= l:quantity 11)
                            (<= 1 p:size 5))
                       (and (= p:brand "Brand#23")
                            (contains? #{"MED BAG" "MED BOX" "MED PKG" "MED PACK"} p:container)
                            (>= l:quantity 10)
                            (<= l:quantity 20)
                            (<= 1 p:size 10))
                       (and (= p:brand "Brand#34")
                            (contains? #{"LG CASE" "LG BOX" "LG PACK" "LG PKG"} p:container)
                            (>= l:quantity 20)
                            (<= l:quantity 30)
                            (<= 1 p:size 15))))
        :group []]
    (c/tuple :revenue (c/sum (* l:extendedprice (- 1.0 l:discount))))))

(defn q20 [{:keys [supplier, nation, partsupp, lineitem, part]}]
  (c/q [n nation
        s supplier
        :when (and (contains? (c/scalar [ps partsupp
                                         :when (and (contains? (c/scalar [p part
                                                                          :when (str/starts-with? p:name "forest")
                                                                          :group []]
                                                                 (set p:partkey))
                                                               ps:partkey)
                                                    (> ps:availqty (c/scalar [l lineitem
                                                                              :when (and (= l:partkey ps:partkey)
                                                                                         (= l:suppkey ps:suppkey)
                                                                                         (>= l:shipdate #inst "1994-01-01")
                                                                                         (< l:shipdate #inst "1995-01-01"))
                                                                              :group []]
                                                                     (* 0.5 (c/sum l:quantity)))))
                                         :group []]
                                (set ps:suppkey))
                              s:suppkey)
                   (= s:nationkey n:nationkey)
                   (= n:name "CANADA"))
        :order [s:name :asc]]
    (c/tuple :name s:name, :address s:address)))

(defn q21 [{:keys [supplier lineitem orders nation]}]
  (c/q [n nation
        s supplier
        o orders
        l1 lineitem
        :when (and (= s:suppkey l1:suppkey)
                   (= o:orderkey l1:orderkey)
                   (= o:orderstatus "F")
                   (> l1:receiptdate l1:commitdate)
                   (c/exists?
                     [l2 lineitem
                      :when (and (= l2:orderkey l1:orderkey)
                                 (not= l2:suppkey l1:suppkey))])
                   (not (c/exists?
                          [l3 lineitem
                           :when (and (= l3:orderkey l1:orderkey)
                                      (not= l3:suppkey l1:suppkey)
                                      (> l3:receiptdate l3:commitdate))]))
                   (= n:nationkey s:nationkey)
                   (= n:name "SAUDI ARABIA"))
        :group [s_name s:name]
        :let [numwait (c/count)]
        :order [numwait :desc]
        :limit 100]
    (c/tuple
      :name s_name
      :numwait numwait)))

(defn q22 [{:keys [customer orders]}]
  (c/q [c customer
        :let [cntrycode (subs c:phone 0 2)]
        :when (and (contains? #{"13", "31", "23", "29", "30", "18", "17"} cntrycode)
                   (> c:acctbal (c/scalar [c customer
                                           :when (and (> c:acctbal 0.0)
                                                      (contains? #{"13", "31", "23", "29", "30", "18", "17"} (subs c:phone 0 2)))
                                           :group []]
                                  (c/avg c:acctbal)))
                   (not (c/exists? [o orders :when (= o:custkey c:custkey)])))
        :group [cntrycode cntrycode]
        :order [cntrycode :asc]]
    (c/tuple :cntrycode cntrycode,
             :numcust (c/count),
             :totacctbal (c/sum c:acctbal))))

(def all-queries
  [#'tpch/q1
   #'tpch/q2
   #'tpch/q3
   #'tpch/q4
   #'tpch/q5
   #'tpch/q6
   #'tpch/q7
   #'tpch/q8
   #'tpch/q9
   #'tpch/q10
   #'tpch/q11
   #'tpch/q12
   #'tpch/q13
   #'tpch/q14
   #'tpch/q15
   #'tpch/q16
   #'tpch/q17
   #'tpch/q18
   #'tpch/q19
   #'tpch/q20
   #'tpch/q21
   #'tpch/q22])

(defn run-all [db]
  (mapv (fn [q] (q db)) all-queries))


(comment

  (io/delete-file "tmp/tpch-05.cinq" true)
  (io/delete-file "tmp/tpch-1.cinq" true)

  (def sf05 (lmdb/database "tmp/tpch-05.cinq"))
  (.close sf05)
  (load-dataset sf05 0.05)

  (def sf1 (lmdb/database "tmp/tpch-1.cinq"))
  (.close sf1)
  (load-dataset sf1 1.0)

  (def db sf05)
  (def db sf1)

  (time (mapv c/rel-count (run-all db)))
  (require 'criterium.core)
  (criterium.core/quick-bench (run-all db))
  )

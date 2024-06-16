(ns com.wotbrew.cinq.tpch-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.instant :as inst]
            [com.wotbrew.cinq :as c :refer [q p]]
            [com.wotbrew.cinq.parse :as parse]
            [com.wotbrew.cinq.plan2 :as plan])
  (:import (io.airlift.tpch GenerateUtils TpchColumn TpchColumnType$Base TpchEntity TpchTable)
           (java.util Date)))

(set! *warn-on-reflection* true)

(defn iter-count [iterable]
  (let [iter (.iterator ^Iterable iterable)]
    (loop [n 0]
      (if (.hasNext iter)
        (do (.next iter)
            (recur (unchecked-inc n)))
        n))))

(defn view-plan [q]
  (-> q parse/parse plan/rewrite plan/stack-view))

(comment

  (for [t (TpchTable/getTables)]
    (list 'defrecord (symbol (str/capitalize (.getTableName t)))
          (vec
            (for [^TpchColumn c (.getColumns t)]
              (symbol (str/join "_" (rest (str/split (.getColumnName c) #"_" 2))))))))

  )

(defrecord Customer [^long custkey name address ^long nationkey phone ^double acctbal mktsegment comment])

(defrecord Order [^long orderkey
                  ^long custkey
                  orderstatus
                  ^double totalprice
                  orderdate
                  orderpriority
                  clerk
                  shippriority
                  comment])

(defrecord Lineitem
  [^long orderkey
   ^long partkey
   ^long suppkey
   ^long linenumber
   ^double quantity
   ^double extendedprice
   ^double discount
   ^double tax
   returnflag
   linestatus
   shipdate
   commitdate
   receiptdate
   shipinstruct
   shipmode
   comment])
(defrecord Part [^long partkey name mfgr brand type ^long size container ^double retailprice comment])
(defrecord Partsupp [^long partkey ^long suppkey ^long availqty ^double supplycost comment])
(defrecord Supplier [^long suppkey name address ^long nationkey phone ^double acctbal comment])
(defrecord Nation [^long nationkey name ^long regionkey comment])
(defrecord Region [^long regionkey name comment])

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
           (long (.getInteger c b))
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
                                 (= "null" tpch) nil
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

(def sf-001 (delay (all-tables 0.01)))
(def sf-005 (delay (all-tables 0.05)))
(def sf-01 (delay (all-tables 0.1)))
(def sf-1 (delay (all-tables 1.0)))

(defn q1 [{:keys [lineitem]}]

  #_
  (->> linetem
       (filter (fn [{:keys [shipdate]}] (<= (compare shipdate #inst "1998-09-02") 0)))
       (group-by (juxt :returnflag :linestatus))
       (sort-by key)
       (map (fn [[returnflag linestatus] lineitems]
              (c/tuple :l_returnflag returnflag
                       :l_linestatus linestatus
                       :sum_qty (reduce + (map :quantity lineitems))
                       :sum_base_price (reduce + (map :extendedprice lineitems))
                       :sum_disc_price (reduce + (map (fn [{:keys [extendedprice, discount]}] (* extendedprice (- 1.0 discount))) lineitems))
                       :sum_charge (reduce + (map (fn [{:keys [extendedprice, discount, tax]}] (* extendedprice (- 1.0 discount) (+ 1.0 tax))) lineitems))
                       :avg_qty (/ (reduce + (map :quantity lineitems)) (max 1 (count lineitems)))
                       :avg_price (/ (reduce + (map :extendedprice lineitems)) (max 1 (count lineitems)))
                       :avg_disc (/ (reduce + (map :discount lineitems)) (max 1 (count lineitems)))
                       :count_order (count lineitems)))))

  (q [^Lineitem l lineitem
      :when (<= l:shipdate #inst "1998-09-02")
      :group [returnflag l:returnflag, linestatus l:linestatus]
      :order [returnflag :asc, linestatus :asc]]
    (c/tuple :l_returnflag returnflag
             :l_linestatus linestatus
             :sum_qty (c/sum l:quantity)
             :sum_base_price (c/sum l:extendedprice)
             :sum_disc_price (c/sum (* l:extendedprice (- 1.0 l:discount)))
             :sum_charge (c/sum (* l:extendedprice (- 1.0 l:discount) (+ 1.0 l:tax)))
             :avg_qty (c/avg l:quantity)
             :avg_price (c/avg l:extendedprice)
             :avg_disc (c/avg l:discount)
             :count_order (c/count))))

(deftest q1-test (check-answer #'q1 @sf-001))

(comment
  (map vec (q1 @sf-001))
  (check-answer #'q1 @sf-001)
  ;; 0.05 70.801619 ms, sf1 1691ms
  (criterium.core/bench (count (q1 @sf-005)))
  ;; 0.05 21.762265 ms, sf1 574.529417ms
  (criterium.core/bench (iter-count (q1-rm @sf-005)))

  )

(defn q2 [{:keys [part, supplier, partsupp, nation, region]}]
  (q [^Region r region
      ^Nation n nation
      ^Supplier s supplier
      ^Part p part
      ^Partsupp ps partsupp
      :when
      (and
        (= p:partkey ps:partkey)
        (= s:suppkey ps:suppkey)
        (= p:size 15)
        (str/ends-with? p:type "BRASS")
        (= s:nationkey n:nationkey)
        (= n:regionkey r:regionkey)
        (= r:name "EUROPE")
        (= ps:supplycost (c/scalar [^Partsupp ps partsupp
                                    ^Supplier s supplier
                                    ^Nation n nation
                                    ^Region r region
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

(comment
  (time (count (q2 @sf-001)))
  (check-answer #'q2 @sf-001)

  )

(deftest q2-test (check-answer #'q2 @sf-001))

;; nested relational calculus

(defn q3 [{:keys [customer orders lineitem]}]
  (q [^Customer c customer
      ^Order o orders
      ^Lineitem l lineitem
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

(comment
  (time (count (q3 @sf-001)))
  (check-answer #'q3 @sf-001)
  )

(deftest q3-test (check-answer #'q3 @sf-001))

(defn q4 [{:keys [orders, lineitem]}]
  ;; needs decor + semijoin
  (q [^Order o orders
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

(comment

  (time (count (q4 @sf-001)))
  (check-answer #'q4 @sf-001)

  )

(deftest q4-test (check-answer #'q4 @sf-001))

(defn q5 [{:keys [customer, orders, lineitem, supplier, nation, region]}]
  (q [^Order o orders
      ^Customer c customer
      ^Lineitem l lineitem
      ^Supplier s supplier
      ^Region r region
      ^Nation n nation
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

(deftest q5-test (check-answer #'q5 @sf-001))

(comment

  (time (count (q5 @sf-005)))
  (check-answer #'q5 @sf-001)

  )

(defn q6 [{:keys [lineitem]}]
  (q [^Lineitem l lineitem
      :when (and (>= l:shipdate #inst "1994-01-01")
                 (< l:shipdate #inst "1995-01-01")
                 (>= l:discount 0.05)
                 (<= l:discount 0.07)
                 (< l:quantity 24.0))
      :group []]
    (c/tuple :foo (c/sum (* l:extendedprice l:discount)))))

(deftest q6-test (check-answer #'q6 @sf-001))

(comment

  (time (count (q6 @sf-005)))
  (check-answer #'q6 @sf-001)

  )

(defn get-year [^Date date]
  (+ 1900 (.getYear date)))

(defn q7 [{:keys [supplier lineitem orders customer nation]}]
  (q [^Supplier s supplier
      ^Lineitem l lineitem
      ^Order o orders
      ^Customer c customer
      ^Nation n1 nation
      ^Nation n2 nation
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

(deftest q7-test (check-answer #'q7 @sf-001))

(comment
  (time (count (q7 @sf-005)))
  (q7 @sf-001)
  (check-answer #'q7 @sf-001)
  )

(defn q8 [{:keys [part supplier region lineitem orders customer nation]}]
  (q [^Region r region
      ^Part p part
      ^Supplier s supplier
      ^Lineitem l lineitem
      ^Order o orders
      ^Customer c customer
      ^Nation n1 nation
      ^Nation n2 nation
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

(deftest q8-test (check-answer #'q8 @sf-001))

(comment
  (time (count (q8 @sf-005)))
  (q8 @sf-001)
  (check-answer #'q8 @sf-001)
  )

(defn q9 [{:keys [part supplier lineitem partsupp orders nation]}]
  (q [^Part p part
      ^Lineitem l lineitem
      ^Supplier s supplier
      ^Partsupp ps partsupp
      ^Nation n nation
      ^Order o orders
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

(deftest q9-test (check-answer #'q9 @sf-001))

(comment
  (time (count (q9 @sf-005)))
  (q9 @sf-001)
  (check-answer #'q9 @sf-001)
  )

(defn q10 [{:keys [lineitem customer orders nation]}]
  (q [^Nation n nation
      ^Customer c customer
      ^Order o orders
      ^Lineitem l lineitem
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

(deftest q10-test (check-answer #'q10 @sf-001))

(comment
  (time (count (q10 @sf-005)))
  (q10 @sf-001)
  (check-answer #'q10 @sf-001)
  )

(defn q11 [{:keys [partsupp supplier nation]}]
  (q [^Nation n nation
      ^Supplier s supplier
      ^Partsupp ps partsupp
      :when
      (and (= ps:suppkey s:suppkey)
           (= s:nationkey n:nationkey)
           (= n:name "GERMANY"))
      :group [ps_partkey ps:partkey]
      :let [value (c/sum (* ps:supplycost ps:availqty))]
      :when (> value (c/scalar [^Partsupp ps partsupp
                                ^Supplier s supplier
                                ^Nation n nation
                                :when
                                (and (= ps:suppkey s:suppkey)
                                     (= s:nationkey n:nationkey)
                                     (= n:name "GERMANY"))
                                :group []]
                       (* 0.0001 (c/sum (* ps:supplycost ps:availqty)))))
      :order [value :desc]]
    (c/tuple :ps_partkey ps_partkey
             :value value)))

(deftest q11-test (check-answer #'q11 @sf-001))

(comment
  (time (count (q11 @sf-005)))
  (q11 @sf-001)
  (check-answer #'q11 @sf-001)
  )

(defn q12 [{:keys [orders lineitem]}]
  (q [^Order o orders
      ^Lineitem l lineitem
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

(deftest q12-test (check-answer #'q12 @sf-001))

(comment
  (time (count (q12 @sf-005)))
  (q12 @sf-001)
  (check-answer #'q12 @sf-001)
  )

(defn q13 [{:keys [customer orders]}]
  (q [^Customer c customer
      :left-join [o orders (and (= c:custkey o:custkey)
                                (not (re-find #"special.*?requests" o:comment)))]
      :group [custkey c:custkey]
      :group [order-count (c/count o:orderkey)]
      :order [(c/count) :desc, order-count :desc]]
    (c/tuple :c_count order-count, :custdist (c/count))))

(deftest q13-test (check-answer #'q13 @sf-001))

(comment
  (time (count (q13 @sf-005)))
  (q13 @sf-001)
  (check-answer #'q13 @sf-001)
  )

(defn q14 [{:keys [lineitem part]}]
  (q [^Lineitem l lineitem
      ^Part p part
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

(deftest q14-test (check-answer #'q14 @sf-001))

(comment
  (time (count (q14 @sf-005)))
  (q14 @sf-001)
  (check-answer #'q14 @sf-001)
  )

(defn q15 [{:keys [lineitem supplier]}]
  (let [revenue (q [l lineitem
                    :when (and (>= l:shipdate #inst "1996-01-01")
                               (< l:shipdate #inst "1996-04-01"))
                    :group [suppkey l:suppkey]
                    :let [total_revenue (c/sum (* l:extendedprice (- 1.0 l:discount)))]]
                  {:suppkey suppkey
                   :total_revenue total_revenue})]
    (q [^Supplier s supplier
        r revenue
        :when (and (= s:suppkey r:suppkey)
                   (= r:total_revenue (c/scalar [r revenue :group []] (c/max r:total_revenue))))
        :order [s:suppkey :asc]]
      (c/tuple :s_suppkey s:suppkey
               :s_name s:name
               :s_address s:address
               :s_phone s:phone
               :total_revenue r:total_revenue))))

(deftest q15-test (check-answer #'q15 @sf-001))

(comment
  (time (count (q15 @sf-005)))
  (q15 @sf-001)
  (check-answer #'q15 @sf-001)
  )

(defn q16 [{:keys [partsupp part supplier]}]
  (q [^Part p part
      ^Partsupp ps partsupp
      :when (and (= p:partkey ps:partkey)
                 (not= p:brand "Brand#45")
                 (not (str/starts-with? p:type "MEDIUM POLISHED"))
                 (contains? #{49, 14, 23, 45, 19, 3, 36, 9} p:size)
                 (not (contains? (c/scalar [^Supplier s supplier
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

(deftest q16-test (check-answer #'q16 @sf-001))

(comment
  (time (count (q16 @sf-005)))
  (q16 @sf-001)
  (check-answer #'q16 @sf-001)
  )

(defn q17 [{:keys [lineitem part]}]
  (q [^Part p part
      ^Lineitem l lineitem
      :when (and (= p:partkey l:partkey)
                 (= p:brand "Brand#23")
                 (= p:container "MED BOX")
                 (< l:quantity
                    (c/scalar [^Lineitem l2 lineitem
                               :when (= l2:partkey p:partkey)
                               :group []]
                      (* 0.2 (c/avg l2:quantity)))))
      :group []]
    (c/tuple :avg_yearly (when (pos? %count) (/ (c/sum l:extendedprice) 7.0)))))

(deftest q17-test (check-answer #'q17 @sf-001))

(comment
  (time (count (q17 @sf-005)))
  (q17 @sf-001)
  (check-answer #'q17 @sf-001)
  )

(defn q18 [{:keys [customer orders lineitem]}]
  ;; TODO make this a sub query without weirdness
  (let [orderkeys (set (q [^Lineitem l lineitem
                           :group [ok l:orderkey]
                           :when (> (c/sum l:quantity) 300)]
                         ok))]
    (q [^Order o orders
        ^Customer c customer
        ^Lineitem l lineitem
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

(deftest q18-test (check-answer #'q18 @sf-001))

(comment
  (time (count (q18 @sf-005)))
  (q18 @sf-001)
  (check-answer #'q18 @sf-001)
  )

;; allow this with the l:partkey p:partkey condition in the OR as god intended

(defn q19 [{:keys [lineitem part]}]
  (q [^Lineitem l lineitem
      ^Part p part
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

(deftest q19-test (check-answer #'q19 @sf-001))

(comment
  (time (count (q19 @sf-005)))
  (q19 @sf-001)
  (check-answer #'q19 @sf-001)
  )

(defn q20 [{:keys [supplier, nation, partsupp, lineitem, part]}]
  (q [^Nation n nation
      ^Supplier s supplier
      :when (and (contains? (c/scalar [^Partsupp ps partsupp
                                       :when (and (contains? (c/scalar [^Part p part
                                                                        :when (str/starts-with? p:name "forest")
                                                                        :group []]
                                                               (set p:partkey))
                                                             ps:partkey)
                                                  (> ps:availqty (c/scalar [^Lineitem l lineitem
                                                                            :when (and (= l:partkey ps:partkey)
                                                                                       (= l:suppkey ps:suppkey)
                                                                                       (>= l:shipdate #inst "1994-01-01")
                                                                                       (< l:shipdate #inst "1995-01-01"))
                                                                            :group []]
                                                                   ;; this query can return nil, the left join
                                                                   (* 0.5 (c/sum l:quantity)))))
                                       :group []]
                              (set ps:suppkey))
                            s:suppkey)
                 (= s:nationkey n:nationkey)
                 (= n:name "CANADA"))
      :order [s:name :asc]]
    (c/tuple :name s:name, :address s:address)))

(deftest q20-test (check-answer #'q20 @sf-001))

(comment
  (time (count (q20 @sf-005)))
  (q20 @sf-001)
  (check-answer #'q20 @sf-001)
  )

(defn q21 [{:keys [supplier lineitem orders nation]}]
  (q [^Nation n nation
      ^Supplier s supplier
      ^Order o orders
      ^Lineitem l1 lineitem
      :when (and (= s:suppkey l1:suppkey)
                 (= o:orderkey l1:orderkey)
                 (= o:orderstatus "F")
                 (> l1:receiptdate l1:commitdate)
                 (c/exists?
                   [^Lineitem l2 lineitem
                    :when (and (= l2:orderkey l1:orderkey)
                               (not= l2:suppkey l1:suppkey))])
                 (not (c/exists?
                        [^Lineitem l3 lineitem
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

(deftest q21-test (check-answer #'q21 @sf-001))

(comment
  (time (count (q21 @sf-005)))
  (q21 @sf-001)

  (check-answer #'q21 @sf-001)
  )

(defn q22 [{:keys [customer orders]}]
  (q [^Customer c customer
      :let [cntrycode (subs c:phone 0 2)]
      :when (and (contains? #{"13", "31", "23", "29", "30", "18", "17"} cntrycode)
                 (> c:acctbal (c/scalar [^Customer c customer
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

(deftest q22-test (check-answer #'q22 @sf-001))

(comment
  (time (count (q22 @sf-005)))
  (q22 @sf-001)
  (check-answer #'q22 @sf-001)
  )

(comment

  (run-tests 'com.wotbrew.cinq.tpch-test)

  ((requiring-resolve 'clj-async-profiler.core/serve-ui) "localhost" 5000)
  ((requiring-resolve 'clojure.java.browse/browse-url) "http://127.0.0.1:5000")

  (require 'criterium.core)

  (System/gc)

  (def dataset @sf-001)
  (def dataset @sf-005)
  (def dataset @sf-01)
  (def dataset @sf-1)

  (update-vals dataset count)
  (type (first (:lineitem dataset)))

  (System/gc)

  (defn run-tpch []
    (let [timings
          (vec (for [q [#'q1, #'q2, #'q3,
                        #'q4, #'q5, #'q6,
                        #'q7, #'q8, #'q9
                        #'q10, #'q11, #'q12,
                        #'q13, #'q14, #'q15,
                        #'q16 #'q17, #'q18
                        #'q19, #'q20, #'q21
                        #'q22]]
                 (let [start-ns (System/nanoTime)
                       _ (count (q dataset))
                       end-ns (System/nanoTime)]
                   [(name (.toSymbol q)) (* 1e-6 (- end-ns start-ns))])))]
      {:timings timings
       :total (reduce + 0.0 (map second timings))}))

  (require 'criterium.core)
  (criterium.core/quick-bench
   (run-tpch)
   )

  ;; 0.05 graal array-seq 639ms
  ;; 1.0 graal array-seq 15835ms
  ;; 0.05 graal eager-loop 196ms / 188ms group fusion
  ;; 1.0 graal eager-loop 3841ms
  (clj-async-profiler.core/profile
    (criterium.core/quick-bench
      (run-tpch)
      )
    )


  (clj-async-profiler.core/profile (run-tpch))
  (time (dotimes [x 1] (count (q1 dataset))))
  (time (dotimes [x 1] (count (q1-el dataset))))
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
  (time (dotimes [x 1] (count (q12 dataset))))
  (time (dotimes [x 1] (count (q13 dataset))))
  (time (dotimes [x 1] (count (q14 dataset))))
  (time (dotimes [x 1] (count (q15 dataset))))
  (time (dotimes [x 1] (count (q16 dataset))))
  (time (dotimes [x 1] (count (q17 dataset))))
  (time (dotimes [x 1] (count (q18 dataset))))
  (time (dotimes [x 1] (count (q19 dataset))))
  (time (dotimes [x 1] (count (q20 dataset))))
  (time (dotimes [x 1] (count (q21 dataset))))
  (time (dotimes [x 1] (count (q22 dataset))))

  (clj-async-profiler.core/profile
    {}
    (dotimes [x 20] (count (q1 dataset))))

  (clj-async-profiler.core/profile
    {}
    (dotimes [x 10] (count (q13 @sf-005))))

  (time (dotimes [x 20] (count (q1-el dataset))))
  (time (dotimes [x 100] (count (q2 dataset))))
  (time (dotimes [x 50] (count (q3 dataset))))

  )

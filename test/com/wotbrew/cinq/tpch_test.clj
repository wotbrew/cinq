(ns com.wotbrew.cinq.tpch-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq.column :as col]
            [com.wotbrew.cinq.vector-seq :refer [q]]
            [meander.epsilon :as m])
  (:import (java.util Spliterator)
           (java.util.function Consumer)))

#_{:q [[:from :lineitem]
       [:where [<= :l_shipdate #inst "1998-09-02"]]
       [:agg
        [:l_returnflag
         :l_linestatus]
        [:sum_qty [rel/sum :l_quantity]]
        [:sum_base_price [rel/sum :l_extendedprice]]
        [:sum_disc_price [rel/sum [* :l_extendedprice [- 1 :l_discount]]]]
        [:sum_charge [rel/sum [* :l_extendedprice [- 1 :l_discount] [+ 1 :l_tax]]]]
        [:avg_qty [rel/avg :l_quantity]]
        [:avg_price [rel/avg :l_extendedprice]]
        [:avg_disc [rel/avg :l_discount]]
        [:count_order count]]
       [:sort [:l_returnflag] [:l_linestatus]]]

   :cols
   [:l_returnflag
    :l_linestatus
    :sum_qty
    :sum_base_price
    :sum_disc_price
    :sum_charge
    :avg_qty
    :avg_price
    :avg_disc
    :count_order]}


(def q1
  {:desc "Q1: Pricing summary report query"

   :q (fn [{:keys [lineitem]}]

        (q [l lineitem
            :where (<= (compare l:shipdate #inst "1998-09-02") 0)
            :group-by [returnflag l:returnflag, linestatus l:linestatus]
            :order-by [returnflag :asc, linestatus :asc]
            :select
            [:returnflag returnflag
             :linestatus linestatus
             :sum_qty (col/sum l:quantity)
             :sum_base_price (col/sum l:extendedprice)
             :sum_disc_price (col/sum [ep l:extendedprice, d l:discount] (* ep (- 1 d)))
             :sum_charge (col/sum [ep l:extendedprice, d l:discount, tax l:tax] (* ep (- 1 d) (inc tax)))
             :avg_qty (col/avg l:quantity)
             :avg_price (col/avg l:extendedprice)
             :avg_disc (col/avg l:discount)
             :count_order (count l)]])

        )

   ;; CAPS = broadcast, find free variables andb v= broadcast the expression over them
   ;;

   #_#_:result (parse-result-file (io/resource "io/airlift/tpch/queries/q1.result"))})


'[[:from :part]

  [:where
   [= :p_size 15]
   [re-find #"^.*BRASS$" :p_type]]

  [:join
   [[:from :partsupp]
    [:join
     :supplier {:ps_suppkey :s_suppkey}
     :nation {:s_nationkey :n_nationkey}
     :region {:n_regionkey :r_regionkey}]
    [:where [= "EUROPE" :r_name]]
    [:agg
     [:ps_partkey :r_regionkey]
     [:min_supplier [rel/min-by :ps_supplycost]]]
    [:select
     :ps_partkey
     :r_regionkey
     [[:ps_suppkey] :min_supplier]]]
   {:p_partkey :ps_partkey}

   :supplier {:ps_suppkey :s_suppkey}
   :nation {:s_nationkey :n_nationkey}
   :region {:n_regionkey :r_regionkey}]

  [:select
   :s_acctbal
   :s_name
   :n_name
   :p_partkey
   :p_mfgr
   :s_address
   :s_phone
   :s_comment]
  [:sort

   [:s_acctbal :desc]
   [:n_name]
   [:s_name]
   [:p_partkey]]]

'[p part
  s supplier
  ps partsupp
  n nation
  r region
  :where
  (and
    (= p:partkey ps:partkey)
    (= s:suppkey ps:suppkey)
    (= p:size 15)
    (re-find #"^.*BRASS$" p:type)
    (= s:nationkey n:nationkey)
    (= n:nationkey r:regionkey)
    ;; S for scalar sub query, C for column sub query, Q for relation sub query
    (= ps:supplycost (S [ps partsupp
                         s supplier
                         n nation
                         r region
                         ;; these lookup syms shadow the outer ones
                         ;; not sure if this is a problem, it will cause redundant scan cols
                         ;; could think of lookup syms as (maybe we'll need it)
                         ;; if the lookup columns are never projected (with select) they can be removed
                         ;; distinct/difference etc might only work if explicit projection set
                         :where (and (= p:partkey ps:partkey)
                                     (= s:suppkey ps:suppkey)
                                     (= s:nationkey n:nationkey)
                                     (= "EUROPE" r:name))
                         :group-by []
                         :return (col/min ps:supplycost)])))
  :order-by [s:acctbal :desc
             n:name :asc
             s:name :asc
             p:partkey :asc]
  :limit 100]

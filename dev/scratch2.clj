(ns scratch2)

(defn compare-timings [{timings-a :timings}
                       {timings-b :timings}]
  (->> (map (fn [[q time-a] [_ time-b]]
              [q (- time-a time-b)])
            timings-a timings-b)))

(compare-timings

  ;; el.clj

  {:timings [["q1" 34.923833]
             ["q2" 6.2375]
             ["q3" 10.29575]
             ["q4" 12.787917]
             ["q5" 28.664792]
             ["q6" 6.2121249999999995]
             ["q7" 31.681458]
             ["q8" 14.057457999999999]
             ["q9" 60.002292]
             ["q10" 17.734666]
             ["q11" 6.259708]
             ["q12" 14.104624999999999]
             ["q13" 37.574791]
             ["q14" 7.250249999999999]
             ["q15" 5.566458]
             ["q16" 10.771167]
             ["q17" 31.298040999999998]
             ["q18" 33.333458]
             ["q19" 33.535167]
             ["q20" 13.423290999999999]
             ["q21" 56.039790999999994]
             ["q22" 4.755959]],
   :total 476.51049700000004}

  ;; aseq.clj
  {:timings [["q1" 79.170459]
             ["q2" 9.953375]
             ["q3" 17.617459]
             ["q4" 22.69275]
             ["q5" 43.402499999999996]
             ["q6" 6.583125]
             ["q7" 55.252209]
             ["q8" 36.548]
             ["q9" 71.290917]
             ["q10" 13.366582999999999]
             ["q11" 7.25575]
             ["q12" 13.858417]
             ["q13" 44.457875]
             ["q14" 6.31375]
             ["q15" 4.625208]
             ["q16" 13.314458]
             ["q17" 43.362375]
             ["q18" 50.490542]
             ["q19" 60.924875]
             ["q20" 13.418292]
             ["q21" 66.156125]
             ["q22" 5.231833]],
   :total 685.2868769999999})

(def supplier (:supplier @com.wotbrew.cinq.tpch-test/sf-001))
(def partsupp (:partsupp @com.wotbrew.cinq.tpch-test/sf-001))
(def part (:part @com.wotbrew.cinq.tpch-test/sf-001))
(def lineitem (:lineitem @com.wotbrew.cinq.tpch-test/sf-001))


(defrecord Customer [custkey name address nationkey phone acctbal mktsegment comment])
(defrecord Order [orderkey custkey orderstatus totalprice orderdate orderpriority clerk shippriority comment])
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
(defrecord Part [partkey name mfgr brand type size container retailprice comment])
(defrecord Partsupp [partkey suppkey availqty supplycost comment])
(defrecord Supplier [suppkey name address nationkey phone acctbal comment])
(defrecord Nation [nationkey name regionkey comment])
(defrecord Region [regionkey name comment])


(set! *warn-on-reflection* true)

(defn foo []
  (clojure.core/vec
    (clojure.core/let
      [list228548 (java.util.ArrayList.)]
      (clojure.core/let
        [rs__208119__auto__
         (.toArray
           (clojure.core/let
             [list228570 (java.util.ArrayList.)]
             (clojure.core/let
               [rs__208083__auto__
                (clojure.core/let
                  [list228589 (java.util.ArrayList.)]
                  (clojure.core/run!
                    (clojure.core/fn
                      scan-fn__208004__auto__
                      [^Lineitem o228590]
                      (clojure.core/let
                        [l228545
                         ^Lineitem o228590
                         l228545:extendedprice
                         (.-extendedprice ^Lineitem o228590)
                         l228545:discount
                         (.-discount ^Lineitem o228590)
                         l228545:quantity
                         (.-quantity ^Lineitem o228590)
                         l228545:linestatus
                         (.-linestatus ^Lineitem o228590)
                         l228545:shipdate
                         (.-shipdate ^Lineitem o228590)
                         l228545:tax
                         (.-tax ^Lineitem o228590)
                         l228545:returnflag
                         (.-returnflag ^Lineitem o228590)]
                        (clojure.core/when
                          (clojure.core/<=
                            (clojure.core/compare
                              l228545:shipdate
                              #inst "1998-09-02T00:00:00.000-00:00")
                            0)
                          (.add
                            list228589
                            (new
                              com.wotbrew.cinq.tpch_test.Tuple228572
                              ^com.wotbrew.cinq.tpch_test.Lineitem l228545
                              ^double l228545:extendedprice
                              ^double l228545:discount
                              ^double l228545:quantity
                              ^java.lang.Object l228545:linestatus
                              ^java.lang.Object l228545:shipdate
                              ^double l228545:tax
                              ^java.lang.Object l228545:returnflag)))))
                    lineitem)
                  list228589)
                ht__208084__auto__
                (java.util.HashMap.)]
               (clojure.core/run!
                 (clojure.core/fn
                   [^com.wotbrew.cinq.tpch_test.Tuple228572 t228571]
                   (clojure.core/let
                     [l228545:linestatus
                      (.-field4 ^com.wotbrew.cinq.tpch_test.Tuple228572 t228571)
                      l228545:returnflag
                      (.-field7 ^com.wotbrew.cinq.tpch_test.Tuple228572 t228571)
                      ^com.wotbrew.cinq.tpch_test.Tuple208764 k228587
                      (new
                        com.wotbrew.cinq.tpch_test.Tuple208764
                        l228545:returnflag
                        l228545:linestatus)
                      f__208085__auto__
                      (clojure.core/reify
                        java.util.function.BiFunction
                        (clojure.core/apply
                          [___208086__auto__ ___208086__auto__ al228588]
                          (clojure.core/let
                            [^java.util.ArrayList al228588
                             (clojure.core/or
                               ^java.util.ArrayList al228588
                               (java.util.ArrayList.))]
                            (.add
                              ^java.util.ArrayList al228588
                              ^com.wotbrew.cinq.tpch_test.Tuple228572 t228571)
                            ^java.util.ArrayList al228588)))]
                     (.compute
                       ht__208084__auto__
                       ^com.wotbrew.cinq.tpch_test.Tuple208764 k228587
                       f__208085__auto__)))
                 rs__208083__auto__)
               (clojure.core/run!
                 (clojure.core/fn
                   [[^com.wotbrew.cinq.tpch_test.Tuple208764 k228587
                     ^java.util.ArrayList al228588]]
                   (clojure.core/let
                     [l228545
                      (com.wotbrew.cinq.column/->Column
                        nil
                        (clojure.core/fn
                          []
                          (clojure.core/let
                            [arr__208075__auto__
                             (clojure.core/object-array
                               (.size ^java.util.ArrayList al228588))]
                            (clojure.core/dotimes
                              [j__208076__auto__
                               (.size ^java.util.ArrayList al228588)]
                              (clojure.core/let
                                [^com.wotbrew.cinq.tpch_test.Tuple228572 t228571
                                 (.get
                                   ^java.util.ArrayList al228588
                                   j__208076__auto__)]
                                (clojure.core/aset
                                  arr__208075__auto__
                                  j__208076__auto__
                                  (clojure.lang.RT/box
                                    (.-field0
                                      ^com.wotbrew.cinq.tpch_test.Tuple228572 t228571)))))
                            arr__208075__auto__)))
                      l228545:extendedprice
                      (com.wotbrew.cinq.column/->DoubleColumn
                        nil
                        (clojure.core/fn
                          []
                          (clojure.core/let
                            [arr__208075__auto__
                             (clojure.core/double-array
                               (.size ^java.util.ArrayList al228588))]
                            (clojure.core/dotimes
                              [j__208076__auto__
                               (.size ^java.util.ArrayList al228588)]
                              (clojure.core/let
                                [^com.wotbrew.cinq.tpch_test.Tuple228572 t228571
                                 (.get
                                   ^java.util.ArrayList al228588
                                   j__208076__auto__)]
                                (clojure.core/aset
                                  arr__208075__auto__
                                  j__208076__auto__
                                  (clojure.core/double
                                    (.-field1
                                      ^com.wotbrew.cinq.tpch_test.Tuple228572 t228571)))))
                            arr__208075__auto__)))
                      l228545:discount
                      (com.wotbrew.cinq.column/->DoubleColumn
                        nil
                        (clojure.core/fn
                          []
                          (clojure.core/let
                            [arr__208075__auto__
                             (clojure.core/double-array
                               (.size ^java.util.ArrayList al228588))]
                            (clojure.core/dotimes
                              [j__208076__auto__
                               (.size ^java.util.ArrayList al228588)]
                              (clojure.core/let
                                [^com.wotbrew.cinq.tpch_test.Tuple228572 t228571
                                 (.get
                                   ^java.util.ArrayList al228588
                                   j__208076__auto__)]
                                (clojure.core/aset
                                  arr__208075__auto__
                                  j__208076__auto__
                                  (clojure.core/double
                                    (.-field2
                                      ^com.wotbrew.cinq.tpch_test.Tuple228572 t228571)))))
                            arr__208075__auto__)))
                      l228545:quantity
                      (com.wotbrew.cinq.column/->DoubleColumn
                        nil
                        (clojure.core/fn
                          []
                          (clojure.core/let
                            [arr__208075__auto__
                             (clojure.core/double-array
                               (.size ^java.util.ArrayList al228588))]
                            (clojure.core/dotimes
                              [j__208076__auto__
                               (.size ^java.util.ArrayList al228588)]
                              (clojure.core/let
                                [^com.wotbrew.cinq.tpch_test.Tuple228572 t228571
                                 (.get
                                   ^java.util.ArrayList al228588
                                   j__208076__auto__)]
                                (clojure.core/aset
                                  arr__208075__auto__
                                  j__208076__auto__
                                  (clojure.core/double
                                    (.-field3
                                      ^com.wotbrew.cinq.tpch_test.Tuple228572 t228571)))))
                            arr__208075__auto__)))
                      l228545:linestatus
                      (com.wotbrew.cinq.column/->Column
                        nil
                        (clojure.core/fn
                          []
                          (clojure.core/let
                            [arr__208075__auto__
                             (clojure.core/object-array
                               (.size ^java.util.ArrayList al228588))]
                            (clojure.core/dotimes
                              [j__208076__auto__
                               (.size ^java.util.ArrayList al228588)]
                              (clojure.core/let
                                [^com.wotbrew.cinq.tpch_test.Tuple228572 t228571
                                 (.get
                                   ^java.util.ArrayList al228588
                                   j__208076__auto__)]
                                (clojure.core/aset
                                  arr__208075__auto__
                                  j__208076__auto__
                                  (clojure.lang.RT/box
                                    (.-field4
                                      ^com.wotbrew.cinq.tpch_test.Tuple228572 t228571)))))
                            arr__208075__auto__)))
                      l228545:shipdate
                      (com.wotbrew.cinq.column/->Column
                        nil
                        (clojure.core/fn
                          []
                          (clojure.core/let
                            [arr__208075__auto__
                             (clojure.core/object-array
                               (.size ^java.util.ArrayList al228588))]
                            (clojure.core/dotimes
                              [j__208076__auto__
                               (.size ^java.util.ArrayList al228588)]
                              (clojure.core/let
                                [^com.wotbrew.cinq.tpch_test.Tuple228572 t228571
                                 (.get
                                   ^java.util.ArrayList al228588
                                   j__208076__auto__)]
                                (clojure.core/aset
                                  arr__208075__auto__
                                  j__208076__auto__
                                  (clojure.lang.RT/box
                                    (.-field5
                                      ^com.wotbrew.cinq.tpch_test.Tuple228572 t228571)))))
                            arr__208075__auto__)))
                      l228545:tax
                      (com.wotbrew.cinq.column/->DoubleColumn
                        nil
                        (clojure.core/fn
                          []
                          (clojure.core/let
                            [arr__208075__auto__
                             (clojure.core/double-array
                               (.size ^java.util.ArrayList al228588))]
                            (clojure.core/dotimes
                              [j__208076__auto__
                               (.size ^java.util.ArrayList al228588)]
                              (clojure.core/let
                                [^com.wotbrew.cinq.tpch_test.Tuple228572 t228571
                                 (.get
                                   ^java.util.ArrayList al228588
                                   j__208076__auto__)]
                                (clojure.core/aset
                                  arr__208075__auto__
                                  j__208076__auto__
                                  (clojure.core/double
                                    (.-field6
                                      ^com.wotbrew.cinq.tpch_test.Tuple228572 t228571)))))
                            arr__208075__auto__)))
                      l228545:returnflag
                      (com.wotbrew.cinq.column/->Column
                        nil
                        (clojure.core/fn
                          []
                          (clojure.core/let
                            [arr__208075__auto__
                             (clojure.core/object-array
                               (.size ^java.util.ArrayList al228588))]
                            (clojure.core/dotimes
                              [j__208076__auto__
                               (.size ^java.util.ArrayList al228588)]
                              (clojure.core/let
                                [^com.wotbrew.cinq.tpch_test.Tuple228572 t228571
                                 (.get
                                   ^java.util.ArrayList al228588
                                   j__208076__auto__)]
                                (clojure.core/aset
                                  arr__208075__auto__
                                  j__208076__auto__
                                  (clojure.lang.RT/box
                                    (.-field7
                                      ^com.wotbrew.cinq.tpch_test.Tuple228572 t228571)))))
                            arr__208075__auto__)))
                      returnflag228546
                      (.-field0 ^com.wotbrew.cinq.tpch_test.Tuple208764 k228587)
                      linestatus228547
                      (.-field1 ^com.wotbrew.cinq.tpch_test.Tuple208764 k228587)
                      %count
                      (.size ^java.util.ArrayList al228588)]
                     (.add
                       list228570
                       (new
                         com.wotbrew.cinq.tpch_test.Tuple228550
                         ^com.wotbrew.cinq.column.Column l228545
                         ^com.wotbrew.cinq.column.DoubleColumn l228545:extendedprice
                         ^com.wotbrew.cinq.column.DoubleColumn l228545:discount
                         ^com.wotbrew.cinq.column.DoubleColumn l228545:quantity
                         ^com.wotbrew.cinq.column.Column l228545:linestatus
                         ^com.wotbrew.cinq.column.Column l228545:shipdate
                         ^com.wotbrew.cinq.column.DoubleColumn l228545:tax
                         ^com.wotbrew.cinq.column.Column l228545:returnflag
                         returnflag228546
                         linestatus228547
                         %count))))
                 ht__208084__auto__))
             list228570))]
        (java.util.Arrays/sort
          rs__208119__auto__
          (clojure.core/reify
            java.util.Comparator
            (clojure.core/compare
              [___208116__auto__ a__208117__auto__ b__208118__auto__]
              (clojure.core/let
                [^com.wotbrew.cinq.tpch_test.Tuple208764 k228568
                 a__208117__auto__
                 ^com.wotbrew.cinq.tpch_test.Tuple208764 k228569
                 b__208118__auto__]
                (clojure.core/let
                  [res__208115__auto__
                   (clojure.core/compare
                     (clojure.core/let
                       [returnflag228546
                        (.-field8 ^com.wotbrew.cinq.tpch_test.Tuple208764 k228568)]
                       returnflag228546)
                     (clojure.core/let
                       [returnflag228546
                        (.-field8 ^com.wotbrew.cinq.tpch_test.Tuple208764 k228569)]
                       returnflag228546))]
                  (if
                    (clojure.core/= 0 res__208115__auto__)
                    (clojure.core/let
                      [res__208115__auto__
                       (clojure.core/compare
                         (clojure.core/let
                           [linestatus228547
                            (.-field9
                              ^com.wotbrew.cinq.tpch_test.Tuple208764 k228568)]
                           linestatus228547)
                         (clojure.core/let
                           [linestatus228547
                            (.-field9
                              ^com.wotbrew.cinq.tpch_test.Tuple208764 k228569)]
                           linestatus228547))]
                      (if
                        (clojure.core/= 0 res__208115__auto__)
                        0
                        (clojure.core/* res__208115__auto__ 1)))
                    (clojure.core/* res__208115__auto__ 1)))))))
        (clojure.core/dotimes
          [i__208120__auto__ (clojure.core/alength rs__208119__auto__)]
          (clojure.core/let
            [^com.wotbrew.cinq.tpch_test.Tuple228550 t228549
             (clojure.core/aget rs__208119__auto__ i__208120__auto__)
             l228545
             (.-field0 ^com.wotbrew.cinq.tpch_test.Tuple228550 t228549)
             l228545:extendedprice
             (.-field1 ^com.wotbrew.cinq.tpch_test.Tuple228550 t228549)
             l228545:discount
             (.-field2 ^com.wotbrew.cinq.tpch_test.Tuple228550 t228549)
             l228545:quantity
             (.-field3 ^com.wotbrew.cinq.tpch_test.Tuple228550 t228549)
             l228545:linestatus
             (.-field4 ^com.wotbrew.cinq.tpch_test.Tuple228550 t228549)
             l228545:shipdate
             (.-field5 ^com.wotbrew.cinq.tpch_test.Tuple228550 t228549)
             l228545:tax
             (.-field6 ^com.wotbrew.cinq.tpch_test.Tuple228550 t228549)
             l228545:returnflag
             (.-field7 ^com.wotbrew.cinq.tpch_test.Tuple228550 t228549)
             returnflag228546
             (.-field8 ^com.wotbrew.cinq.tpch_test.Tuple228550 t228549)
             linestatus228547
             (.-field9 ^com.wotbrew.cinq.tpch_test.Tuple228550 t228549)
             %count
             (.-field10 ^com.wotbrew.cinq.tpch_test.Tuple228550 t228549)]
            (clojure.core/let
              [col228544
               [returnflag228546
                linestatus228547
                (com.wotbrew.cinq.column/sum
                  [^com.wotbrew.cinq.column.DoubleColumn l228545:quantity
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:quantity]
                  l228545:quantity)
                (com.wotbrew.cinq.column/sum
                  [^com.wotbrew.cinq.column.DoubleColumn l228545:extendedprice
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:extendedprice]
                  l228545:extendedprice)
                (com.wotbrew.cinq.column/sum
                  [^com.wotbrew.cinq.column.DoubleColumn l228545:extendedprice
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:extendedprice
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:discount
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:discount]
                  (* l228545:extendedprice (- 1.0 l228545:discount)))
                (com.wotbrew.cinq.column/sum
                  [^com.wotbrew.cinq.column.DoubleColumn l228545:extendedprice
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:extendedprice
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:discount
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:discount
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:tax
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:tax]
                  (*
                    l228545:extendedprice
                    (- 1.0 l228545:discount)
                    (+ 1.0 l228545:tax)))
                (com.wotbrew.cinq.column/avg
                  [^com.wotbrew.cinq.column.DoubleColumn l228545:quantity
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:quantity]
                  l228545:quantity)
                (com.wotbrew.cinq.column/avg
                  [^com.wotbrew.cinq.column.DoubleColumn l228545:extendedprice
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:extendedprice]
                  l228545:extendedprice)
                (com.wotbrew.cinq.column/avg
                  [^com.wotbrew.cinq.column.DoubleColumn l228545:discount
                   ^com.wotbrew.cinq.column.DoubleColumn l228545:discount]
                  l228545:discount)
                %count]]
              (.add list228548 (clojure.lang.RT/box col228544))))))
      list228548)))

(ns scratch2)

(def lineitem (take 1 (:lineitem @com.wotbrew.cinq.tpch-test/sf-001)))

(reify*
  [clojure.lang.IReduceInit com.wotbrew.cinq.protocols.Scannable]
  (com.wotbrew.cinq.eager-loop/scan
    [this__11352__auto__ f__11353__auto__ init__11354__auto__]
    (clojure.core/let
      [rsn32450 (clojure.core/long-array 1)]
      (clojure.core/aset rsn32450 0 -1)
      (.reduce
        this__11352__auto__
        (clojure.core/fn
          [acc__11355__auto__ x__11356__auto__]
          (f__11353__auto__
            acc__11355__auto__
            nil
            (clojure.core/aset rsn32450 0 (clojure.core/unchecked-inc (clojure.core/aget rsn32450 0)))
            x__11356__auto__))
        init__11354__auto__)))
  (clojure.core/reduce
    [com.wotbrew.cinq.eager-loop/_ f32451 init__11354__auto__]
    (clojure.core/let
      [box32449 (clojure.core/object-array 1)]
      (clojure.core/aset box32449 0 init__11354__auto__)
      ((clojure.core/fn
         [box32449]
         (clojure.core/let
           [rs__11697__auto__
            (.toArray
              (clojure.core/let
                [list32455 (java.util.ArrayList.)]
                (clojure.core/let
                  [rs__11430__auto__
                   (clojure.core/let
                     [list32473 (java.util.ArrayList.)]
                     (com.wotbrew.cinq.eager-loop/run-scan-no-rsn
                       (clojure.core/reify
                         clojure.lang.IFn
                         (com.wotbrew.cinq.eager-loop/invoke
                           [f__11198__auto__ agg__11199__auto__ rv__11200__auto__ rsn__11201__auto__ o__11202__auto__]
                           (if
                             (.filter f__11198__auto__ rsn__11201__auto__ o__11202__auto__)
                             (.apply f__11198__auto__ agg__11199__auto__ rv__11200__auto__ rsn__11201__auto__ o__11202__auto__)
                             agg__11199__auto__))
                         com.wotbrew.cinq.CinqScanFunction
                         (clojure.core/filter
                           [___11208__auto__ rsn32475 o__11209__auto__]
                           (clojure.core/let
                             [o32474 o__11209__auto__]
                             (clojure.core/let
                               [l32446:shipdate (.-shipdate o32474)]
                               (clojure.core/and
                                 (clojure.core/<=
                                   (com.wotbrew.cinq.CinqUtil/compare l32446:shipdate #inst"1998-09-02T00:00:00.000-00:00")
                                   0)
                                 true))))
                         (com.wotbrew.cinq.eager-loop/nativeFilter
                           [___11211__auto__ symbol-table32478]
                           (clojure.core/let
                             [l32446:shipdate32479
                              (com.wotbrew.cinq.nio-codec/encode-heap #inst"1998-09-02T00:00:00.000-00:00" symbol-table32478 false)
                              l32446:shipdate32480
                              (com.wotbrew.cinq.nio-codec/intern-symbol symbol-table32478 :shipdate false)]
                             (clojure.core/when
                               (clojure.core/and l32446:shipdate32479 l32446:shipdate32480)
                               (clojure.core/reify
                                 com.wotbrew.cinq.CinqScanFunction$NativeFilter
                                 (clojure.core/apply
                                   [___11211__auto__ rsn__11212__auto__ buf__11213__auto__]
                                   (clojure.core/let
                                     [valbuf32477 (.slice buf__11213__auto__)]
                                     (clojure.core/and
                                       (clojure.core/>=
                                         (com.wotbrew.cinq.nio-codec/bufcmp-ksv l32446:shipdate32479 valbuf32477 l32446:shipdate32480)
                                         0))))))))
                         (clojure.core/apply
                           [___11217__auto__ ___11217__auto__ rv32476 rsn32475 o__11218__auto__]
                           (clojure.core/let
                             [o32474 o__11218__auto__]
                             (clojure.core/let
                               [l32446:discount
                                (.-discount o32474)
                                l32446:extendedprice
                                (.-extendedprice o32474)
                                l32446:linestatus
                                (.-linestatus o32474)
                                l32446:quantity
                                (.-quantity o32474)
                                l32446:returnflag
                                (.-returnflag o32474)
                                l32446:tax
                                (.-tax o32474)
                                l32446:shipdate
                                (.-shipdate o32474)]
                               (clojure.core/some->
                                 (do
                                   (.add
                                     list32473
                                     (new
                                       com.wotbrew.cinq.tpch_test.Tuple32457
                                       false
                                       l32446:tax
                                       l32446:returnflag
                                       l32446:discount
                                       l32446:quantity
                                       l32446:linestatus
                                       l32446:extendedprice))
                                   nil)
                                 clojure.core/reduced))))
                         (com.wotbrew.cinq.eager-loop/rootDoesNotEscape [___11217__auto__] true))
                       lineitem)
                     list32473)
                   ht__11431__auto__
                   (java.util.HashMap. 64)]
                  (clojure.core/run!
                    (clojure.core/fn
                      [t32456]
                      (clojure.core/let
                        [l32446:returnflag
                         (.-field1 t32456)
                         l32446:linestatus
                         (.-field4 t32456)
                         k32471
                         (new com.wotbrew.cinq.tpch_test.Tuple19053 false l32446:returnflag l32446:linestatus)
                         f__11432__auto__
                         (clojure.core/reify
                           java.util.function.BiFunction
                           (clojure.core/apply
                             [___11433__auto__ ___11433__auto__ al32472]
                             (clojure.core/let
                               [al32472 (clojure.core/or al32472 (java.util.ArrayList.))]
                               (.add al32472 t32456)
                               al32472)))]
                        (.compute ht__11431__auto__ k32471 f__11432__auto__)))
                    rs__11430__auto__)
                  (clojure.core/reduce
                    (clojure.core/fn
                      [___11433__auto__ [k32471 al32472]]
                      (clojure.core/let
                        [l32446:tax
                         (com.wotbrew.cinq.column/->DoubleColumn
                           nil
                           (clojure.core/fn
                             []
                             (clojure.core/let
                               [arr__11394__auto__ (clojure.core/double-array (.size al32472))]
                               (clojure.core/dotimes
                                 [j__11395__auto__ (.size al32472)]
                                 (clojure.core/let
                                   [t32456 (.get al32472 j__11395__auto__)]
                                   (clojure.core/aset arr__11394__auto__ j__11395__auto__ (clojure.core/double (.-field0 t32456)))))
                               arr__11394__auto__)))
                         l32446:returnflag
                         (com.wotbrew.cinq.column/->Column
                           nil
                           (clojure.core/fn
                             []
                             (clojure.core/let
                               [arr__11394__auto__ (clojure.core/object-array (.size al32472))]
                               (clojure.core/dotimes
                                 [j__11395__auto__ (.size al32472)]
                                 (clojure.core/let
                                   [t32456 (.get al32472 j__11395__auto__)]
                                   (clojure.core/aset arr__11394__auto__ j__11395__auto__ (clojure.lang.RT/box (.-field1 t32456)))))
                               arr__11394__auto__)))
                         l32446:discount
                         (com.wotbrew.cinq.column/->DoubleColumn
                           nil
                           (clojure.core/fn
                             []
                             (clojure.core/let
                               [arr__11394__auto__ (clojure.core/double-array (.size al32472))]
                               (clojure.core/dotimes
                                 [j__11395__auto__ (.size al32472)]
                                 (clojure.core/let
                                   [t32456 (.get al32472 j__11395__auto__)]
                                   (clojure.core/aset arr__11394__auto__ j__11395__auto__ (clojure.core/double (.-field2 t32456)))))
                               arr__11394__auto__)))
                         l32446:quantity
                         (com.wotbrew.cinq.column/->DoubleColumn
                           nil
                           (clojure.core/fn
                             []
                             (clojure.core/let
                               [arr__11394__auto__ (clojure.core/double-array (.size al32472))]
                               (clojure.core/dotimes
                                 [j__11395__auto__ (.size al32472)]
                                 (clojure.core/let
                                   [t32456 (.get al32472 j__11395__auto__)]
                                   (clojure.core/aset arr__11394__auto__ j__11395__auto__ (clojure.core/double (.-field3 t32456)))))
                               arr__11394__auto__)))
                         l32446:linestatus
                         (com.wotbrew.cinq.column/->Column
                           nil
                           (clojure.core/fn
                             []
                             (clojure.core/let
                               [arr__11394__auto__ (clojure.core/object-array (.size al32472))]
                               (clojure.core/dotimes
                                 [j__11395__auto__ (.size al32472)]
                                 (clojure.core/let
                                   [t32456 (.get al32472 j__11395__auto__)]
                                   (clojure.core/aset arr__11394__auto__ j__11395__auto__ (clojure.lang.RT/box (.-field4 t32456)))))
                               arr__11394__auto__)))
                         l32446:extendedprice
                         (com.wotbrew.cinq.column/->DoubleColumn
                           nil
                           (clojure.core/fn
                             []
                             (clojure.core/let
                               [arr__11394__auto__ (clojure.core/double-array (.size al32472))]
                               (clojure.core/dotimes
                                 [j__11395__auto__ (.size al32472)]
                                 (clojure.core/let
                                   [t32456 (.get al32472 j__11395__auto__)]
                                   (clojure.core/aset arr__11394__auto__ j__11395__auto__ (clojure.core/double (.-field5 t32456)))))
                               arr__11394__auto__)))
                         returnflag32447
                         (.-field0 k32471)
                         linestatus32448
                         (.-field1 k32471)
                         %count
                         (.size al32472)]
                        (clojure.core/some->
                          (do
                            (.add
                              list32455
                              (new
                                com.wotbrew.cinq.tpch_test.Tuple19018
                                false
                                l32446:tax
                                l32446:discount
                                l32446:quantity
                                l32446:extendedprice
                                returnflag32447
                                linestatus32448
                                %count))
                            nil)
                          clojure.core/reduced)))
                    nil
                    ht__11431__auto__))
                list32455))]
           (java.util.Arrays/sort
             rs__11697__auto__
             (clojure.core/reify
               java.util.Comparator
               (clojure.core/compare
                 [___11694__auto__ a__11695__auto__ b__11696__auto__]
                 (clojure.core/let
                   [t32453 a__11695__auto__ t32454 b__11696__auto__]
                   (clojure.core/let
                     [res__11693__auto__
                      (clojure.core/compare
                        (clojure.core/let [returnflag32447 (.-field4 t32453)] returnflag32447)
                        (clojure.core/let [returnflag32447 (.-field4 t32454)] returnflag32447))]
                     (if
                       (clojure.core/= 0 res__11693__auto__)
                       (clojure.core/let
                         [res__11693__auto__
                          (clojure.core/compare
                            (clojure.core/let [linestatus32448 (.-field5 t32453)] linestatus32448)
                            (clojure.core/let [linestatus32448 (.-field5 t32454)] linestatus32448))]
                         (if (clojure.core/= 0 res__11693__auto__) 0 (clojure.core/* res__11693__auto__ 1)))
                       (clojure.core/* res__11693__auto__ 1)))))))
           (clojure.core/loop
             [i__11698__auto__ 0]
             (clojure.core/when
               (clojure.core/< i__11698__auto__ (clojure.core/alength rs__11697__auto__))
               (clojure.core/let
                 [t32452
                  (clojure.core/aget rs__11697__auto__ i__11698__auto__)
                  l32446:tax
                  (.-field0 t32452)
                  l32446:discount
                  (.-field1 t32452)
                  l32446:quantity
                  (.-field2 t32452)
                  l32446:extendedprice
                  (.-field3 t32452)
                  returnflag32447
                  (.-field4 t32452)
                  linestatus32448
                  (.-field5 t32452)
                  %count
                  (.-field6 t32452)]
                 (clojure.core/or
                   (clojure.core/let
                     [col32445
                      [returnflag32447
                       linestatus32448
                       (com.wotbrew.cinq.column/sum [l32446:quantity l32446:quantity] l32446:quantity)
                       (com.wotbrew.cinq.column/sum [l32446:extendedprice l32446:extendedprice] l32446:extendedprice)
                       (com.wotbrew.cinq.column/sum
                         [l32446:extendedprice l32446:extendedprice l32446:discount l32446:discount]
                         (com.wotbrew.cinq.CinqUtil/mul l32446:extendedprice (com.wotbrew.cinq.CinqUtil/sub 1.0 l32446:discount)))
                       (com.wotbrew.cinq.column/sum
                         [l32446:extendedprice l32446:extendedprice l32446:discount l32446:discount l32446:tax l32446:tax]
                         (com.wotbrew.cinq.CinqUtil/mul
                           l32446:extendedprice
                           (com.wotbrew.cinq.CinqUtil/mul
                             (com.wotbrew.cinq.CinqUtil/sub 1.0 l32446:discount)
                             (com.wotbrew.cinq.CinqUtil/add 1.0 l32446:tax))))
                       (com.wotbrew.cinq.column/avg [l32446:quantity l32446:quantity] l32446:quantity)
                       (com.wotbrew.cinq.column/avg [l32446:extendedprice l32446:extendedprice] l32446:extendedprice)
                       (com.wotbrew.cinq.column/avg [l32446:discount l32446:discount] l32446:discount)
                       %count]]
                     (clojure.core/let
                       [r__11351__auto__ (f32451 (clojure.core/aget box32449 0) (clojure.lang.RT/box col32445))]
                       (if
                         (clojure.core/reduced? r__11351__auto__)
                         (clojure.core/aset box32449 0 (clojure.core/deref r__11351__auto__))
                         (do (clojure.core/aset box32449 0 r__11351__auto__) nil))))
                   (recur (clojure.core/unchecked-inc i__11698__auto__))))))))
       box32449)
      (clojure.core/aget box32449 0))))

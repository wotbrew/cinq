(ns scratch2
  (:require [clojure.java.io :as io]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.lmdb :as lmdb]))

(def sf001 @com.wotbrew.cinq.tpch-test/sf-001)
(def nation (vec (:nation sf001)))
(def partsupp (vec (:partsupp sf001)))
(def supplier (vec (:supplier sf001)))
(def lineitem (vec (:lineitem sf001)))

(reify*
  [clojure.lang.IReduceInit com.wotbrew.cinq.protocols.Scannable]
  (com.wotbrew.cinq.eager-loop/scan
    [this__134455__auto__ f__134456__auto__ init__134457__auto__]
    (clojure.core/let
      [rsn173129 (clojure.core/long-array 1)]
      (clojure.core/aset rsn173129 0 -1)
      (.reduce
        this__134455__auto__
        (clojure.core/fn
          [acc__134458__auto__ x__134459__auto__]
          (f__134456__auto__
            acc__134458__auto__
            nil
            (clojure.core/aset rsn173129 0 (clojure.core/unchecked-inc (clojure.core/aget rsn173129 0)))
            x__134459__auto__))
        init__134457__auto__)))
  (clojure.core/reduce
    [com.wotbrew.cinq.eager-loop/_ f173130 init__134457__auto__]
    (clojure.core/let
      [box173128 (clojure.core/object-array 1)]
      (clojure.core/aset box173128 0 init__134457__auto__)
      ((clojure.core/fn
         [box173128]
         (clojure.core/let
           [rs__134931__auto__
            (.toArray
              (clojure.core/let
                [list173134 (java.util.ArrayList.)]
                (clojure.core/let
                  [ht173137 (java.util.HashMap.)]
                  (com.wotbrew.cinq.eager-loop/run-scan-no-rsn
                    (clojure.core/reify
                      clojure.lang.IFn
                      (com.wotbrew.cinq.eager-loop/invoke
                        [f__134301__auto__ agg__134302__auto__ rv__134303__auto__ rsn__134304__auto__ o__134305__auto__]
                        (if
                          (.filter f__134301__auto__ rsn__134304__auto__ o__134305__auto__)
                          (.apply f__134301__auto__ agg__134302__auto__ rv__134303__auto__ rsn__134304__auto__ o__134305__auto__)
                          agg__134302__auto__))
                      com.wotbrew.cinq.CinqScanFunction
                      (clojure.core/filter
                        [___134311__auto__ rsn173139 o__134312__auto__]
                        (clojure.core/let
                          [o173138 o__134312__auto__]
                          (clojure.core/let
                            [l173105:shipdate (.-shipdate o173138)]
                            (clojure.core/and
                              (com.wotbrew.cinq.CinqUtil/lte l173105:shipdate #inst"1998-09-02T00:00:00.000-00:00")
                              true))))
                      (com.wotbrew.cinq.eager-loop/nativeFilter
                        [___134314__auto__ symbol-table173142]
                        (clojure.core/let
                          [l173105:shipdate173143
                           (com.wotbrew.cinq.nio-codec/encode-heap #inst"1998-09-02T00:00:00.000-00:00" symbol-table173142 false)
                           l173105:shipdate173144
                           (com.wotbrew.cinq.nio-codec/intern-symbol symbol-table173142 :shipdate false)]
                          (clojure.core/when
                            (clojure.core/and l173105:shipdate173143 l173105:shipdate173144)
                            (clojure.core/reify
                              com.wotbrew.cinq.CinqScanFunction$NativeFilter
                              (clojure.core/apply
                                [___134314__auto__ rsn__134315__auto__ buf__134316__auto__]
                                (clojure.core/let
                                  [valbuf173141 (.slice buf__134316__auto__)]
                                  (clojure.core/and
                                    (com.wotbrew.cinq.CinqUtil/gte
                                      (com.wotbrew.cinq.nio-codec/bufcmp-ksv l173105:shipdate173143 valbuf173141 l173105:shipdate173144)
                                      0))))))))
                      (clojure.core/apply
                        [___134320__auto__ ___134320__auto__ rv173140 rsn173139 o__134321__auto__]
                        (clojure.core/let
                          [o173138 o__134321__auto__]
                          (clojure.core/let
                            [l173105
                             o173138
                             l173105:discount
                             (.-discount o173138)
                             l173105:extendedprice
                             (.-extendedprice o173138)
                             l173105:linestatus
                             (.-linestatus o173138)
                             l173105:quantity
                             (.-quantity o173138)
                             l173105:returnflag
                             (.-returnflag o173138)
                             l173105:tax
                             (.-tax o173138)
                             l173105:shipdate
                             (.-shipdate o173138)]
                            (clojure.core/some->
                              (clojure.core/let
                                [k173135
                                 (new com.wotbrew.cinq.tpch_test.Tuple72840 false l173105:returnflag l173105:linestatus)
                                 f__134756__auto__
                                 (clojure.core/reify
                                   java.util.function.BiFunction
                                   (clojure.core/apply
                                     [___134757__auto__ ___134757__auto__ arr__134758__auto__]
                                     (clojure.core/let
                                       [arr173136
                                        (clojure.core/or
                                          arr__134758__auto__
                                          (clojure.core/doto
                                            (clojure.core/object-array 11)
                                            (clojure.core/aset 0 (clojure.lang.RT/box 0.0))
                                            (clojure.core/aset 1 (clojure.lang.RT/box 0.0))
                                            (clojure.core/aset 2 (clojure.lang.RT/box 0.0))
                                            (clojure.core/aset 3 (clojure.lang.RT/box (java.lang.Long/valueOf 0)))
                                            (clojure.core/aset 4 (clojure.lang.RT/box 0.0))
                                            (clojure.core/aset 5 (clojure.lang.RT/box 0))
                                            (clojure.core/aset 6 (clojure.lang.RT/box 0.0))
                                            (clojure.core/aset 7 (clojure.lang.RT/box 0))
                                            (clojure.core/aset 8 (clojure.lang.RT/box 0.0))
                                            (clojure.core/aset 9 (clojure.lang.RT/box 0))
                                            (clojure.core/aset 10 (clojure.lang.RT/box 0))))
                                        acc-0-agg173109173117
                                        (clojure.core/aget arr173136 0)
                                        acc-0-agg173110173118
                                        (clojure.core/aget arr173136 1)
                                        acc-0-agg173111173119
                                        (clojure.core/aget arr173136 2)
                                        acc-0-agg173112173120
                                        (clojure.core/aget arr173136 3)
                                        acc-0-agg173113173121
                                        (clojure.core/aget arr173136 4)
                                        acc-1-agg173113173122
                                        (clojure.core/aget arr173136 5)
                                        acc-0-agg173114173123
                                        (clojure.core/aget arr173136 6)
                                        acc-1-agg173114173124
                                        (clojure.core/aget arr173136 7)
                                        acc-0-agg173115173125
                                        (clojure.core/aget arr173136 8)
                                        acc-1-agg173115173126
                                        (clojure.core/aget arr173136 9)
                                        acc-0-agg173116173127
                                        (clojure.core/aget arr173136 10)]
                                       (clojure.core/aset
                                         arr173136
                                         0
                                         (clojure.lang.RT/box (com.wotbrew.cinq.CinqUtil/sumStep acc-0-agg173109173117 l173105:quantity)))
                                       (clojure.core/aset
                                         arr173136
                                         1
                                         (clojure.lang.RT/box
                                           (com.wotbrew.cinq.CinqUtil/sumStep acc-0-agg173110173118 l173105:extendedprice)))
                                       (clojure.core/aset
                                         arr173136
                                         2
                                         (clojure.lang.RT/box
                                           (com.wotbrew.cinq.CinqUtil/sumStep
                                             acc-0-agg173111173119
                                             (com.wotbrew.cinq.CinqUtil/mul
                                               l173105:extendedprice
                                               (com.wotbrew.cinq.CinqUtil/sub 1.0 l173105:discount)))))
                                       (clojure.core/aset
                                         arr173136
                                         3
                                         (clojure.lang.RT/box
                                           (com.wotbrew.cinq.CinqUtil/sumStep
                                             acc-0-agg173112173120
                                             (com.wotbrew.cinq.CinqUtil/mul
                                               l173105:extendedprice
                                               (com.wotbrew.cinq.CinqUtil/mul
                                                 (com.wotbrew.cinq.CinqUtil/sub 1.0 l173105:discount)
                                                 (com.wotbrew.cinq.CinqUtil/add 1.0 l173105:tax))))))
                                       (clojure.core/aset
                                         arr173136
                                         4
                                         (clojure.lang.RT/box (com.wotbrew.cinq.CinqUtil/sumStep acc-0-agg173113173121 l173105:quantity)))
                                       (clojure.core/aset
                                         arr173136
                                         5
                                         (clojure.lang.RT/box (clojure.core/unchecked-inc acc-1-agg173113173122)))
                                       (clojure.core/aset
                                         arr173136
                                         6
                                         (clojure.lang.RT/box
                                           (com.wotbrew.cinq.CinqUtil/sumStep acc-0-agg173114173123 l173105:extendedprice)))
                                       (clojure.core/aset
                                         arr173136
                                         7
                                         (clojure.lang.RT/box (clojure.core/unchecked-inc acc-1-agg173114173124)))
                                       (clojure.core/aset
                                         arr173136
                                         8
                                         (clojure.lang.RT/box (com.wotbrew.cinq.CinqUtil/sumStep acc-0-agg173115173125 l173105:discount)))
                                       (clojure.core/aset
                                         arr173136
                                         9
                                         (clojure.lang.RT/box (clojure.core/unchecked-inc acc-1-agg173115173126)))
                                       (clojure.core/aset
                                         arr173136
                                         10
                                         (clojure.lang.RT/box (clojure.core/unchecked-inc acc-0-agg173116173127)))
                                       arr173136)))]
                                (.compute ht173137 k173135 f__134756__auto__)
                                nil)
                              clojure.core/reduced))))
                      (com.wotbrew.cinq.eager-loop/rootDoesNotEscape [___134320__auto__] false))
                    lineitem)
                  (clojure.core/reduce
                    (clojure.core/fn
                      [___134759__auto__ [k173135 arr173136]]
                      (clojure.core/let
                        [returnflag173106
                         (.-field0 k173135)
                         linestatus173107
                         (.-field1 k173135)
                         acc-0-agg173109173117
                         (clojure.core/aget arr173136 0)
                         acc-0-agg173110173118
                         (clojure.core/aget arr173136 1)
                         acc-0-agg173111173119
                         (clojure.core/aget arr173136 2)
                         acc-0-agg173112173120
                         (clojure.core/aget arr173136 3)
                         acc-0-agg173113173121
                         (clojure.core/aget arr173136 4)
                         acc-1-agg173113173122
                         (clojure.core/aget arr173136 5)
                         acc-0-agg173114173123
                         (clojure.core/aget arr173136 6)
                         acc-1-agg173114173124
                         (clojure.core/aget arr173136 7)
                         acc-0-agg173115173125
                         (clojure.core/aget arr173136 8)
                         acc-1-agg173115173126
                         (clojure.core/aget arr173136 9)
                         acc-0-agg173116173127
                         (clojure.core/aget arr173136 10)
                         agg173109
                         acc-0-agg173109173117
                         agg173110
                         acc-0-agg173110173118
                         agg173111
                         acc-0-agg173111173119
                         agg173112
                         acc-0-agg173112173120
                         agg173113
                         (com.wotbrew.cinq.CinqUtil/div
                           acc-0-agg173113173121
                           (com.wotbrew.cinq.expr/apply-n2n clojure.core/max 1 acc-1-agg173113173122))
                         agg173114
                         (com.wotbrew.cinq.CinqUtil/div
                           acc-0-agg173114173123
                           (com.wotbrew.cinq.expr/apply-n2n clojure.core/max 1 acc-1-agg173114173124))
                         agg173115
                         (com.wotbrew.cinq.CinqUtil/div
                           acc-0-agg173115173125
                           (com.wotbrew.cinq.expr/apply-n2n clojure.core/max 1 acc-1-agg173115173126))
                         agg173116
                         acc-0-agg173116173127
                         col173104173108
                         [returnflag173106
                          linestatus173107
                          agg173109
                          agg173110
                          agg173111
                          agg173112
                          agg173113
                          agg173114
                          agg173115
                          agg173116]]
                        (clojure.core/some->
                          (do (.add list173134 (new com.wotbrew.cinq.distinct_test.Tuple16803 false col173104173108)) nil)
                          clojure.core/reduced)))
                    nil
                    ht173137))
                list173134))]
           (java.util.Arrays/sort
             rs__134931__auto__
             (clojure.core/reify
               java.util.Comparator
               (clojure.core/compare
                 [___134928__auto__ a__134929__auto__ b__134930__auto__]
                 (clojure.core/let
                   [t173132 a__134929__auto__ t173133 b__134930__auto__]
                   (clojure.core/let
                     [res__134927__auto__
                      (clojure.core/compare (clojure.core/let [] returnflag173106) (clojure.core/let [] returnflag173106))]
                     (if
                       (clojure.core/= 0 res__134927__auto__)
                       (clojure.core/let
                         [res__134927__auto__
                          (clojure.core/compare (clojure.core/let [] linestatus173107) (clojure.core/let [] linestatus173107))]
                         (if (clojure.core/= 0 res__134927__auto__) 0 (clojure.core/* res__134927__auto__ 1)))
                       (clojure.core/* res__134927__auto__ 1)))))))
           (clojure.core/loop
             [i__134932__auto__ 0]
             (clojure.core/when
               (clojure.core/< i__134932__auto__ (clojure.core/alength rs__134931__auto__))
               (clojure.core/let
                 [t173131 (clojure.core/aget rs__134931__auto__ i__134932__auto__) col173104173108 (.-field0 t173131)]
                 (clojure.core/or
                   (clojure.core/let
                     [col173104 col173104173108]
                     (clojure.core/let
                       [r__134454__auto__ (f173130 (clojure.core/aget box173128 0) (clojure.lang.RT/box col173104))]
                       (if
                         (clojure.core/reduced? r__134454__auto__)
                         (clojure.core/aset box173128 0 (clojure.core/deref r__134454__auto__))
                         (do (clojure.core/aset box173128 0 r__134454__auto__) nil))))
                   (recur (clojure.core/unchecked-inc i__134932__auto__))))))))
       box173128)
      (clojure.core/aget box173128 0))))

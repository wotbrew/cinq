(ns scratch2
  (:require [clojure.java.io :as io]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.lmdb :as lmdb]))
(reify*
  [clojure.lang.IReduceInit com.wotbrew.cinq.protocols.Scannable]
  (com.wotbrew.cinq.eager-loop/scan
    [this__11466__auto__ f__11467__auto__ init__11468__auto__]
    (clojure.core/let
      [rsn24405 (clojure.core/long-array 1)]
      (clojure.core/aset rsn24405 0 -1)
      (.reduce
        this__11466__auto__
        (clojure.core/fn
          [acc__11469__auto__ x__11470__auto__]
          (f__11467__auto__
            acc__11469__auto__
            nil
            (clojure.core/aset rsn24405 0 (clojure.core/unchecked-inc (clojure.core/aget rsn24405 0)))
            x__11470__auto__))
        init__11468__auto__)))
  (clojure.core/reduce
    [com.wotbrew.cinq.eager-loop/_ f24406 init__11468__auto__]
    (clojure.core/let
      [box24404 (clojure.core/object-array 1)]
      (clojure.core/aset box24404 0 init__11468__auto__)
      ((clojure.core/fn
         [box24404]
         (com.wotbrew.cinq.eager-loop/run-scan-no-rsn
           (clojure.core/reify
             clojure.lang.IFn
             (com.wotbrew.cinq.eager-loop/invoke
               [f__24041__auto__ agg__24042__auto__ rv__24043__auto__ rsn__24044__auto__ o__24045__auto__]
               (.apply f__24041__auto__ agg__24042__auto__ rv__24043__auto__ rsn__24044__auto__ o__24045__auto__))
             com.wotbrew.cinq.CinqScanFunction
             (clojure.core/filter [___24048__auto__ ___24048__auto__ ___24048__auto__] true)
             (com.wotbrew.cinq.eager-loop/nativeFilter
               [___24052__auto__ ___24052__auto__]
               (clojure.core/reify
                 com.wotbrew.cinq.CinqScanFunction$NativeFilter
                 (clojure.core/apply [___24052__auto__ _rsn__24053__auto__ _buf__24054__auto__] true)))
             (clojure.core/apply
               [___24055__auto__ ___24055__auto__ rv24412 rsn24411 o__24056__auto__]
               (clojure.core/let
                 [o24410 o__24056__auto__]
                 (clojure.core/prn rsn24411 o24410)
                 (clojure.core/let
                   [t24402:nconst (:nconst o24410)]
                   (clojure.core/some->
                     (com.wotbrew.cinq.eager-loop/run-scan-no-rsn
                       (clojure.core/reify
                         clojure.lang.IFn
                         (com.wotbrew.cinq.eager-loop/invoke
                           [f__24041__auto__ agg__24042__auto__ rv__24043__auto__ rsn__24044__auto__ o__24045__auto__]
                           (.apply f__24041__auto__ agg__24042__auto__ rv__24043__auto__ rsn__24044__auto__ o__24045__auto__))
                         com.wotbrew.cinq.CinqScanFunction
                         (clojure.core/filter [___24048__auto__ ___24048__auto__ ___24048__auto__] true)
                         (com.wotbrew.cinq.eager-loop/nativeFilter
                           [___24052__auto__ ___24052__auto__]
                           (clojure.core/reify
                             com.wotbrew.cinq.CinqScanFunction$NativeFilter
                             (clojure.core/apply [___24052__auto__ _rsn__24053__auto__ _buf__24054__auto__] true)))
                         (clojure.core/apply
                           [___24055__auto__ ___24055__auto__ rv24409 rsn24408 o__24056__auto__]
                           (clojure.core/let
                             [o24407 o__24056__auto__]
                             (clojure.core/prn rsn24408 o24407)
                             (clojure.core/let
                               [n24403 o24407]
                               (clojure.core/some->
                                 (clojure.core/let
                                   [col24401 n24403]
                                   (clojure.core/let
                                     [r__11465__auto__ (f24406 (clojure.core/aget box24404 0) (clojure.lang.RT/box col24401))]
                                     (if
                                       (clojure.core/reduced? r__11465__auto__)
                                       (clojure.core/aset box24404 0 (clojure.core/deref r__11465__auto__))
                                       (do (clojure.core/aset box24404 0 r__11465__auto__) nil))))
                                 clojure.core/reduced))))
                         (com.wotbrew.cinq.eager-loop/rootDoesNotEscape [___24055__auto__] false))
                       ((:nconst names) t24402:nconst))
                     clojure.core/reduced))))
             (com.wotbrew.cinq.eager-loop/rootDoesNotEscape [___24055__auto__] true))
           ((:tconst principals) 78748)))
       box24404)
      (clojure.core/aget box24404 0))))

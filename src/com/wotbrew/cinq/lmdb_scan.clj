(ns com.wotbrew.cinq.lmdb-scan
  (:require [com.wotbrew.cinq.nio-codec :as codec])
  (:import (com.wotbrew.cinq CinqScanFunction2 CinqScanFunction2$NativeFilter)
           (org.lmdbjava Cursor GetOp)))

(defn- join-cursor
  ([^Cursor index-cursor ^Cursor primary-cursor]
   (when-not (.get primary-cursor (.val index-cursor) GetOp/MDB_SET_KEY)
     (throw (ex-info "No primary record found for index seek, index may be corrupted" {})))
   true))

(defn- secondary-step
  ([^Cursor index-cursor ^CinqScanFunction2$NativeFilter nf fwd]
   (and (if fwd (.next index-cursor) (.prev index-cursor))
        (.indexKeyCont nf (.key index-cursor)))))

(defn clustered-index-scan
  [^Cursor index-cursor
   ^CinqScanFunction2 scan-function
   extract-key
   relvar
   init
   symbol-list]
  (if-some [nf (.nativeFilter scan-function symbol-list)]
    (let [fwd (.fwd nf)
          start-key (.startKey nf)]
      (if-not (and (if start-key
                     (.get index-cursor start-key GetOp/MDB_SET_RANGE)
                     (.first index-cursor))
                   (.indexKeyCont nf (.key index-cursor)))
        init
        (loop [acc init]
          (when (Thread/interrupted) (throw (InterruptedException.)))
          (if-not (.indexKeyPred nf (.key index-cursor))
            (if (secondary-step index-cursor nf fwd)
              (recur acc)
              acc)
            (if-not (.valPred nf (.val index-cursor))
              (if (secondary-step index-cursor nf fwd)
                (recur acc)
                acc)
              (let [o (codec/decode-object (.val index-cursor) symbol-list)
                    k (extract-key o)
                    ret (.apply scan-function acc relvar k o)]
                (cond
                  (reduced? ret) @ret
                  (secondary-step index-cursor nf fwd) (recur ret)
                  :else ret)))))))
    init))

(defn secondary-index-scan
  [^Cursor index-cursor
   ^Cursor primary-cursor
   ^CinqScanFunction2 scan-function
   extract-key
   relvar
   init
   symbol-list]
  (if-some [nf (.nativeFilter scan-function symbol-list)]
    (let [fwd (.fwd nf)
          start-key (.startKey nf)]
      (if-not (and (if start-key
                     (.get index-cursor start-key GetOp/MDB_SET_RANGE)
                     (.first index-cursor))
                   (.indexKeyCont nf (.key index-cursor)))
        init
        (loop [acc init]
          (when (Thread/interrupted) (throw (InterruptedException.)))
          (if-not (.indexKeyPred nf (.key index-cursor))
            (if (secondary-step index-cursor nf fwd)
              (recur acc)
              acc)
            (do (join-cursor index-cursor primary-cursor)
                (if-not (.valPred nf (.val primary-cursor))
                  (if (secondary-step index-cursor nf fwd)
                    (recur acc)
                    acc)
                  (let [o (codec/decode-object (.val primary-cursor) symbol-list)
                        k (extract-key o)
                        ret (.apply scan-function acc relvar k o)]
                    (cond
                      (reduced? ret) @ret
                      (secondary-step index-cursor nf fwd) (recur ret)
                      :else ret))))))))
    init))

[{:a 0, :b 3}
 {:a 1, :b 1}
 {:a 2, :b 2}]

'([:= 1])
:start [1]
:direction :asc
:end-key [:<> 1]

'([:= 2] [:= 3])
:start [2 3]
:direction :asc
:end-k [:<> 2 3]
:end-v nil

'([:< 2] [:> 3])
:start [2 :MAX]
:direction :desc
:end-k nil

'([:= 2] [:< 3])
:start [2 3]
:direction :desc
:end-k [:< 2]

'([:< 2 5] [:= 3])
:start [2 3]
:direction :asc
:end-k [:> 5]

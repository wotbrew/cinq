(ns com.wotbrew.cinq.lmdb
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.nio-codec :as codec]
            [com.wotbrew.cinq.protocols :as p])
  (:import (clojure.lang IFn ILookup IReduceInit Reduced)
           (com.wotbrew.cinq CinqScanFunction CinqUnsafeDynamicMap CinqUtil)
           (com.wotbrew.cinq.protocols AutoIncrementing Indexable)
           (java.io Closeable)
           (java.nio ByteBuffer)
           (java.util Arrays ArrayList Comparator HashMap Map)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Function Supplier)
           (org.lmdbjava Cursor Dbi DbiFlags Env Env$MapFullException EnvFlags EnvInfo GetOp PutFlags Txn Txn$NotReadyException))
  (:refer-clojure :exclude [replace]))

(set! *warn-on-reflection* true)

(def ^:redef open-files #{})

(defn- native-scan [^Cursor cursor ^CinqScanFunction f init symbol-table rv]
  (if-not (.first cursor)
    init
    (let [symbol-list (codec/symbol-list symbol-table)
          mut-record (CinqUnsafeDynamicMap. symbol-list)
          native-filter (.nativeFilter f symbol-table)]
      (if-not native-filter
        init
        (loop [acc init]
          (when (Thread/interrupted) (throw (InterruptedException.)))
          (let [rsn (.getLong ^ByteBuffer (.key cursor))
                val (.val cursor)
                pos (.position ^ByteBuffer val)]
            (if-not (.apply native-filter rsn val)
              (if (.next cursor)
                (recur acc)
                acc)
              (let [_ (.position ^ByteBuffer val pos)
                    o (codec/decode-root-unsafe mut-record val symbol-list)
                    ret (.apply f acc rv rsn o)]
                (if (reduced? ret)
                  @ret
                  (if (.next cursor)
                    (recur ret)
                    ret))))))))))

(defn- heap-scan [^Cursor cursor f init symbol-list rv]
  (if-not (.first cursor)
    init
    (loop [acc init]
      (when (Thread/interrupted) (throw (InterruptedException.)))
      (let [rsn (.getLong ^ByteBuffer (.key cursor))
            o (codec/decode-object (.val cursor) symbol-list)
            ret (f acc rv rsn o)]
        (if (reduced? ret)
          @ret
          (if (.next cursor)
            (recur ret)
            ret))))))

(defn- scan [^Cursor cursor f init symbol-table rv]
  (if (and (instance? CinqScanFunction f) (.rootDoesNotEscape ^CinqScanFunction f))
    (native-scan cursor f init symbol-table rv)
    (heap-scan cursor f init (some-> symbol-table codec/symbol-list) rv)))

(defn- last-rsn ^long [^Cursor cursor]
  (if (.last cursor)
    (let [^ByteBuffer k (.key cursor)
          rsn (.getLong k)]
      rsn)
    -1))

(defn- proxy-scan-function
  "Allows the relvar parameter to be overridden, useful for root variables that dispatch to a tx variable
  to do actual scans."
  [rv f]
  (if (instance? CinqScanFunction f)
    (let [^CinqScanFunction f f]
      (reify CinqScanFunction
        (rootDoesNotEscape [_] (.rootDoesNotEscape f))
        (apply [_ acc _ rsn x] (.apply f acc rv rsn x))
        (nativeFilter [_ st] (.nativeFilter f st))
        (filter [_ rsn x] (.filter f rsn x))
        IFn
        (invoke [_ acc _ rsn x] (f acc rv rsn x))))
    (fn [acc _ rsn x] (f acc rv rsn x))))

(defn- reduce-scan [r f init]
  (p/scan r (fn [acc _ _ x] (f acc x)) init))

(defn get-index-entry [relvar
                       ^Txn txn
                       ^Dbi dbi
                       ^Dbi rsn-dbi
                       ^ByteBuffer key-buffer
                       ^ByteBuffer rsn-buffer
                       symbol-table
                       indexed-key
                       k
                       ^longs key-changed]
  (reify
    p/Scannable
    (scan [_ f init]
      (with-open [cursor (.openCursor dbi txn)]
        (.clear key-buffer)
        (.clear rsn-buffer)
        (codec/encode-key k key-buffer)
        (let [last-key (aset key-changed 0 (unchecked-inc (aget key-changed 0)))]
          (.flip key-buffer)
          (if-not (.get cursor key-buffer GetOp/MDB_SET)
            init
            (with-open [rsn-cursor (.openCursor rsn-dbi txn)]
              (loop [acc init]
                (when (Thread/interrupted) (throw (InterruptedException.)))
                (.clear rsn-buffer)
                (let [rsn (.getLong ^ByteBuffer (.val cursor))
                      _ (.putLong rsn-buffer rsn)
                      _ (.flip rsn-buffer)
                      has-next (and (.next cursor)
                                    (if (= last-key (aget key-changed 0))
                                      (= 0 (.compareTo key-buffer (.key cursor)))
                                      (do (.clear key-buffer)
                                          (codec/encode-key k key-buffer)
                                          (.flip key-buffer)
                                          (aset key-changed 0 last-key)
                                          (= 0 (.compareTo key-buffer (.key cursor))))))]
                  (when-not (.get rsn-cursor rsn-buffer GetOp/MDB_SET)
                    (throw (IllegalStateException. "Could not find rsn during index seek, suspect index corruption")))
                  (let [record (codec/decode-object (.val rsn-cursor) (codec/symbol-list symbol-table))
                        ;; todo use lossy byte to determine whether we know ke == k from the buf
                        ke (get record indexed-key)]
                    (if-not (CinqUtil/eq ke k)
                      (if has-next (recur acc) acc)
                      (let [r (f acc
                                 relvar
                                 (.getLong ^ByteBuffer (.key rsn-cursor))
                                 record)]
                        (if (reduced? r)
                          @r
                          (if has-next
                            (recur r)
                            r))))))))))))
    IReduceInit
    (reduce [index-entry f init]
      (reduce-scan index-entry f init))))

(defn get-index-range-entry [relvar
                             ^Txn txn
                             ^Dbi dbi
                             ^Dbi rsn-dbi
                             ^ByteBuffer key-buffer
                             ^ByteBuffer rsn-buffer
                             symbol-table
                             indexed-key
                             ;; either cinq/gt or cinq/gte
                             start-test
                             start
                             ;; either cinq/lt or cinq/lte
                             end-test
                             end
                             ^longs key-changed]
  (let [pred-a (if start-test #(start-test %1 %2) (constantly true))
        pred-b (if end-test #(end-test %1 %2) (constantly true))
        pred #(and (pred-a %1 %2) (pred-b %1 %3))]
    (reify
      p/Scannable
      (scan [_ f init]
        (with-open [cursor (.openCursor dbi txn)]
          (.clear key-buffer)
          (.clear rsn-buffer)
          ;; nil is = -127
          (codec/encode-key start key-buffer)
          (.flip key-buffer)
          (let [last-key (aset key-changed 0 (unchecked-inc (aget key-changed 0)))]
            (if-not (.get cursor key-buffer GetOp/MDB_SET_RANGE)
              init
              (do
                (.clear key-buffer)
                (when end-test (codec/encode-key end key-buffer))
                (with-open [rsn-cursor (.openCursor rsn-dbi txn)]
                  (loop [acc init]
                    (when (Thread/interrupted) (throw (InterruptedException.)))
                    (let [rsn (.getLong ^ByteBuffer (.val cursor))
                          _ (.putLong rsn-buffer rsn)
                          _ (.flip rsn-buffer)
                          has-next (and (.next cursor)
                                        (if (= last-key (aget key-changed 0))
                                          (<= (.compareTo key-buffer (.key cursor)) 0)
                                          (do (.clear key-buffer)
                                              (if end-test
                                                (codec/encode-key end key-buffer)
                                                (codec/encode-key start key-buffer))
                                              (.flip key-buffer)
                                              (aset key-changed 0 last-key)
                                              (<= (.compareTo key-buffer (.key cursor)) 0))))]
                      (do
                        (when-not (.get rsn-cursor rsn-buffer GetOp/MDB_SET)
                          (throw (IllegalStateException. "Could not find rsn during index seek, suspect index corruption")))
                        (let [record (codec/decode-object (.val rsn-cursor) (codec/symbol-list symbol-table))
                              ke (get record indexed-key)]
                          (if-not (pred ke start end)
                            (if has-next (recur acc) acc)
                            (let [r (f acc
                                       relvar
                                       (.getLong ^ByteBuffer (.key rsn-cursor))
                                       record)]
                              (if (reduced? r)
                                @r
                                (if has-next
                                  (recur r)
                                  acc))))))))))))))
      IReduceInit
      (reduce [index-entry f init]
        (reduce-scan index-entry f init)))))

(defn get-top-bottom-entry
  [relvar
   ^Txn txn
   ^Dbi dbi
   ^Dbi rsn-dbi
   ^ByteBuffer key-buffer
   ^ByteBuffer rsn-buffer
   symbol-table
   indexed-key
   top]
  (reify
    p/Scannable
    (scan [_ f init]
      (with-open [cursor (.openCursor dbi txn)]
        (.clear rsn-buffer)
        (if-not (if top (.last cursor) (.first cursor))
          init
          (let [rsns (ArrayList.)
                records (ArrayList.)
                symbol-list (codec/symbol-list symbol-table)]
            (loop [acc init]
              (.clear key-buffer)
              (.put key-buffer ^ByteBuffer (.key cursor))
              (.add rsns (.getLong ^ByteBuffer (.val cursor)))
              (let [has-next (if top (.prev cursor) (.next cursor))]
                (cond
                  ;; same key
                  (and has-next (.equals (.flip key-buffer) (.key cursor)))
                  (recur acc)

                  :else
                  (do
                    (with-open [rsn-cursor (.openCursor rsn-dbi txn)]
                      (dotimes [i (.size rsns)]
                        (let [rsn (.get rsns i)]
                          (.clear rsn-buffer)
                          (.putLong rsn-buffer rsn)
                          (.get rsn-cursor (.flip rsn-buffer) GetOp/MDB_SET)
                          (.add records (codec/decode-object (.val rsn-cursor) symbol-list)))))

                    (let [sorted (.toArray records)
                          _
                          (Arrays/sort
                            sorted
                            (reify Comparator
                              (compare [_ a b]
                                (let [ak (get a indexed-key)
                                      bk (get b indexed-key)]
                                  (if top
                                    (compare bk ak)
                                    (compare ak bk))))))
                          ret (loop [acc acc
                                     i 0]
                                (if (< i (alength sorted))
                                  (let [o (aget sorted i)
                                        ret (f acc relvar (.get rsns i) o)]
                                    (if (instance? Reduced ret)
                                      ret
                                      (recur ret (unchecked-inc i))))
                                  acc))]
                      (cond
                        (instance? Reduced ret) @ret
                        has-next (do
                                   (.clear rsns)
                                   (.clear records)
                                   (recur ret))
                        :else ret))))))))))
    IReduceInit
    (reduce [index-entry f init]
      (reduce-scan index-entry f init))))

(defn scan-index [^Dbi dbi ^Dbi rsn-dbi ^Txn txn f init symbol-list rv]
  (with-open [cursor (.openCursor dbi txn)
              rsn-cursor (.openCursor rsn-dbi txn)]
    (if (.first cursor)
      (loop [acc init]
        (let [_ (.get rsn-cursor (.val cursor) GetOp/MDB_SET)
              o (codec/decode-object (.val rsn-cursor) symbol-list)
              ret (f acc rv (.getLong ^ByteBuffer (.val cursor) 0) o)]
          (if (instance? Reduced ret)
            @ret
            (if (.next cursor)
              (recur ret)
              ret))))
      init)))

(defn get-index [relvar ^Txn txn ^Dbi dbi ^Dbi rsn-dbi ^ByteBuffer key-buffer ^ByteBuffer rsn-buffer indexed-key symbol-table keys-changed]
  (reify
    IFn
    (invoke [i k] (.valAt i k))
    (invoke [i k not-found] (.valAt i k not-found))
    p/Scannable
    (scan [_ f init]
      (scan-index dbi rsn-dbi txn f init (codec/symbol-list symbol-table) relvar))
    IReduceInit
    (reduce [index f init]
      (reduce-scan index f init))

    ;; lookup one key
    ILookup
    (valAt [_ k] (get-index-entry relvar txn dbi rsn-dbi key-buffer rsn-buffer symbol-table indexed-key k keys-changed))
    (valAt [r k _not-found] (.valAt r k))

    p/Index
    (indexed-key [_] indexed-key)
    (getn [i k] (.valAt i k))
    (get1 [i k not-found] (c/rel-first (.valAt i k) not-found))
    (range-scan [_ test-a a test-b b] (get-index-range-entry relvar txn dbi rsn-dbi key-buffer rsn-buffer symbol-table indexed-key test-a a test-b b keys-changed))
    (sorted-scan [_ high] (get-top-bottom-entry relvar txn dbi rsn-dbi key-buffer rsn-buffer symbol-table indexed-key high))))

(def ^:private ^"[Lorg.lmdbjava.PutFlags;" insert-flags
  (make-array PutFlags 0))

(def ^:private ^"[Lorg.lmdbjava.PutFlags;" replace-flags
  (doto ^"[Lorg.lmdbjava.PutFlags;" (make-array PutFlags 1)
    (aset 0 PutFlags/MDB_CURRENT)))

(defn- index-insert [^Cursor index-cursor ^ByteBuffer key-buffer ^ByteBuffer val-buffer rsn key symbol-table]
  (.clear key-buffer)
  (.clear val-buffer)
  (codec/encode-key key key-buffer)
  (.putLong val-buffer rsn)
  (.flip key-buffer)
  (.flip val-buffer)
  (.put index-cursor key-buffer val-buffer insert-flags))

(defn index-delete [^Cursor index-cursor ^ByteBuffer key-buffer ^ByteBuffer val-buffer rsn key symbol-list]
  (.clear key-buffer)
  (.clear val-buffer)
  (codec/encode-key key key-buffer)
  (when (.get index-cursor (.flip key-buffer) GetOp/MDB_SET)
    (let [rsn (long rsn)]
      (loop []
        (if (= rsn (.getLong ^ByteBuffer (.val index-cursor)))
          (.delete index-cursor insert-flags)
          ;; we can assume if the index is not corrupted, we _must_ find the rsn in this sequence, so no repeat key checks.
          (when (.next index-cursor)
            (recur)))))))

(defn- rel-set [^Dbi dbi
                ^Txn txn
                ^ByteBuffer key-buffer
                ^ByteBuffer val-buffer
                rel
                symbol-table
                ^objects index-dbi-array
                ^objects index-key-array
                ^objects index-cur-array]
  (let [step (fn [_acc _relvar rsn r]
               (.clear key-buffer)
               (.clear val-buffer)
               (.putLong key-buffer rsn)
               (if-some [err (codec/encode-object r val-buffer symbol-table true)]
                 (reduced err)
                 (do (.put dbi txn (.flip key-buffer) (.flip val-buffer) insert-flags)
                     (dotimes [i (alength index-key-array)]
                       (let [index-key (aget index-key-array i)
                             index-cur (aget index-cur-array i)]
                         (when-some [key (get r index-key)]
                           (index-insert index-cur key-buffer val-buffer rsn key symbol-table))))
                     nil)))]
    (.drop dbi txn)
    (dotimes [i (alength index-dbi-array)]
      (let [^Dbi index-dbi (aget index-dbi-array i)]
        (.drop index-dbi txn)))
    (when-some [err (p/scan rel step nil)]
      (throw (ex-info "Transaction error during rel-set" {:error err})))))

(defn- insert [^Dbi dbi ^Txn txn ^ByteBuffer key-buffer ^ByteBuffer val-buffer rsn record symbol-table]
  (if (nil? rsn)
    (recur dbi txn key-buffer val-buffer (with-open [cursor (.openCursor dbi txn)] (last-rsn cursor)) record symbol-table)
    (do
      (.clear key-buffer)
      (.clear val-buffer)
      (.putLong key-buffer rsn)
      (if-some [err (codec/encode-object record val-buffer symbol-table true)]
        (throw (ex-info "Not enough space in write-buffer during insert" {:cinq/error err}))
        (.put dbi txn (.flip key-buffer) (.flip val-buffer) insert-flags)))))

(defn- delete [^Cursor cursor ^ByteBuffer key-buffer ^long rsn symbol-list]
  (.clear key-buffer)
  (.putLong key-buffer rsn)
  (when (.get cursor (.flip key-buffer) GetOp/MDB_SET)
    (let [record (codec/decode-object (.val cursor) symbol-list)]
      (.delete cursor insert-flags)
      record)))

(defn- replace [^Dbi dbi ^Txn txn ^ByteBuffer key-buffer ^ByteBuffer val-buffer ^Long rsn ^Object record symbol-table]
  (.clear key-buffer)
  (.clear val-buffer)
  (.putLong key-buffer rsn)
  ;; maybe reuse a cursor here?
  (with-open [cursor (.openCursor dbi txn)]
    (if-some [err (codec/encode-object record val-buffer symbol-table true)]
      (throw (ex-info "Not enough space in write-buffer during replace" {:cinq/error err}))
      (if (.get cursor (.flip key-buffer) GetOp/MDB_SET)
        (.put cursor
              key-buffer
              (.flip val-buffer)
              replace-flags)
        false))))

(defn open-dbi ^Dbi [^Env env ^String dbi-name]
  (let [flags (into-array DbiFlags [DbiFlags/MDB_CREATE])]
    (.openDbi env dbi-name ^"[Lorg.lmdbjava.DbiFlags;" flags)))

(defn index-dbi? [dbi-name] (str/includes? dbi-name "."))

(defn open-index-dbi ^Dbi [^Env env ^String dbi-name]
  (let [flags (into-array DbiFlags [DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT])]
    (.openDbi env dbi-name ^"[Lorg.lmdbjava.DbiFlags;" flags)))

(def ^:const symbols-dbi-name "$symbols")
(def ^:const variables-dbi-name "$variables")

(defn- assoc-if-absent [m k v] (if (contains? m k) m (assoc m k v)))

(deftype LMDBWriteTransactionVariable
  [^Env env
   ^Txn txn
   ^Dbi dbi
   ^ByteBuffer key-buffer
   ^ByteBuffer rsn-buffer
   ^ByteBuffer val-buffer
   ^:unsynchronized-mutable next-rsn
   symbol-table
   indexes
   ^Map dbis
   ^Cursor cursor
   ^objects index-dbi-array
   ^objects index-key-array
   ^objects index-cur-array
   ^longs keys-changed
   auto-key]
  ILookup
  (valAt [r k] (.valAt r k nil))
  (valAt [r k not-found]
    (if-some [{index-dbi-name :dbi-name} (indexes k)]
      (get-index r txn (.get dbis index-dbi-name) dbi key-buffer rsn-buffer k symbol-table keys-changed)
      not-found))
  p/Scannable
  (scan [rv f init]
    (scan cursor f init symbol-table rv))
  p/BigCount
  (big-count [_] (.-entries (.stat dbi txn)))
  IReduceInit
  (reduce [relvar f start]
    (p/scan relvar (fn [acc _ _ record] (f acc record)) start))
  p/Relvar
  (rel-set [relvar rel]
    (set! (.-next-rsn relvar) -1)
    (rel-set dbi txn key-buffer val-buffer rel symbol-table index-dbi-array index-key-array index-cur-array))
  p/IncrementalRelvar
  (insert [relvar record]
    (if (= -1 (.-next-rsn relvar))
      (do (set! (.-next-rsn relvar) (unchecked-inc (last-rsn cursor)))
          (recur record))
      (let [rsn next-rsn
            record (if auto-key (assoc-if-absent record auto-key rsn) record)
            _ (insert dbi txn key-buffer val-buffer rsn record symbol-table)]

        (dotimes [i (alength index-key-array)]
          (let [index-key (aget index-key-array i)
                index-cursor (aget index-cur-array i)]
            ;; only non-nil in indexes
            (when-some [key (get record index-key)]
              (index-insert index-cursor key-buffer val-buffer rsn key symbol-table))))

        (set! (.-next-rsn relvar) (unchecked-inc rsn))
        rsn)))
  (delete [_ rsn]
    (let [symbol-list (codec/symbol-list symbol-table)]
      (when-some [old-record (delete cursor key-buffer rsn symbol-list)]
        ;; only non-nil in indexes (if old-record is also nil, this still works!)
        (dotimes [i (alength index-key-array)]
          (let [index-key (aget index-key-array i)
                index-cursor (aget index-cur-array i)]
            (when-some [key (get old-record index-key)]
              (index-delete index-cursor key-buffer val-buffer rsn key symbol-list))))
        (some? old-record))))
  Closeable
  (close [_]
    (when index-cur-array
      (dotimes [i (alength index-cur-array)]
        (.close ^Cursor (aget index-cur-array i))))
    (.close cursor)))

(deftype LMDBReadTransactionVariable
  [^Env env
   ^Txn txn
   ^Dbi dbi
   ^ByteBuffer key-buffer
   ^ByteBuffer rsn-buffer
   symbol-table
   indexes
   ^Map dbis
   ^longs keys-changed]
  ILookup
  (valAt [r k] (.valAt r k nil))
  (valAt [r k not-found]
    (if-some [{index-dbi-name :dbi-name} (indexes k)]
      (get-index r txn (.get dbis index-dbi-name) dbi key-buffer rsn-buffer k symbol-table keys-changed)
      not-found))
  p/Scannable
  (scan [rv f init]
    (with-open [cursor (.openCursor dbi txn)]
      (scan cursor f init symbol-table rv)))
  p/BigCount
  (big-count [_] (.-entries (.stat dbi txn)))
  IReduceInit
  (reduce [relvar f start]
    (p/scan relvar (fn [acc _ _ record] (f acc record)) start)))

(defn- commit [^Txn txn ^ByteBuffer key-buffer ^ByteBuffer val-buffer ^Map dbis symbol-table]
  (try
    (let [^ArrayList adds (codec/list-adds symbol-table)]
      (when (< 0 (.size adds))
        (let [^Dbi symbols-dbi (.get dbis symbols-dbi-name)
              rsn (inc (with-open [cursor (.openCursor symbols-dbi txn)] (last-rsn cursor)))]
          (dotimes [i (.size adds)]
            (let [symbol (.get adds i)]
              (insert symbols-dbi txn key-buffer val-buffer (+ rsn i) symbol nil))))))
    (.commit txn)
    (codec/clear-adds symbol-table)
    (catch Throwable t
      (try (.abort txn) (catch Txn$NotReadyException _))
      (codec/rollback-adds symbol-table)
      (throw t))))

(deftype LMDBWriteTransaction
  [^Env env
   ^Map varmap
   ^Map dbis
   ^Txn txn
   ^ByteBuffer key-buffer
   ^ByteBuffer rsn-buffer
   ^ByteBuffer val-buffer
   symbol-table
   ^Map var-cache
   ^longs keys-changed]
  ILookup
  (valAt [tx k] (.valAt tx k nil))
  (valAt [_ k not-found]
    (if-some [{:keys [dbi-name indexes auto-key]} (.get varmap k)]
      (->> (reify Function
             (apply [_ _]
               (let [^Dbi dbi (.get dbis dbi-name)
                     cursor (.openCursor dbi txn)
                     index-dbi-array (object-array (count indexes))
                     index-key-array (object-array (count indexes))
                     index-cursor-array (object-array (count indexes))
                     ictr (int-array 1)
                     _ (reduce-kv (fn [_ k {:keys [dbi-name]}]
                                    (let [i (aget ictr 0)
                                          ^Dbi dbi (.get dbis dbi-name)]
                                      (aset index-dbi-array i dbi)
                                      (aset index-key-array i k)
                                      (aset index-cursor-array i (.openCursor dbi txn))
                                      (aset ictr 0 (unchecked-inc i)))) nil indexes)]
                 (->LMDBWriteTransactionVariable env txn dbi
                                                 key-buffer
                                                 rsn-buffer
                                                 val-buffer
                                                 -1
                                                 symbol-table
                                                 indexes
                                                 dbis
                                                 cursor
                                                 index-dbi-array
                                                 index-key-array
                                                 index-cursor-array
                                                 keys-changed
                                                 auto-key))))
           (.computeIfAbsent var-cache k))
      not-found))
  p/Transaction
  (commit [t] (commit txn key-buffer val-buffer dbis symbol-table))
  Closeable
  (close [_]
    (.close txn)))

(deftype LMDBReadTransaction
  [^Env env
   ^Map varmap
   ^Map dbis
   ^Txn txn
   ^ByteBuffer key-buffer
   ^ByteBuffer rsn-buffer
   symbol-table
   keys-changed]
  ILookup
  (valAt [_ k]
    (when-some [{:keys [dbi-name, indexes]} (.get varmap k)]
      (->LMDBReadTransactionVariable
        env
        txn
        (.get dbis dbi-name)
        key-buffer
        rsn-buffer
        symbol-table
        indexes
        dbis
        keys-changed)))
  (valAt [tx k not-found] (or (get tx k) not-found))
  Closeable
  (close [_] (.close txn)))

(declare ->LMDBVariable)

(defn- intrinsic-rel [rel-fn]
  (reify
    p/Scannable
    (scan [_ f init]
      (p/scan (rel-fn) f init))
    IReduceInit
    (reduce [_ f init]
      (p/scan (rel-fn) (fn [acc _ _ r] (f acc r)) init))))

(defn- stat-rel [^Env env]
  (intrinsic-rel
    (fn []
      (let [env-stat (.stat env)
            ^EnvInfo env-info (.info env)]
        [{:last-page-number (.-lastPageNumber env-info)
          :last-transaction-id (.-lastTransactionId env-info)
          :map-address (.-mapAddress env-info)
          :map-size (.-mapSize env-info)
          :map-readers (.-maxReaders env-info)
          :num-readers (.-numReaders env-info)
          :branch-pages (.-branchPages env-stat)
          :depth (.-depth env-stat)
          :entries (.-depth env-stat)
          :leaf-pages (.-leafPages env-stat)
          :overflow-pages (.-overflowPages env-stat)
          :page-size (.-pageSize env-stat)}]))))

(declare create-index create-relvar with-resizing)

(def ^:dynamic *read-transactions* {})
(def ^:dynamic *write-transactions* {})

(defprotocol Resize
  (grow-mapsize [db]))

(deftype LMDBDatabase
  [file
   ^Env env
   ^ConcurrentHashMap dbis
   ^ConcurrentHashMap varmap
   ^ThreadLocal key-buffer
   ^ThreadLocal rsn-buffer
   ^ByteBuffer val-buffer
   auto-resize
   ^:unsynchronized-mutable ^long map-size
   symbol-table]
  p/Database
  (write-transaction [db f]
    (locking db
      (let [tx-f (fn []
                   (with-open [^Closeable tx
                               (->LMDBWriteTransaction
                                 env
                                 varmap
                                 dbis
                                 (.txnWrite env)
                                 (.get key-buffer)
                                 (.get rsn-buffer)
                                 val-buffer
                                 symbol-table
                                 (HashMap.)
                                 (long-array 1))]
                     (let [ret (binding [*write-transactions* (assoc *write-transactions* db tx)] (f tx))]
                       (p/commit tx)
                       ret)))]
        (if-not auto-resize
          (tx-f)
          (with-resizing db env tx-f)))))
  (read-transaction [db f]
    ;; lock can be removed once we have map resize read/write solution
    (locking db
      (with-open [^Closeable tx (->LMDBReadTransaction env
                                                       varmap
                                                       dbis
                                                       (.txnRead env)
                                                       (.get key-buffer)
                                                       (.get rsn-buffer)
                                                       symbol-table
                                                       (long-array 1))]
        (binding [*read-transactions* (assoc *read-transactions* db tx)] (f tx)))))
  (create-relvar [db k] (create-relvar db k)
    (.valAt ^ILookup db k))
  ILookup
  (valAt [db k]
    (case k
      :lmdb/stat (stat-rel env)
      :lmdb/variables (intrinsic-rel (fn [] (into [{:k :lmdb/stat} {:k :lmdb/variables}] (.values varmap))))
      :lmdb/symbols (intrinsic-rel (fn [] (remove nil? (codec/symbol-list symbol-table))))
      (when-some [{:keys [indexes]} (.get varmap k)]
        (->LMDBVariable db k key-buffer val-buffer indexes))))
  (valAt [db k not-found] (or (get db k) not-found))
  Resize
  (grow-mapsize [db]
    (let [new-map-size (* (.-map-size db) 2)]
      (.setMapSize env new-map-size)
      (set! (.-map-size db) new-map-size)))
  Closeable
  (close [_]
    (.close env)
    (alter-var-root #'open-files disj file)
    nil))

(defn- with-resizing [^LMDBDatabase db ^Env env f]
  (let [ret
        (try
          (f)
          (catch Env$MapFullException _e
            (grow-mapsize db)
            ::map-resized))]
    (if (identical? ::map-resized ret)
      (recur db env f)
      ret)))

(defn- variable-read [db k f]
  (if-some [tx (or (*write-transactions* db) (*read-transactions* db))]
    (f (.valAt ^ILookup tx k))
    (p/read-transaction db (fn [tx] (f (.valAt ^ILookup tx k))))))

(defn- variable-write [db k f]
  (if-some [tx (*write-transactions* db)]
    (f (.valAt ^ILookup tx k))
    (p/write-transaction db (fn [tx] (f (.valAt ^ILookup tx k))))))

(defn create-index [db relvar-key index-key]
  (let [^LMDBDatabase db db
        ^Map varmap (.-varmap db)
        ^Env env (.-env db)
        ^Map dbis (.-dbis db)
        ^ThreadLocal key-buffer (.-key-buffer db)
        ^ByteBuffer val-buffer (.-val-buffer db)
        symbol-table (.-symbol-table db)]
    (if-some [{:keys [indexes]} (.get varmap relvar-key)]
      (when-not (contains? indexes index-key)
        (locking db
          (let [^Dbi variables-dbi (.get dbis variables-dbi-name)
                {:keys [rsn, dbi-name, index-ctr, indexes] :as record} (.get varmap relvar-key)
                ^Dbi rsn-dbi (.get dbis dbi-name)
                _ (when (contains? indexes index-key)
                    (throw (ex-info "Could not create index, relvar does not exist" {:relvar-key relvar-key, :index-key index-key})))
                index-dbi-name (str dbi-name "." index-ctr)
                index-dbi (open-index-dbi env index-dbi-name)
                new-record (assoc record :indexes (assoc indexes index-key {:dbi-name index-dbi-name})
                                         :index-ctr (inc index-ctr))
                ^ByteBuffer key-buffer (.get key-buffer)]
            (with-resizing
              db
              env
              (fn []
                (with-open [txn (.txnWrite env)]
                  (replace variables-dbi txn key-buffer val-buffer rsn new-record symbol-table)

                  ;; initialise index value
                  (with-open [rsn-cursor (.openCursor rsn-dbi txn)
                              index-cursor (.openCursor index-dbi txn)]
                    (scan rsn-cursor
                          (fn [_ _ rsn o]
                            (when-some [key (get o index-key)]
                              (index-insert index-cursor key-buffer val-buffer rsn key symbol-table))
                            nil)
                          nil
                          symbol-table
                          nil))

                  (commit txn key-buffer val-buffer dbis symbol-table))))
            (.put dbis index-dbi-name index-dbi)
            (.put varmap relvar-key new-record))))
      (throw (ex-info "Could not create index, relvar does not exist" {:relvar-key relvar-key, :index-key index-key})))
    (let [variable (.valAt db relvar-key)]
      (.valAt ^ILookup variable index-key))))

(defn create-relvar [^LMDBDatabase db k]
  (let [^Map varmap (.-varmap db)
        ^Map dbis (.-dbis db)
        ^Env env (.-env db)
        ^ThreadLocal key-buffer (.-key-buffer db)
        ^ByteBuffer val-buffer (.-val-buffer db)
        symbol-table (.-symbol-table db)]
    (locking db
      (->> (fn []
             (when-not (.get varmap k)
               (let [^Dbi variable-dbi (.get dbis variables-dbi-name)
                     rsn (with-open [txn (.txnRead env)
                                     cursor (.openCursor variable-dbi txn)]
                           (inc (last-rsn cursor)))
                     dbi-name (str rsn)
                     dbi (open-dbi env dbi-name)
                     record {:k k, :rsn rsn, :dbi-name dbi-name, :index-ctr 0, :indexes {}}]
                 (with-open [txn (.txnWrite env)]
                   (insert variable-dbi txn (.get key-buffer) val-buffer rsn record symbol-table)
                   (commit txn (.get key-buffer) val-buffer dbis symbol-table))
                 (.put dbis dbi-name dbi)
                 (.put varmap k record))))
           (with-resizing db env))))
  (.valAt ^ILookup db k))

(defn set-auto-increment [^LMDBDatabase db relvar-key new-auto-key]
  (let [^LMDBDatabase db db
        ^Map varmap (.-varmap db)
        ^Env env (.-env db)
        ^Map dbis (.-dbis db)
        ^ThreadLocal key-buffer (.-key-buffer db)
        ^ByteBuffer val-buffer (.-val-buffer db)
        symbol-table (.-symbol-table db)]
    (if-some [{:keys [auto-key]} (.get varmap relvar-key)]
      (when (not= new-auto-key auto-key)
        (locking db
          (let [^Dbi variables-dbi (.get dbis variables-dbi-name)
                {:keys [rsn] :as record} (.get varmap relvar-key)
                new-record (assoc record :auto-key new-auto-key)
                ^ByteBuffer key-buffer (.get key-buffer)]
            (with-resizing
              db
              env
              (fn []
                (with-open [txn (.txnWrite env)]
                  (replace variables-dbi txn key-buffer val-buffer rsn new-record symbol-table)
                  (commit txn key-buffer val-buffer dbis symbol-table))))
            (.put varmap relvar-key new-record)
            nil)))
      (throw (ex-info "Could not assign auto-id, relvar does not exist" {:relvar-key relvar-key, :auto-key new-auto-key})))))

(deftype LMDBVariable
  [^LMDBDatabase db
   k
   ^ThreadLocal key-buffer
   ^ByteBuffer val-buffer
   ^:volatile-mutable indexes]
  AutoIncrementing
  (set-auto-increment [_ key]
    (set-auto-increment db k key))
  Indexable
  (index [_ indexed-key]
    (let [idx (create-index db k indexed-key)]
      (set! indexes (:indexes (get (.-varmap db) k)))
      idx))
  ILookup
  (valAt [r indexed-key] (.valAt r indexed-key nil))
  (valAt [r indexed-key not-found]
    (if (indexes indexed-key)
      (reify
        IFn
        (invoke [i k] (.valAt i k))
        (invoke [i k not-found] (.valAt i k not-found))
        p/Scannable
        (scan [_ f init] (variable-read db k (fn [v] (p/scan (.valAt ^ILookup v indexed-key) (proxy-scan-function r f) init))))
        IReduceInit
        (reduce [index f init]
          (reduce-scan index f init))
        ILookup
        (valAt [_ entry-key]
          (reify
            p/Scannable
            (scan [_ f init]
              (variable-read
                db k
                (fn [v]
                  (let [^ILookup idx (.valAt ^ILookup v indexed-key)
                        entry (.valAt idx entry-key)]
                    (p/scan entry (proxy-scan-function r f) init)))))
            IReduceInit
            (reduce [index-entry f init]
              (reduce-scan index-entry f init))))
        (valAt [r k _not-found] (.valAt r k))
        p/Index
        (indexed-key [_] indexed-key)
        (get1 [idx k not-found] (c/rel-first (.valAt idx k) not-found))
        (getn [idx k] (.valAt idx k))
        (range-scan [_ test-a a test-b b]
          (reify p/Scannable
            (scan [_ f init]
              (variable-read
                db k
                (fn [v]
                  (let [^ILookup idx (.valAt ^ILookup v indexed-key)
                        entry (p/range-scan idx test-a a test-b b)]
                    (p/scan entry (proxy-scan-function r f) init)))))
            IReduceInit
            (reduce [index-entry f init]
              (reduce-scan index-entry f init))))
        (sorted-scan [_ high]
          (reify p/Scannable
            (scan [_ f init]
              (variable-read
                db k
                (fn [v]
                  (let [^ILookup idx (.valAt ^ILookup v indexed-key)
                        entry (p/sorted-scan idx high)]
                    (p/scan entry (proxy-scan-function r f) init)))))
            IReduceInit
            (reduce [index-entry f init]
              (reduce-scan index-entry f init)))))
      not-found))
  p/Scannable
  (scan [r f init] (variable-read db k (fn [v] (p/scan v (proxy-scan-function r f) init))))
  IReduceInit
  (reduce [relvar f start] (p/scan relvar (fn [acc _ _ record] (f acc record)) start))
  p/BigCount
  (big-count [_] (variable-read db k p/big-count))
  p/Relvar
  (rel-set [_ rel] (variable-write db k (fn [v] (p/rel-set v rel))))
  p/IncrementalRelvar
  (insert [_ record] (variable-write db k (fn [v] (p/insert v record))))
  (delete [_ rsn] (variable-write db k (fn [v] (p/delete v rsn)))))

(defn database [file & {:keys [max-variables
                               map-size
                               auto-resize],
                        :or {max-variables 4096}}]
  (let [file (io/file file)
        auto-resize (if (nil? map-size) true (boolean auto-resize))
        map-size (or map-size (* 64 1024 1024))]
    (when-not (.exists file) (.mkdirs (.getParentFile file)))
    (alter-var-root #'open-files
                    (fn [files]
                      (if (contains? files file)
                        (throw (IllegalArgumentException. "File already open"))
                        (conj files file))))
    (try
      (let [env (-> (Env/create)
                    (.setMapSize map-size)
                    (.setMaxDbs max-variables)
                    (.open file (doto ^"[Lorg.lmdbjava.EnvFlags;" (make-array EnvFlags 1)
                                  (aset 0 EnvFlags/MDB_NOSUBDIR))))

            symbol-dbi (open-dbi env symbols-dbi-name)
            variables-dbi (open-dbi env variables-dbi-name)

            dbis (let [dbis (ConcurrentHashMap.)]
                   (doseq [^bytes dbi-bytes (.getDbiNames env)
                           :let [dbi-name (String. dbi-bytes "UTF-8")
                                 dbi (if (index-dbi? dbi-name)
                                       (open-index-dbi env dbi-name)
                                       (open-dbi env dbi-name))]]
                     (.put dbis dbi-name dbi))
                   dbis)

            symbol-table (codec/empty-symbol-table)
            varmap (ConcurrentHashMap.)]

        (with-open [txn (.txnRead env)]

          ;; read symbols
          (with-open [cursor (.openCursor symbol-dbi txn)]
            (scan cursor (fn [_ _ _ symbol] (codec/intern-symbol symbol-table symbol true)) nil nil nil))

          ;; read variables
          (with-open [cursor (.openCursor variables-dbi txn)]
            (scan cursor (fn [_ _ _ {:keys [k] :as record}] (.put varmap k record)) nil symbol-table nil)))

        (codec/clear-adds symbol-table)
        (->LMDBDatabase file
                        env
                        dbis
                        varmap
                        (ThreadLocal/withInitial (reify Supplier (get [_] (ByteBuffer/allocateDirect 511))))
                        (ThreadLocal/withInitial (reify Supplier (get [_] (ByteBuffer/allocateDirect 8))))
                        (ByteBuffer/allocateDirect (* 32 1024 1024))
                        auto-resize
                        map-size
                        symbol-table))
      (catch Throwable t
        (alter-var-root #'open-files disj file)
        (throw t)))))

(comment
  (set! *print-length* 100)

  (io/delete-file "tmp/foo" true)

  (def db (database "tmp/foo"))
  (.close db)

  (:foo db)
  (time (c/agg 0 n (:foo db) (unchecked-inc n)))

  (c/create db :foo)
  ;;todo
  (c/drop db :foo)

  (c/rel-set (:foo db) (range 1e6))
  (c/rel-set (:foo db) nil)
  (c/insert (:foo db) (str (random-uuid)))
  (c/run [f (:foo db) :when (= 42 f)] (c/replace f "The answer"))
  (c/rel [f (:foo db) :when (string? f) :limit 10] f)
  (c/del-key (:foo db) string? true)
  (c/run [f (:foo db) :when (string? f)] (delete f))
  (c/write [db db] (c/run [f (:foo db) :when (even? f)] (c/replace f 42)))
  (c/rel [f (:foo db) :when (odd? f) :limit 10] f)

  (:foo db)

  (:lmdb/stat db)
  (:lmdb/variables db)
  (:lmdb/symbols db)

  (do
    (c/create db :foo)
    (c/rel-set (:foo db) [{:id 42}])
    (c/rel-set (:foo db) [{:id 44} {:id 42}, {:id 43}])
    (c/insert (:foo db) {:id 44})
    (c/run [f (:foo db) :when (= 42 f:id)] (c/delete f))
    (:foo db)
    (-> db :foo (c/index :id))
    (:id (:foo db))
    (get (:id (:foo db)) 42)
    (get (:id (:foo db)) 43)
    (get (:id (:foo db)) 44)
    (-> db :foo :id (c/getn 44))
    (-> db :foo :id (c/get1 44))
    (-> db :foo :id (c/get1 45 ::not-found))

    (c/range (:id (:foo db)) > 0)
    (c/range (:id (:foo db)) < 44)
    (c/range (:id (:foo db)) < 44 >= 42)
    (c/range (:id (:foo db)) > 40 <= 42)


    (c/rel-set (:foo db) (for [i (range 1024)] {:id i}))
    (:foo db)

    )

  (require 'criterium.core)
  (criterium.core/quick-bench (c/rel [i (:foo db) :limit 10] i))
  (criterium.core/quick-bench (c/rel [i (:foo db) :limit 10] i))
  (criterium.core/quick-bench (vec (:foo db)))

  (def sf005 ((requiring-resolve 'com.wotbrew.cinq.tpch-test/all-tables) 0.05))

  (doseq [[table coll] sf005]
    (println table)
    (c/create db table)
    (time (c/rel-set (get db table) coll)))

  (-> db :lineitem (c/index :orderkey))

  (time (count (vec (:lineitem db))))
  (time (count (vec (:orderkey (:lineitem db)))))

  ((requiring-resolve 'clj-async-profiler.core/serve-ui) "localhost" 5000)
  ((requiring-resolve 'clojure.java.browse/browse-url) "http://127.0.0.1:5000")

  (clj-async-profiler.core/profile
    (criterium.core/quick-bench
      (c/agg nil _ [li (:lineitem db)] nil)
      )
    )

  (criterium.core/quick-bench (c/agg nil _ [li (:lineitem sf005)] nil))

  (defn q1 [{:keys [lineitem]}]
    (c/rel [l lineitem
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
    (c/rel [r region
          n nation
          s supplier
          p part
          ps partsupp
          :when
          (and
            (= p:partkey ps:partkey)
            (= s:suppkey ps:suppkey)
            (= p:size 15)
            (clojure.string/ends-with? p:type "BRASS")
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

  (c/rel [l (:lineitem db)
        :group [pred-match (<= l:shipdate #inst "1998-09-02")]]
         [pred-match (c/count)])

  (clj-async-profiler.core/profile
    (criterium.core/quick-bench
      (c/rel-count (q1 db))
      )
    )

  (clj-async-profiler.core/profile
    (criterium.core/quick-bench
      (c/rel-count (q2 db))
      )
    )

  (:lmdb/symbols db)
  (c/rel-first (:partsupp db))

  (= (vec (q1 db)) (q1 sf005))

  (q2 sf005)
  (time (count (q2 db)))
  (time (count (q2 sf005)))

  (= (q2 db) (q2 sf005))

  )

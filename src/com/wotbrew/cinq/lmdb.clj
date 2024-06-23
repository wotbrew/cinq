(ns com.wotbrew.cinq.lmdb
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.nio-codec :as codec]
            [com.wotbrew.cinq.protocols :as p])
  (:import (clojure.lang ILookup IReduceInit Reduced)
           (com.wotbrew.cinq CinqUtil)
           (java.io Closeable)
           (java.nio ByteBuffer)
           (java.util ArrayList HashMap Map)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Function Supplier)
           (org.lmdbjava Cursor Dbi DbiFlags Env Env$MapFullException EnvFlags EnvInfo GetOp PutFlags Txn))
  (:refer-clojure :exclude [replace]))

(set! *warn-on-reflection* true)

(def ^:redef open-files #{})

(defn- scan [^Cursor cursor f init symbol-list]
  (if-not (.first cursor)
    init
    (loop [acc init]
      (let [rsn (.getLong ^ByteBuffer (.key cursor))
            o (codec/decode-object (.val cursor) symbol-list)
            ret (f acc rsn o)]
        (if (reduced? ret)
          @ret
          (if (.next cursor)
            (recur ret)
            ret))))))

(defn- last-rsn ^long [^Cursor cursor]
  (if (.last cursor)
    (let [^ByteBuffer k (.key cursor)
          rsn (.getLong k)]
      rsn)
    -1))

(defn- reduce-scan [r f init]
  (p/scan r (fn [acc _ x] (f acc x)) init))

(defn get-rel [^Txn txn ^Dbi dbi symbol-table]
  (reify p/Scannable
    (scan [_ f init]
      (with-open [cursor (.openCursor dbi txn)]
        (scan cursor f init (codec/symbol-list symbol-table))))
    IReduceInit
    (reduce [r f init] (reduce-scan r f init))))

(defn get-index-entry [^Txn txn ^Dbi dbi ^Dbi rsn-dbi ^ByteBuffer key-buffer symbol-table k]
  (reify p/Scannable
    (scan [_ f init]
      (with-open [cursor (.openCursor dbi txn)]
        (.clear key-buffer)
        (codec/encode-key k key-buffer)
        (.flip key-buffer)
        (if-not (.get cursor key-buffer GetOp/MDB_SET)
          init
          (with-open [rsn-cursor (.openCursor rsn-dbi txn)]
            (loop [acc init]
              (when-not (.get rsn-cursor (.val cursor) GetOp/MDB_SET)
                (throw (IllegalStateException. "Should always find an rsn if referenced by an index")))
              (let [r (f acc
                         (.getLong ^ByteBuffer (.key rsn-cursor))
                         (codec/decode-object (.val rsn-cursor) (codec/symbol-list symbol-table)))]
                (if (reduced? r)
                  @r
                  (if (and (.next cursor)
                           ;; same key
                           (= 0 (.compareTo key-buffer (.key cursor))))
                    (recur r)
                    acc))))))))
    IReduceInit
    (reduce [index-entry f init]
      (reduce-scan index-entry f init))))

(defn scan-index [^Dbi dbi ^Txn txn f init]
  (with-open [cursor (.openCursor dbi txn)]
    (if (.first cursor)
      (loop [acc init]
        (let [k (codec/decode-key (.key cursor))
              rsn (.getLong ^ByteBuffer (.val cursor))
              ret (f acc rsn k)]
          (if (instance? Reduced ret)
            @ret
            (if (.next cursor)
              (recur ret)
              ret))))
      init)))

(defn get-index [^Txn txn ^Dbi dbi ^Dbi rsn-dbi ^ByteBuffer key-buffer symbol-table]
  (reify p/Scannable
    (scan [_ f init] (scan-index dbi txn f init))
    IReduceInit
    (reduce [index f init]
      (reduce-scan index f init))
    ILookup
    (valAt [_ k] (get-index-entry txn dbi rsn-dbi key-buffer symbol-table k))
    (valAt [r k _not-found] (.valAt r k))))

(def ^:private ^"[Lorg.lmdbjava.PutFlags;" insert-flags
  (make-array PutFlags 0))

(def ^:private ^"[Lorg.lmdbjava.PutFlags;" replace-flags
  (doto ^"[Lorg.lmdbjava.PutFlags;" (make-array PutFlags 1)
    (aset 0 PutFlags/MDB_CURRENT)))

(defn- index-insert [^Cursor index-cursor ^ByteBuffer key-buffer ^ByteBuffer val-buffer rsn key symbol-table]
  (.clear key-buffer)
  (.clear val-buffer)
  (codec/encode-key key key-buffer)
  ;; I think it is the case the object + rsn must fit in the buffer, as any non-nil object that gets returned as a key
  ;; must be smaller than the 'associative' that contained it by at least 8 bytes.
  (.putLong val-buffer rsn)
  ;; add this once get-index-entry loop rewritten to check for eq the key
  #_(codec/encode-object record val-buffer symbol-table)
  (.put index-cursor (.flip key-buffer) (.flip val-buffer) insert-flags))

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
  (let [reduced (reduced ::not-enough-space-in-write-buffer)
        step (fn [_acc rsn r]
               (.clear key-buffer)
               (.clear val-buffer)
               (.putLong key-buffer rsn)
               (if-not (codec/encode-object r val-buffer symbol-table)
                 reduced
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
      (if-not (codec/encode-object record val-buffer symbol-table)
        (throw (ex-info "Not enough space in write-buffer during insert" {:cinq/error ::not-enough-space-in-write-buffer}))
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
    (if-not (codec/encode-object record val-buffer symbol-table)
      (throw (ex-info "Not enough space in write-buffer during replace" {:cinq/error ::not-enough-space-in-write-buffer}))
      (if (.get cursor (.flip key-buffer) GetOp/MDB_SET)
        (.put cursor
              key-buffer
              (.flip val-buffer)
              replace-flags)
        false))))

(defn open-dbi ^Dbi [^Env env ^String dbi-name]
  (let [flags (into-array DbiFlags [DbiFlags/MDB_CREATE DbiFlags/MDB_INTEGERKEY])]
    (.openDbi env dbi-name ^"[Lorg.lmdbjava.DbiFlags;" flags)))

(defn index-dbi? [dbi-name] (str/includes? dbi-name "."))

(defn open-index-dbi ^Dbi [^Env env ^String dbi-name]
  (let [flags (into-array DbiFlags [DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT])]
    (.openDbi env dbi-name ^"[Lorg.lmdbjava.DbiFlags;" flags)))

(def ^:const symbols-dbi-name "$symbols")
(def ^:const variables-dbi-name "$variables")

(deftype LMDBWriteTransactionVariable
  [^Env env
   ^Txn txn
   ^Dbi dbi
   ^ByteBuffer key-buffer
   ^ByteBuffer val-buffer
   ^:unsynchronized-mutable next-rsn
   symbol-table
   indexes
   ^Map dbis
   ^Cursor cursor
   ^objects index-dbi-array
   ^objects index-key-array
   ^objects index-cur-array]
  ILookup
  (valAt [r k] (.valAt r k nil))
  (valAt [r k not-found]
    (if-some [{index-dbi-name :dbi-name} (indexes k)]
      (get-index txn (.get dbis index-dbi-name) dbi key-buffer symbol-table)
      not-found))
  p/Scannable
  (scan [_ f init]
    (scan cursor f init (codec/symbol-list symbol-table)))
  p/BigCount
  (big-count [_] (.-entries (.stat dbi txn)))
  IReduceInit
  (reduce [relvar f start]
    (p/scan relvar (fn [acc _ record] (f acc record)) start))
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
   symbol-table
   indexes
   ^Map dbis]
  ILookup
  (valAt [r k] (.valAt r k nil))
  (valAt [r k not-found]
    (if-some [{index-dbi-name :dbi-name} (indexes k)]
      (get-index txn (.get dbis index-dbi-name) dbi key-buffer symbol-table)
      not-found))
  p/Scannable
  (scan [_ f init]
    (with-open [cursor (.openCursor dbi txn)]
      (scan cursor f init (codec/symbol-list symbol-table))))
  p/BigCount
  (big-count [_] (.-entries (.stat dbi txn)))
  IReduceInit
  (reduce [relvar f start]
    (p/scan relvar (fn [acc _ record] (f acc record)) start)))

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
      (codec/rollback-adds symbol-table)
      (throw t))))

(deftype LMDBWriteTransaction
  [^Env env
   ^Map varmap
   ^Map dbis
   ^Txn txn
   ^ByteBuffer key-buffer
   ^ByteBuffer val-buffer
   symbol-table
   ^Map var-cache]
  ILookup
  (valAt [tx k] (.valAt tx k nil))
  (valAt [_ k not-found]
    (if-some [{:keys [dbi-name indexes]} (.get varmap k)]
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
                                                 val-buffer
                                                 -1
                                                 symbol-table
                                                 indexes
                                                 dbis
                                                 cursor
                                                 index-dbi-array
                                                 index-key-array
                                                 index-cursor-array))))
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
   symbol-table]
  ILookup
  (valAt [_ k]
    (when-some [{:keys [dbi-name, indexes]} (.get varmap k)]
      (->LMDBReadTransactionVariable
        env
        txn
        (.get dbis dbi-name)
        key-buffer
        symbol-table
        indexes
        dbis)))
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
      (p/scan (rel-fn) (fn [acc _ r] (f acc r)) init))))

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

(deftype LMDBDatabase
  [file
   ^Env env
   ^ConcurrentHashMap dbis
   ^ConcurrentHashMap varmap
   ^ThreadLocal key-buffer
   ^ByteBuffer val-buffer
   auto-resize
   ^:unsynchronized-mutable ^long map-size
   symbol-table]
  p/Database
  (write-transaction [db f]
    (if-not auto-resize
      (locking db
        (with-open [^Closeable tx
                    (->LMDBWriteTransaction
                      env
                      varmap
                      dbis
                      (.txnWrite env)
                      (.get key-buffer)
                      val-buffer
                      symbol-table
                      (HashMap.))]
          (let [ret (f tx)]
            (p/commit tx)
            ret)))
      (let [ret (locking db
                  (try
                    (with-open [^Closeable tx
                                (->LMDBWriteTransaction
                                  env
                                  varmap
                                  dbis
                                  (.txnWrite env)
                                  (.get key-buffer)
                                  val-buffer
                                  symbol-table
                                  (HashMap.))]
                      (let [ret (f tx)]
                        (p/commit tx)
                        ret))
                    (catch Env$MapFullException _e
                      (let [new-map-size (* (.-map-size db) 2)]
                        (.setMapSize env new-map-size)
                        (set! (.-map-size db) new-map-size)
                        ::map-resized))))]
        (if (identical? ::map-resized ret)
          (recur f)
          ret))))
  (read-transaction [_ f]
    (with-open [^Closeable tx (->LMDBReadTransaction env varmap dbis (.txnRead env) (.get key-buffer) symbol-table)]
      (f tx)))
  (create-relvar [db k]
    (when-not (.get varmap k)
      (locking db
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
            (.put varmap k record)))))
    (.valAt ^ILookup db k))
  (create-index [db relvar-key index-key]
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
            (with-open [txn (.txnWrite env)]
              (replace variables-dbi txn key-buffer val-buffer rsn new-record symbol-table)

              ;; initialise index value
              (with-open [rsn-cursor (.openCursor rsn-dbi txn)
                          index-cursor (.openCursor index-dbi txn)]
                (scan rsn-cursor
                      (fn [_ rsn o]
                        (when-some [key (get o index-key)]
                          (index-insert index-cursor key-buffer val-buffer rsn key symbol-table))
                        nil)
                      nil
                      (codec/symbol-list symbol-table)))

              (commit txn key-buffer val-buffer dbis symbol-table))
            (.put dbis index-dbi-name index-dbi)
            (.put varmap relvar-key new-record))))
      (throw (ex-info "Could not create index, relvar does not exist" {:relvar-key relvar-key, :index-key index-key})))
    (let [variable (.valAt db relvar-key)]
      (.valAt ^ILookup variable index-key)))
  ILookup
  (valAt [db k]
    (case k
      :lmdb/stat (stat-rel env)
      :lmdb/variables (intrinsic-rel (fn []
                                       (let [all (into [{:k :lmdb/stat} {:k :lmdb/variables}] (.values varmap))
                                             {comparable true
                                              not-comparable false} (group-by #(instance? Comparable (:k %)) all)]
                                         (concat (sort-by :k comparable) not-comparable))))
      :lmdb/symbols (intrinsic-rel (fn [] (sort (remove nil? (codec/symbol-list symbol-table)))))
      (when-some [{:keys [indexes]} (.get varmap k)]
        (->LMDBVariable db k key-buffer val-buffer indexes))))
  (valAt [db k not-found] (or (get db k) not-found))
  Closeable
  (close [_]
    (.close env)
    (alter-var-root #'open-files disj file)
    nil))

(defn- variable-read [db k f]
  (p/read-transaction db (fn [tx] (f (.valAt ^ILookup tx k)))))

(defn- variable-write [db k f]
  (p/write-transaction db (fn [tx] (f (.valAt ^ILookup tx k)))))

(deftype LMDBVariable
  [^LMDBDatabase db
   k
   ^ThreadLocal key-buffer
   ^ByteBuffer val-buffer
   indexes]
  ILookup
  (valAt [r index-key] (.valAt r index-key nil))
  (valAt [_ index-key not-found]
    (if (indexes index-key)
      (reify p/Scannable
        (scan [_ f init] (variable-read db k (fn [v] (p/scan (.valAt ^ILookup v index-key) f init))))
        IReduceInit
        (reduce [index f init]
          (reduce-scan index f init))
        ILookup
        (valAt [_ entry-key]
          (reify p/Scannable
            (scan [_ f init]
              (variable-read
                db k
                (fn [v]
                  (let [^ILookup idx (.valAt ^ILookup v index-key)
                        entry (.valAt idx entry-key)]
                    (p/scan entry f init)))))
            IReduceInit
            (reduce [index-entry f init]
              (reduce-scan index-entry f init))))
        (valAt [r k _not-found] (.valAt r k)))
      not-found))
  p/Scannable
  (scan [_ f init] (variable-read db k (fn [v] (p/scan v f init))))
  IReduceInit
  (reduce [relvar f start] (p/scan relvar (fn [acc _ record] (f acc record)) start))
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
            (scan cursor (fn [_ _ symbol] (codec/intern-symbol symbol-table symbol)) nil nil))

          ;; read variables
          (with-open [cursor (.openCursor variables-dbi txn)]
            (scan cursor (fn [_ _ {:keys [k] :as record}] (.put varmap k record)) nil (codec/symbol-list symbol-table))))

        (codec/clear-adds symbol-table)
        (->LMDBDatabase file
                        env
                        dbis
                        varmap
                        (ThreadLocal/withInitial (reify Supplier (get [_] (ByteBuffer/allocateDirect 512))))
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
  (c/update [f (:foo db) :when (= f 42)] "The answer")
  (c/q [f (:foo db) :when (string? f) :limit 10] f)
  (c/delete [f (:foo db) :when (string? f)])
  (c/write [db db] (c/update [f (:foo db) :when (even? f)] 42))
  (c/q [f (:foo db) :when (odd? f) :limit 10] f)

  (:foo db)

  (:lmdb/stat db)
  (:lmdb/variables db)
  (:lmdb/symbols db)

  (do
    (c/create db :foo)
    (c/rel-set (:foo db) [{:id 42}])
    (c/rel-set (:foo db) [{:id 42}, {:id 43}])
    (c/insert (:foo db) {:id 44})
    (c/delete [f (:foo db) :when (= 42 f:id)])
    (:foo db)
    (p/create-index db :foo :id)
    (:id (:foo db))
    (get (:id (:foo db)) 42)
    (get (:id (:foo db)) 43)
    (get (:id (:foo db)) 44)

    )

  (require 'criterium.core)
  (criterium.core/quick-bench (c/q [i (:foo db) :limit 10] i))
  (criterium.core/quick-bench (c/q [i (:foo db) :limit 10] i))
  (criterium.core/quick-bench (vec (:foo db)))

  (def sf005 ((requiring-resolve 'com.wotbrew.cinq.tpch-test/all-tables) 0.05))

  (doseq [[table coll] sf005]
    (println table)
    (c/create db table)
    (time (c/rel-set (get db table) coll)))

  (p/create-index db :lineitem :orderkey)

  (time (count (vec (:lineitem db))))

  ((requiring-resolve 'clj-async-profiler.core/serve-ui) "localhost" 5001)
  ((requiring-resolve 'clojure.java.browse/browse-url) "http://127.0.0.1:5001")

  (clj-async-profiler.core/profile
    (criterium.core/quick-bench
      (count (vec (:lineitem db)))
      ))

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

  (c/q [l (:lineitem db)
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

  (= (vec db) (q1 sf005))

  (q2 sf005)
  (time (count (q2 db)))
  (time (count (q2 sf005)))

  (= (q2 db) (q2 sf005))

  )

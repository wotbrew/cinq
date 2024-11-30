(ns com.wotbrew.cinq.lmdb-table
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [com.wotbrew.cinq.lmdb-kv :as kv]
            [com.wotbrew.cinq.nio-codec :as codec]
            [com.wotbrew.cinq.protocols :as p])
  (:import (clojure.lang ExceptionInfo IDeref ILookup IReduceInit)
           (java.io Closeable File)
           (java.nio ByteBuffer)
           (java.util HashMap Map)
           (org.lmdbjava Cursor Dbi DbiFlags Env Env$MapFullException EnvFlags Txn)))

(set! *warn-on-reflection* true)

(defn- do-reduce [^Txn txn ^Dbi data ^ByteBuffer lmdb-key-buffer symbol-table primary-index f init]
  (with-open [cursor (.openCursor data txn)]
    (let [sl (codec/symbol-list symbol-table)
          _ (.clear lmdb-key-buffer)
          _ (codec/encode-interned primary-index lmdb-key-buffer symbol-table false)]
      (kv/scan cursor
               symbol-table
               (fn [acc index ^ByteBuffer k ^ByteBuffer v]
                 (if-not (identical? index primary-index)
                   (reduced acc)
                   (f acc (codec/decode-object k sl) (codec/decode-object v sl))))
               init
               (.flip lmdb-key-buffer)
               true))))

(def state-primary-index
  (codec/map->SingleKeyIndex
    {:table-id :cinq/state
     :kind :primary
     :key :k}))

(def symbol-table-primary-index
  (codec/map->SingleKeyIndex
    {:table-id :cinq/symbol-table
     :kind :primary
     :key :offset}))

(defn- do-del [^Cursor cursor lmdb-key-buffer write-butter symbol-table primary-index ^objects indexes primary-key]
  (when-some [^ByteBuffer existing-raw (kv/del cursor lmdb-key-buffer write-butter symbol-table primary-index primary-key true)]
    (let [existing (codec/decode-object existing-raw (codec/symbol-list symbol-table))]
      (dotimes [i (alength indexes)]
        (let [index (aget indexes i)
              old-indexed-key (codec/get-indexed-key index primary-key existing)]
          (kv/del cursor lmdb-key-buffer write-butter symbol-table index old-indexed-key false))))))

(defn- do-put [^Cursor cursor lmdb-key-buffer write-buffer symbol-table primary-index ^objects indexes rsn record]
  (let [primary-key (codec/get-indexed-key primary-index rsn record)]
    (if-some [^ByteBuffer existing-raw (kv/put cursor lmdb-key-buffer write-buffer symbol-table primary-index primary-key record (some? indexes))]
      (when indexes
        (let [existing (codec/decode-object existing-raw (codec/symbol-list symbol-table))]
          (dotimes [i (alength indexes)]
            (let [index (aget indexes i)
                  old-indexed-key (codec/get-indexed-key index primary-key existing)
                  new-indexed-key (codec/get-indexed-key index primary-key record)]
              (kv/del cursor lmdb-key-buffer write-buffer symbol-table index old-indexed-key false)
              (kv/put cursor lmdb-key-buffer write-buffer symbol-table index new-indexed-key (codec/get-indexed-val index primary-key record) false)))))
      (when indexes
        (dotimes [i (alength indexes)]
          (let [index (aget indexes i)
                new-indexed-key (codec/get-indexed-key index primary-key record)]
            (kv/put cursor lmdb-key-buffer write-buffer symbol-table index new-indexed-key (codec/get-indexed-val index primary-key record) false)))))))

(defn- preput [auto-inc rsn record]
  (if (some? auto-inc)
    (assoc record auto-inc rsn)
    record))

(declare do-reindex drop-index)

(deftype LMDBTransactionVariable [^Env env
                                  ^Dbi data
                                  ^:unsynchronized-mutable primary-index
                                  ^:unsynchronized-mutable indexes
                                  symbol-table
                                  ^Txn txn
                                  ^Cursor cursor
                                  ^ByteBuffer lmdb-key-buffer
                                  ^ByteBuffer write-buffer
                                  ^:unsynchronized-mutable auto-inc
                                  rsn-counter
                                  structure-ref
                                  table-id
                                  ;; so we can use same variable type for system relations
                                  ;; possibly use relations could later be read-only
                                  read-only
                                  ;; when reading we need to disallow reindexes or structural changes
                                  ^:unsynchronized-mutable reading
                                  ;; aborting is needed in case an operation like put/delete might
                                  ;; have partially succeeded, which could lead to inconsistent index state
                                  ;; we need to ensure the index corruption does not get committed
                                  aborted-ref
                                  ;; used as a fast-path for aborted-ref for this relation
                                  ^:unsynchronized-mutable aborted]
  IReduceInit
  (reduce [_ f init]
    (some-> aborted throw)
    (set! reading true)
    (try
      (do-reduce txn data lmdb-key-buffer symbol-table primary-index (fn [acc _k v] (f acc v)) init)
      (finally (set! reading false))))
  p/Indexed
  (reindex [_ f]
    (some-> aborted throw)
    (when reading (throw (ex-info "Cannot reindex table while reading" {:error ::reindex-while-reading})))
    (when read-only (throw (ex-info "Called (reindex) on read-only table" {:error ::read-only-violation, :op :reindex})))
    (let [existing-structure (get @structure-ref table-id)
          new-structure (f existing-structure)]
      (do-reindex existing-structure new-structure)
      (set! primary-index (:primary new-structure))
      (set! indexes (some-> (:indexes new-structure) object-array))
      (set! auto-inc (:auto-inc new-structure))))
  p/Relvar
  (rel-set [txv rel]
    (some-> aborted throw)
    #_(when reading (throw (ex-info "Called (rel-set) while reading" {:error ::write-on-read, :op :rel-set})))
    (when read-only (throw (ex-info "Called (rel-set) on read-only table" {:error ::read-only-violation, :op :rel-set})))
    (doseq [index (cons primary-index indexes)] (drop-index txv index))
    (reduce (fn [_ record] (p/insert txv record)) nil rel))
  p/IncrementalRelvar
  (insert [_ record]
    (some-> aborted throw)
    #_(when reading (throw (ex-info "Called (insert) while reading" {:error ::write-on-read, :op :insert})))
    (when read-only (throw (ex-info "Called (insert) on read-only table" {:error ::read-only-violation, :op :insert, :record record})))
    (try
      (let [rsn (vswap! rsn-counter inc)]
        (do-put cursor lmdb-key-buffer write-buffer symbol-table primary-index indexes rsn (preput auto-inc rsn record)))
      (catch Throwable t
        (set! aborted t)
        (vreset! aborted-ref t)
        (throw t))))
  (delete [_ primary-key]
    (some-> aborted throw)
    #_(when reading (throw (ex-info "Called (delete) while reading" {:error ::write-on-read, :op :delete})))
    (when read-only (throw (ex-info "Called (delete) on read-only table" {:error ::read-only-violation, :op :delete, :primary-key primary-key})))
    (try
      (do-del cursor lmdb-key-buffer write-buffer symbol-table primary-index indexes primary-key)
      (catch Throwable t
        (set! aborted t)
        (vreset! aborted-ref t)
        (throw t)))))

(deftype LMDBVariable [db
                       table-id]
  IReduceInit
  (reduce [_ f init] (p/read-transaction db (fn [tx] (reduce f init (get tx table-id)))))
  p/Relvar
  (rel-set [_ rel] (p/write-transaction db (fn [tx] (p/rel-set (get tx table-id) rel))))
  p/IncrementalRelvar
  (insert [_ record]
    ;; if you are in an update/delete loop
    ;; can we 'attach' an open transaction to the query context
    ;; and install commit/abort hooks?
    ;; because otherwise if you (update) database relvars its gonna be really slow
    ;; although, is it surprising to do this? what if you want the
    ;; updates to be discrete?
    ;; option: we could have (update) (replace) etc expand to specific transaction-only calls?
    (p/write-transaction db (fn [tx] (p/insert (get tx table-id) record))))
  (delete [_ primary-key] (p/write-transaction db (fn [tx] (p/delete (get tx table-id) primary-key)))))

(defn- throw-if-already-exists [tables relvar-key]
  (when-some [ex (tables relvar-key)]
    (throw (ex-info "Table already exists" {:error :cinq.error/table-already-exists
                                            :table ex}))))

(defn get-table [^Cursor system-cursor lmdb-key-buffer write-buffer symbol-table table-id]
  )

(deftype LMDBTransaction
  [^Env env
   ^Dbi data
   ^Map tables-cache
   ^Txn txn
   ^Cursor cursor
   ^ByteBuffer lmdb-key-buffer
   write-buffer
   symbol-table
   symbol-offset-ref
   rsn-counter
   structure-ref
   aborted-ref]
  p/Database
  (create-relvar [db table-id]
    (or (get db table-id)
        (do (vswap! structure-ref assoc table-id {:primary (codec/->RsnIndex table-id :primary), :indexes #{}})
            (get db table-id))))
  (write-transaction [db f]
    (if (.isReadOnly txn)
      (throw (ex-info "Cannot promote read transaction to write transaction" {:error ::bad-transaction-type}))
      (f db)))
  (read-transaction [db f] (f db))
  ILookup
  (valAt [_ k not-found]
    (or (.get tables-cache k)
        (if-some [table-structure (get @structure-ref k)]
          (let [tx-var (->LMDBTransactionVariable
                         env
                         data
                         (:primary table-structure)
                         (object-array (:indexes table-structure))
                         symbol-table
                         txn
                         cursor
                         lmdb-key-buffer
                         write-buffer
                         (:auto-inc table-structure)
                         rsn-counter
                         structure-ref
                         k
                         false
                         false
                         aborted-ref
                         nil)]
            (.put tables-cache k tx-var)
            tx-var)
          not-found)))
  (valAt [tx k] (.valAt tx k nil)))

(defn- drop-index [^LMDBTransactionVariable txv index]
  (kv/drop-index (.cursor txv) (.-lmdb-key-buffer txv) (.-symbol-table txv) index))

(defn- do-reindex [^LMDBTransactionVariable txv existing-structure new-structure]
  (let [^Txn txn (.-txn txv)
        ^Dbi data (.-data txv)
        symbol-table (.-symbol-table txv)
        lmdb-key-buffer (.-lmdb-key-buffer txv)
        write-buffer (.-write-buffer txv)]
    (cond
      (= existing-structure new-structure) nil
      ;; incremental reindex
      (and (= (:primary existing-structure) (:primary new-structure))
           (= (:auto-inc existing-structure) (:auto-inc new-structure)))
      (let [old-indexes (set (:indexes existing-structure))
            new-indexes (set (:indexes new-structure))
            added (object-array (set/difference new-indexes old-indexes))
            dropped (set/difference old-indexes new-indexes)
            primary (:primary new-structure)]
        (doseq [index dropped] (drop-index txv index))
        (with-open [cursor (.openCursor data txn)]
          (do-reduce txn
                     data
                     lmdb-key-buffer
                     symbol-table
                     primary
                     (fn [_ primary-key record]
                       (dotimes [i (alength added)]
                         (let [index (aget added i)
                               k (codec/get-indexed-key index primary-key record)
                               v (codec/get-indexed-val index primary-key record)]
                           (kv/put cursor lmdb-key-buffer write-buffer symbol-table index k v false))))
                     nil)))
      ;; full rebuild
      :else
      (let [old-primary (:primary existing-structure)
            new-primary (:primary new-structure)
            new-indexes (object-array (:indexes new-structure))
            auto-inc (:auto-inc new-structure)
            rsn-counter (.-rsn-counter txv)]
        (doseq [index (:indexes existing-structure)] (drop-index txv index))
        (with-open [cursor (.openCursor data txn)]
          (do-reduce txn
                     data
                     lmdb-key-buffer
                     symbol-table
                     old-primary
                     (fn [_ _ record]
                       (let [record (preput auto-inc rsn-counter record)
                             primary-key (codec/get-indexed-key new-primary nil record)]
                         (kv/put cursor lmdb-key-buffer write-buffer symbol-table new-primary primary-key record false)
                         (dotimes [i (alength new-indexes)]
                           (let [index (aget new-indexes i)
                                 k (codec/get-indexed-key index primary-key record)
                                 v (codec/get-indexed-val index primary-key record)]
                             (kv/put cursor lmdb-key-buffer write-buffer symbol-table index k v false)))))
                     nil))))))

(defn- do-commit [^LMDBTransaction tx original-structure]
  (let [st (.-symbol-table tx)
        cursor (.-cursor tx)
        lmdb-key-buffer (.-lmdb-key-buffer tx)
        write-buffer (.-write-buffer tx)
        symbol-offset-ref (.-symbol-offset-ref tx)
        new-symbols (codec/list-adds st)
        rsn @(.-rsn-counter tx)
        structure @(.-structure-ref tx)
        offset @symbol-offset-ref]
    (try
      (some-> tx .-aborted-ref deref throw)
      (do-put cursor lmdb-key-buffer write-buffer st state-primary-index nil nil {:k :rsn, :v rsn})
      (when-not (identical? structure original-structure)
        (do-put cursor lmdb-key-buffer write-buffer st state-primary-index nil nil {:k :structure, :v structure}))
      (doseq [[i o] (map-indexed vector new-symbols)]
        (do-put cursor lmdb-key-buffer write-buffer nil symbol-table-primary-index nil nil {:offset (+ offset i), :obj o}))
      (.close ^Cursor (.-cursor tx))
      (.commit ^Txn (.-txn tx))
      (codec/clear-adds st)
      (vswap! symbol-offset-ref + (count new-symbols))
      (catch Throwable t
        (codec/rollback-adds st)
        (.close ^Cursor (.-cursor tx))
        (.abort ^Txn (.-txn tx))
        (throw t)))))

(defn- do-abort [^LMDBTransaction tx]
  (codec/rollback-adds (.-symbol-table tx)))

(defn- tx-mat-all [^LMDBTransaction tx]
  (kv/materialize-all (.-cursor tx) (.-symbol-table tx)))

(declare with-tx)

(deftype LMDBDatabase [file
                       env
                       data
                       ^ByteBuffer lmdb-key-buffer
                       write-buffer-ref
                       symbol-table
                       state-ref
                       tmp]
  p/Database
  (create-relvar [db table-id] (p/write-transaction db (fn [tx] (p/create-relvar tx table-id))) (get db table-id))
  ;; todo locking
  (write-transaction [db f] (locking db (with-tx db :write f)))
  (read-transaction [db f] (locking db (with-tx db :read f)))
  ILookup
  (valAt [db k not-found]
    (if-some [_ (-> @state-ref :structure (get k not-found))]
      (->LMDBVariable db k)
      not-found))
  (valAt [db k] (.valAt db k nil))
  Closeable
  (close [_]
    (.close ^Env env)
    (when tmp
      (io/delete-file file true))))

(defn- try-tx [^LMDBDatabase db mode f]
  (let [^Env env (.-env db)
        ^Dbi data (.-data db)
        lmdb-key-buffer (.-lmdb-key-buffer db)
        write-buffer @(.-write-buffer-ref db)
        symbol-table (.-symbol-table db)
        state-ref (.-state-ref db)]
    (with-open [^Txn txn (case mode :read (.txnRead env)
                                    :write (.txnWrite env))
                cursor (.openCursor data txn)]
      (let [{:keys [symbol-offset structure rsn]} @state-ref
            ^LMDBTransaction tx
            (->LMDBTransaction
              env
              data
              (HashMap.)
              txn
              cursor
              lmdb-key-buffer
              write-buffer
              symbol-table
              (volatile! symbol-offset)
              (volatile! rsn)
              (volatile! structure)
              ;; aborted-ref
              (volatile! nil))]
        (try
          (let [ret (f tx)]
            (when (identical? :write mode) (do-commit tx structure))
            (vswap! state-ref assoc
                    :symbol-offset @(.-symbol-offset-ref tx)
                    :rsn @(.-rsn-counter tx)
                    :structure @(.-structure-ref tx)
                    :last-tx-id (.getId txn))
            [ret])
          (catch Throwable t
            (when (identical? :write mode) (do-abort tx))
            [nil t]))))))

(defn- with-tx [^LMDBDatabase db mode f]
  (let [[ret ex] (try-tx db mode f)]
    (cond
      (not ex) ret

      (= ::codec/not-enough-space (:error (ex-data ex)))
      (let [write-buffer-ref (.-write-buffer-ref db)
            ^ByteBuffer write-buffer @write-buffer-ref
            new-write-buffer (ByteBuffer/allocateDirect (* 2 (.capacity write-buffer)))]
        (vreset! write-buffer-ref new-write-buffer)
        (recur db mode f))

      (instance? Env$MapFullException ex)
      (do
        (.setMapSize ^Env (.-env db) (get (vswap! (.-state-ref db) update :map-size * 2) :map-size))
        (recur db mode f))

      :else (throw ex))))

(defn mat-all [db] (p/read-transaction db tx-mat-all))

(defn temp-db []
  (let [file (File/createTempFile "tempdb" ".cinq")
        env (-> (Env/create)
                (.open file (into-array EnvFlags [EnvFlags/MDB_NOSUBDIR EnvFlags/MDB_NORDAHEAD])))]
    (->LMDBDatabase
      file
      env
      (.openDbi env "data" ^"[Lorg.lmdbjava.DbiFlags;" (into-array DbiFlags [DbiFlags/MDB_CREATE]))
      (ByteBuffer/allocateDirect 511)
      (volatile! (ByteBuffer/allocateDirect 4096))
      (codec/empty-symbol-table)
      (volatile! {:rsn -1
                  :symbol-offset -1
                  :structure {}})
      true)))

(comment

  (def db (temp-db))
  (.close db)

  (vec (:foo db))
  (vec (:bar db))

  (p/create-relvar db :foo)
  (p/create-relvar db :bar)

  (p/insert (:foo db) 42)
  (p/rel-set (:foo db) (range 10))

  (p/insert (:bar db) 43)
  (p/insert (:foo db) '(println "Hello, world!"))

  (mat-all db)

  )

(ns com.wotbrew.cinq.lmdb
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.nio-codec :as codec]
            [com.wotbrew.cinq.protocols :as p])
  (:import (clojure.lang ILookup IReduceInit)
           (java.io Closeable)
           (java.nio ByteBuffer)
           (java.util HashMap)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Function Supplier)
           (org.lmdbjava Cursor Dbi DbiFlags Env Env$MapFullException EnvFlags EnvInfo GetOp PutFlags Txn))
  (:refer-clojure :exclude [replace]))

(set! *warn-on-reflection* true)

(def ^:redef open-files #{})

(defn- scan [^Cursor cursor ^ByteBuffer key-buffer f init start-rsn symbol-list]
  (.clear key-buffer)
  (.putLong key-buffer (long start-rsn))
  (if-not (.get cursor (.flip key-buffer) GetOp/MDB_SET_RANGE)
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

#_(defn- scan2 [^Cursor cursor ^ByteBuffer key-buffer ^CinqScannable$Step step holder start-rsn]
    (.clear key-buffer)
    (.putLong key-buffer (long start-rsn))
    (if-not (.get cursor (.flip key-buffer) GetOp/MDB_SET_RANGE)
      false
      (loop []
        (let [rsn (.getLong ^ByteBuffer (.key cursor))
              o (codec/decode-object (.val cursor) nil)
              ret (.apply step rsn holder o)]
          (or ret (when (.next cursor) (recur)))))))

(defn- last-rsn ^long [^Cursor cursor]
  (if (.last cursor)
    (let [^ByteBuffer k (.key cursor)
          rsn (.getLong k)]
      rsn)
    -1))

(def ^:private ^"[Lorg.lmdbjava.PutFlags;" insert-flags
  (make-array PutFlags 0))

(def ^:private ^"[Lorg.lmdbjava.PutFlags;" replace-flags
  (doto ^"[Lorg.lmdbjava.PutFlags;" (make-array PutFlags 1)
    (aset 0 PutFlags/MDB_CURRENT)))

(defn- rel-set [^Dbi dbi ^Txn txn ^ByteBuffer key-buffer ^ByteBuffer val-buffer rel symbol-table]
  (let [reduced (reduced ::not-enough-space-in-write-buffer)
        step (fn [_acc rsn r]
               (.clear key-buffer)
               (.clear val-buffer)
               (.putLong key-buffer rsn)
               (if-not (codec/encode-object r val-buffer symbol-table)
                 reduced
                 (do (.put dbi txn (.flip key-buffer) (.flip val-buffer) insert-flags)
                     nil)))]
    (.drop dbi txn)
    (when-some [err (p/scan rel step nil 0)]
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

(defn- delete [^Dbi dbi ^Txn txn ^ByteBuffer key-buffer ^long rsn]
  (.clear key-buffer)
  (.putLong key-buffer rsn)
  (.delete dbi txn (.flip key-buffer)))

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

(defn create-dbi ^Dbi [^Env env ^String dbi-name]
  (let [flags (into-array DbiFlags [DbiFlags/MDB_CREATE])]
    (.openDbi env dbi-name ^"[Lorg.lmdbjava.DbiFlags;" flags)))

(deftype LMDBWriteTransactionVariable
  [^Env env
   ^Txn txn
   ^Dbi dbi
   ^ByteBuffer key-buffer
   ^ByteBuffer val-buffer
   ^:unsynchronized-mutable next-rsn
   symbol-table]
  p/Scannable
  (scan [_ f init start-rsn]
    (with-open [cursor (.openCursor dbi txn)]
      (scan cursor key-buffer f init start-rsn (codec/symbol-list symbol-table))))
  IReduceInit
  (reduce [relvar f start]
    (p/scan relvar (fn [acc _ record] (f acc record)) start 0))
  p/Relvar
  (rel-set [relvar rel]
    (set! (.-next-rsn relvar) -1)
    (rel-set dbi txn key-buffer val-buffer rel symbol-table))
  p/IncrementalRelvar
  (insert [relvar record]
    (if (= -1 (.-next-rsn relvar))
      (do (with-open [^Cursor cursor (.openCursor dbi txn)]
            (set! (.-next-rsn relvar) (unchecked-inc (last-rsn cursor))))
          (recur record))
      (let [rsn next-rsn
            _ (insert dbi txn key-buffer val-buffer rsn record symbol-table)]
        (set! (.-next-rsn relvar) (unchecked-inc rsn))
        rsn)))
  (delete [_ rsn]
    (delete dbi txn key-buffer rsn))
  (replace [_ rsn record]
    (replace dbi txn key-buffer val-buffer rsn record symbol-table)))

(deftype LMDBReadTransactionVariable
  [^Env env
   ^Txn txn
   ^Dbi dbi
   ^ByteBuffer key-buffer
   symbol-table]
  p/Scannable
  (scan [_ f init start-rsn]
    (with-open [cursor (.openCursor dbi txn)]
      (scan cursor key-buffer f init start-rsn (codec/symbol-list symbol-table))))
  IReduceInit
  (reduce [relvar f start]
    (p/scan relvar (fn [acc _ record] (f acc record)) start 0)))

(deftype LMDBWriteTransaction
  [^Env env
   ^HashMap varmap
   ^ConcurrentHashMap dbis
   ^Txn txn
   ^ByteBuffer key-buffer
   ^ByteBuffer val-buffer
   symbol-table]
  ILookup
  (valAt [_ k]
    (when-some [dbi (.get dbis k)]
      (->> (reify Function
             (apply [_ dbi]
               (->LMDBWriteTransactionVariable env txn dbi key-buffer val-buffer -1 (if (identical? :lmdb/symbols k) nil symbol-table))))
           (.computeIfAbsent varmap dbi))))
  (valAt [tx k not-found] (or (get tx k) not-found))
  p/Transaction
  (commit [t]
    (doseq [symbol (:added @symbol-table)] (p/insert (:lmdb/symbols t) symbol))
    (.commit txn)
    (codec/clear-symbol-adds symbol-table))
  Closeable
  (close [_]
    (.close txn)))

(deftype LMDBReadTransaction
  [^Env env
   ^HashMap varmap
   ^ConcurrentHashMap dbis
   ^Txn txn
   ^ByteBuffer key-buffer
   symbol-table]
  ILookup
  (valAt [_ k]
    (when-some [dbi (.get dbis k)]
      (->> (reify Function
             (apply [_ dbi]
               (->LMDBReadTransactionVariable
                 env
                 txn
                 dbi
                 key-buffer
                 symbol-table)))
           (.computeIfAbsent varmap dbi))))
  (valAt [tx k not-found] (or (get tx k) not-found))
  Closeable
  (close [_] (.close txn)))

(declare ->LMDBVariable)

(defn- intrinsic-rel [rel-fn]
  (reify
    p/Scannable
    (scan [_ f init rsn]
      (p/scan (rel-fn) f init rsn))
    IReduceInit
    (reduce [_ f init]
      (p/scan (rel-fn) (fn [acc _ r] (f acc r)) init 0))))

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
      (with-open [^Closeable tx
                  (->LMDBWriteTransaction
                    env
                    (HashMap.)
                    dbis
                    (.txnWrite env)
                    (.get key-buffer)
                    val-buffer
                    symbol-table)]
        (let [ret (f tx)]
          (p/commit tx)
          ret))
      (let [ret (locking db
                  (try
                    (with-open [^Closeable tx
                                (->LMDBWriteTransaction
                                  env
                                  (HashMap.)
                                  dbis
                                  (.txnWrite env)
                                  (.get key-buffer)
                                  val-buffer
                                  symbol-table)]
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
    (with-open [^Closeable tx (->LMDBReadTransaction env (HashMap.) dbis (.txnRead env) (.get key-buffer) symbol-table)]
      (f tx)))
  (create-relvar [rv k]
    (.computeIfAbsent dbis k (reify Function (apply [_ k] (create-dbi env (pr-str k)))))
    (.valAt ^ILookup rv k))
  ILookup
  (valAt [db k]
    (case k
      :lmdb/stat
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
              :page-size (.-pageSize env-stat)}])))
      :lmdb/variables
      (intrinsic-rel
        (fn []
          (sort (into [:lmdb/stat :lmdb/variables] (keys dbis)))))
      (when (.containsKey dbis k)
        (->> (reify Function (apply [_ _] (->LMDBVariable db k key-buffer val-buffer)))
             (.computeIfAbsent varmap k)))))
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
   ^ByteBuffer val-buffer]
  p/Scannable
  (scan [_ f init start-rsn] (variable-read db k (fn [v] (p/scan v f init start-rsn))))
  IReduceInit
  (reduce [relvar f start] (p/scan relvar (fn [acc _ record] (f acc record)) start 0))
  p/Relvar
  (rel-set [_ rel] (variable-write db k (fn [v] (p/rel-set v rel))))
  p/IncrementalRelvar
  (insert [_ record] (variable-write db k (fn [v] (p/insert v record))))
  (delete [_ rsn] (variable-write db k (fn [v] (p/delete v rsn))))
  (replace [_ rsn record] (variable-write db k (fn [v] (p/replace v rsn record)))))

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
            symbol-dbi (create-dbi env (pr-str :lmdb/symbols))
            dbis (let [dbis (ConcurrentHashMap.)]
                   (doseq [^bytes dbi-bytes (.getDbiNames env)
                           :let [dbi-name (String. dbi-bytes "UTF-8")]]
                     (.put dbis (edn/read-string dbi-name) (create-dbi env dbi-name)))
                   dbis)
            symbol-table (codec/empty-symbol-table)]
        ;; read symbols
        (with-open [txn (.txnRead env)
                    cursor (.openCursor symbol-dbi txn)]
          (when (.first cursor)
            (loop []
              (codec/intern-symbol symbol-table (codec/decode-object (.val cursor) nil))
              (when (.next cursor) (recur)))))
        (codec/clear-symbol-adds symbol-table)
        (->LMDBDatabase file
                        env
                        dbis
                        (ConcurrentHashMap.)
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
  (c/create db :foo)
  (c/create db :foo (range 1e6))
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

  (require 'criterium.core)
  (criterium.core/quick-bench (c/q [i (:foo db) :limit 10] i))
  (criterium.core/quick-bench (c/q [i (:foo db) :limit 10] i))
  (criterium.core/quick-bench (vec (:foo db)))

  (def sf005 ((requiring-resolve 'com.wotbrew.cinq.tpch-test/all-tables) 0.05))

  (doseq [[table coll] sf005]
    (println table)
    (c/create db table)
    (time (c/rel-set (get db table) coll)))

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
        :when (<= l:shipdate #inst "1998-09-02") :group []]
    (c/count))

  (clj-async-profiler.core/profile
    (criterium.core/quick-bench
      (count (q1 db))
      )
    )

  (clj-async-profiler.core/profile
    (criterium.core/quick-bench
      (count (q1 sf005))
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

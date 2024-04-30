(ns com.wotbrew.cinq.lmdb
  (:require [clojure.java.io :as io]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.nio-codec :as codec]
            [com.wotbrew.cinq.protocols :as p])
  (:import (clojure.lang ILookup IReduceInit)
           (java.io Closeable)
           (java.nio ByteBuffer)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.util ArrayList HashMap Map)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Function Supplier)
           (org.lmdbjava Cursor Dbi DbiFlags Env EnvFlags GetOp PutFlags Txn))
  (:refer-clojure :exclude [replace]))

(set! *warn-on-reflection* true)

(def ^:redef open-directories #{})

(defn- scan [^Cursor cursor ^ByteBuffer key-buffer f init start-rsn]
  (.clear key-buffer)
  (.putLong key-buffer (long start-rsn))
  (if-not (.get cursor (.flip key-buffer) GetOp/MDB_SET_RANGE)
    init
    (loop [acc init]
      (let [rsn (.getLong ^ByteBuffer (.key cursor))
            o (codec/decode-object (.val cursor) nil)
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

(def ^:private ^"[Lorg.lmdbjava.PutFlags;" insert-flags
  (make-array PutFlags 0))

(def ^:private ^"[Lorg.lmdbjava.PutFlags;" replace-flags
  (doto ^"[Lorg.lmdbjava.PutFlags;" (make-array PutFlags 1)
    (aset 0 PutFlags/MDB_CURRENT)))

(defn- rel-set [^Dbi dbi ^Txn txn ^ByteBuffer key-buffer ^ByteBuffer val-buffer rel]
  (let [reduced (reduced ::not-enough-space-in-write-buffer)
        step (fn [_acc rsn r]
               (.clear key-buffer)
               (.clear val-buffer)
               (.putLong key-buffer rsn)
               (if-not (codec/encode-object r val-buffer nil)
                 reduced
                 (do (.put dbi txn (.flip key-buffer) (.flip val-buffer) insert-flags)
                     nil)))]
    (.drop dbi txn)
    (when-some [err (p/scan rel step nil 0)]
      (throw (ex-info "Transaction error during rel-set" {:error err})))))

(defn- insert [^Dbi dbi ^Txn txn ^ByteBuffer key-buffer ^ByteBuffer val-buffer rsn record]
  (if (nil? rsn)
    (recur dbi txn key-buffer val-buffer (with-open [cursor (.openCursor dbi txn)] (last-rsn cursor)) record)
    (do
      (.clear key-buffer)
      (.clear val-buffer)
      (.putLong key-buffer rsn)
      (if-not (codec/encode-object record val-buffer nil)
        (throw (ex-info "Not enough space in write-buffer during insert" {:cinq/error ::not-enough-space-in-write-buffer}))
        (.put dbi txn (.flip key-buffer) (.flip val-buffer) insert-flags)))))

(defn- delete [^Dbi dbi ^Txn txn ^ByteBuffer key-buffer ^long rsn]
  (.clear key-buffer)
  (.putLong key-buffer rsn)
  (.delete dbi txn (.flip key-buffer)))

(defn- replace [^Dbi dbi ^Txn txn ^ByteBuffer key-buffer ^ByteBuffer val-buffer ^Long rsn ^Object record]
  (.clear key-buffer)
  (.clear val-buffer)
  (.putLong key-buffer rsn)
  (if-not (codec/encode-object record val-buffer nil)
    (throw (ex-info "Not enough space in write-buffer during replace" {:cinq/error ::not-enough-space-in-write-buffer}))
    (.put dbi txn
          (.flip key-buffer)
          (.flip val-buffer)
          replace-flags)))

(defn create-dbi ^Dbi [^Env env ^String dbi-name]
  (let [flags (into-array DbiFlags [DbiFlags/MDB_CREATE])]
    (.openDbi env dbi-name ^"[Lorg.lmdbjava.DbiFlags;" flags)))

(deftype LMDBWriteTransactionVariable
  [^Env env
   ^Txn txn
   ^Dbi dbi
   ^ByteBuffer key-buffer
   ^ByteBuffer val-buffer
   ^:unsynchronized-mutable next-rsn]
  p/Scannable
  (scan [_ f init start-rsn]
    (with-open [cursor (.openCursor dbi txn)]
      (scan cursor key-buffer f init start-rsn)))
  IReduceInit
  (reduce [relvar f start]
    (p/scan relvar (fn [acc _ record] (f acc record)) start 0))
  p/Relvar
  (rel-set [relvar rel]
    (set! (.-next-rsn relvar) -1)
    (rel-set dbi txn key-buffer val-buffer rel))
  p/IncrementalRelvar
  (insert [relvar record]
    (if (= -1 (.-next-rsn relvar))
      (do (with-open [^Cursor cursor (.openCursor dbi txn)]
            (set! (.-next-rsn relvar) (unchecked-inc (last-rsn cursor))))
          (recur record))
      (let [rsn next-rsn
            _ (insert dbi txn key-buffer val-buffer rsn record)]
        (set! (.-next-rsn relvar) (unchecked-inc rsn))
        rsn)))
  (delete [_ rsn]
    (delete dbi txn key-buffer rsn))
  (replace [_ rsn record]
    (replace dbi txn key-buffer val-buffer rsn record)))

(deftype LMDBReadTransactionVariable
  [^Env env
   ^Txn txn
   ^Dbi dbi
   ^ByteBuffer key-buffer
   ^ByteBuffer val-buffer
   ^:unsynchronized-mutable dropped
   ^Map overrides
   ^:unsynchronized-mutable next-rsn
   ^ArrayList tail]
  p/Scannable
  (scan [relvar f init start-rsn]
    (let [ret
          (if dropped
            init
            (with-open [cursor (.openCursor dbi txn)]
              (let [ret (scan cursor key-buffer
                              (fn [acc rsn record]
                                (let [ret (if (.containsKey overrides rsn)
                                            (let [override (.get overrides rsn)]
                                              (if (identical? override ::deleted)
                                                acc
                                                (f acc rsn record)))
                                            (f acc rsn record))]
                                  (if (reduced? ret)
                                    (reduced ret)
                                    ret)))
                              init
                              start-rsn)]
                (set! (.-next-rsn relvar) (unchecked-inc (last-rsn cursor)))
                ret)))]
      (if (reduced? ret)
        @ret
        (loop [acc ret
               i 0]
          (if (< i (.size tail))
            (let [o (.get tail i)]
              (if (identical? o ::deleted)
                (recur acc (unchecked-inc i))
                (let [ret (f acc (+ (.-next-rsn relvar) i) o)]
                  (if (reduced? ret)
                    @ret)
                  (recur ret (unchecked-inc i)))))
            acc)))))
  IReduceInit
  (reduce [relvar f start]
    (p/scan relvar (fn [acc _ record] (f acc record)) start 0))
  p/Relvar
  (rel-set [relvar rel]
    (set! dropped true)
    (set! next-rsn 0)
    (.clear overrides)
    (.clear tail)
    (let [step (fn [_acc _ r] (p/insert relvar r))]
      (when-some [err (p/scan rel step nil 0)]
        (throw (ex-info "Transaction error during rel-set" {:error err})))))
  p/IncrementalRelvar
  (insert [_ record]
    (.add tail record)
    (+ next-rsn (.size tail)))
  (delete [_ rsn]
    (cond
      (< rsn next-rsn)
      (cond
        ;; if key in overrides
        (contains? overrides rsn)
        (if (identical? ::deleted (.get overrides rsn))
          false
          (do (.put overrides rsn ::deleted)
              true))

        ;; if dropped key cannot be in lmdb
        dropped false

        ;; if key in lmdb
        (with-open [cursor (.openCursor dbi txn)]
          (.clear key-buffer)
          (.putLong key-buffer rsn)
          (.get cursor (.flip key-buffer) GetOp/MDB_SET))
        (do (.put overrides rsn ::deleted)
            true)

        :else false)

      (< (- rsn next-rsn 1) (.size tail))
      (do (.set tail (- rsn next-rsn 1) ::deleted)
          true)

      :else false)
    (.clear key-buffer)
    (.putLong key-buffer rsn)
    (.delete dbi txn (.flip key-buffer)))
  (replace [_ rsn record]
    (cond
      (< rsn next-rsn)
      (cond
        ;; if key in overrides
        (contains? overrides rsn)
        (if (identical? ::deleted (.get overrides rsn))
          false
          (do (.put overrides rsn record)
              true))

        ;; if dropped key cannot be in lmdb
        dropped false

        ;; if key in lmdb
        (with-open [cursor (.openCursor dbi txn)]
          (.clear key-buffer)
          (.putLong key-buffer rsn)
          (.get cursor (.flip key-buffer) GetOp/MDB_SET))
        (do (.put overrides rsn record)
            true)

        :else false)

      (< (- rsn next-rsn 1) (.size tail))
      (do (.set tail (- rsn next-rsn 1) record)
          true)

      :else false)))

(deftype LMDBWriteTransaction
  [^Env env
   ^HashMap varmap
   ^ConcurrentHashMap dbis
   ^Txn txn
   ^ByteBuffer key-buffer
   ^ByteBuffer val-buffer]
  ILookup
  (valAt [_ k]
    (when-some [dbi (.get dbis k)]
      (->> (reify Function
             (apply [_ dbi]
               (->LMDBWriteTransactionVariable env txn dbi key-buffer val-buffer -1)))
           (.computeIfAbsent varmap dbi))))
  (valAt [tx k not-found] (or (get tx k) not-found))
  p/Transaction
  (commit [_]
    (.commit txn))
  Closeable
  (close [_] (.close txn)))

(deftype LMDBReadTransaction
  [^Env env
   ^HashMap varmap
   ^ConcurrentHashMap dbis
   ^Txn txn
   ^ByteBuffer key-buffer
   ^ByteBuffer val-buffer]
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
                 val-buffer
                 false
                 (HashMap.)
                 -1
                 (ArrayList.))))
           (.computeIfAbsent varmap dbi))))
  (valAt [tx k not-found] (or (get tx k) not-found))
  p/Transaction
  (commit [_]
    (.commit txn))
  Closeable
  (close [_] (.close txn)))

(deftype LMDBVariable
  [^Env env
   ^Dbi dbi
   ^ThreadLocal key-buffer
   ^ByteBuffer val-buffer]
  p/Scannable
  (scan [_ f init start-rsn]
    (with-open [txn (.txnRead env)
                cursor (.openCursor dbi txn)]
      (scan cursor (.get key-buffer) f init start-rsn)))
  IReduceInit
  (reduce [relvar f start]
    (p/scan relvar (fn [acc _ record] (f acc record)) start 0))
  p/Relvar
  (rel-set [_ rel]
    (with-open [txn (.txnWrite env)]
      (rel-set dbi txn (.get key-buffer) val-buffer rel)
      (.commit txn)))
  p/IncrementalRelvar
  (insert [_ record]
    (with-open [txn (.txnWrite env)]
      (insert dbi txn (.get key-buffer) val-buffer nil record)
      (.commit txn)))
  (delete [_ rsn]
    (with-open [txn (.txnWrite env)]
      (delete dbi txn (.get key-buffer) val-buffer rsn)
      (.commit txn)))
  (replace [_ rsn record]
    (with-open [txn (.txnWrite env)]
      (replace dbi txn (.get key-buffer) val-buffer rsn record)
      (.commit txn))))

(deftype LMDBDatabase
  [directory
   ^Env env
   ^ConcurrentHashMap dbis
   ^ConcurrentHashMap varmap
   ^ThreadLocal key-buffer
   ^ByteBuffer val-buffer]
  p/Database
  (open-write-tx [_]
    (->LMDBWriteTransaction env (HashMap.) dbis (.txnWrite env) (.get key-buffer) val-buffer))
  (open-read-tx [_]
    (->LMDBReadTransaction env (HashMap.) dbis (.txnRead env) (.get key-buffer) val-buffer))
  (create-relvar [_ k]
    (.computeIfAbsent dbis k (reify Function (apply [_ k] (create-dbi env (pr-str k))))))
  ILookup
  (valAt [_ k]
    (when-some [dbi (.get dbis k)]
      (->> (reify Function (apply [_ dbi] (->LMDBVariable env dbi key-buffer val-buffer)))
           (.computeIfAbsent varmap dbi))))
  (valAt [db k not-found] (or (get db k) not-found))
  Closeable
  (close [_] (.close env)))

(defn create-db [& {:keys [directory
                           max-variables
                           max-size],
                    :or {max-variables 4096
                         max-size (* 64 1024 1024)}}]
  (let [directory (if directory
                    (io/file directory)
                    (io/file (str (Files/createTempDirectory "cinq-lmdb" (make-array FileAttribute 0)))))]
    (when-not (.exists directory) (.mkdirs directory))
    (when-not (.isDirectory directory) (throw (IllegalArgumentException. "Not a directory")))
    (alter-var-root #'open-directories
                    (fn [dirs]
                      (if (contains? dirs directory)
                        (throw (IllegalArgumentException. "Directory already open"))
                        (conj dirs directory))))
    (try
      (let [env (-> (Env/create)
                    (.setMapSize max-size)
                    (.setMaxDbs max-variables)
                    (.open directory (make-array EnvFlags 0)))]
        (->LMDBDatabase directory env
                        (ConcurrentHashMap.)
                        (ConcurrentHashMap.)
                        (ThreadLocal/withInitial (reify Supplier (get [_] (ByteBuffer/allocateDirect 8))))
                        (ByteBuffer/allocateDirect (* 32 1024 1024))))
      (catch Throwable t
        (alter-var-root #'open-directories disj directory)
        (throw t)))))

(defn get-dbi ^Dbi [^LMDBDatabase lmdb ^String dbi-name]
  (let [^ConcurrentHashMap m (.-dbis lmdb)]
    (.get m dbi-name)))

(defn get-or-create-dbi ^Dbi [^LMDBDatabase lmdb ^String dbi-name]
  (let [^ConcurrentHashMap m (.-dbis lmdb)
        ^Env env (.-env lmdb)]
    (.computeIfAbsent m dbi-name (reify Function (apply [_ _] (create-dbi env dbi-name))))))

(defn scan-all ^ByteBuffer [^LMDBDatabase lmdb dbi-name]
  (let [^Env env (.-env lmdb)
        dbi (get-or-create-dbi lmdb dbi-name)]
    (with-open [txn (.txnRead env)
                cursor (.openCursor dbi txn)]
      (if-not (.first cursor)
        (ByteBuffer/allocate 0)
        (let [stat (.stat dbi txn)
              ps (.-pageSize stat)
              size (* ps (.-leafPages stat))
              buf (ByteBuffer/allocate size)]
          (loop []
            (let [^ByteBuffer v (.val cursor)]
              (.put buf v)
              (when (.next cursor) (recur))))
          (.flip buf))))))

(defn decode-scan-buffer [^ByteBuffer buf]
  (let [al (ArrayList.)]
    (while (.hasRemaining buf)
      (.add al (codec/decode-object buf nil)))
    al))

(defn dbi-value [^LMDBDatabase lmdb dbi-name]
  (decode-scan-buffer (scan-all lmdb dbi-name)))

(defn set-txn-value
  [^Txn txn
   ^Dbi dbi
   ;; min size 8? (key) + 16 = 24
   ^ByteBuffer write-buffer
   ^Iterable rel]
  (.clear write-buffer)
  (let [put-flags (make-array PutFlags 0)
        key-buffer (.slice write-buffer 0 8)
        _ (.position write-buffer 8)
        val-buffer (.slice write-buffer 8 (- (.limit write-buffer) 8))
        iterator (some-> rel .iterator)]
    (.drop dbi txn)
    (when iterator
      (loop [i 0]
        (.clear key-buffer)
        (.clear val-buffer)
        (.putLong key-buffer i)
        (cond
          (not (.hasNext iterator)) true
          (not (codec/encode-object (.next iterator) val-buffer nil)) false
          :else (do (.put dbi txn (.flip key-buffer) (.flip val-buffer) put-flags)
                    (recur (unchecked-inc i))))))))

(defn set-dbi-value
  [^LMDBDatabase lmdb
   dbi-name
   ;; min size 8? (key) + 16 = 24
   ^ByteBuffer write-buffer
   rel]
  (let [^Env env (.-env lmdb)
        dbi (get-or-create-dbi lmdb dbi-name)]
    (with-open [txn (.txnWrite env)]
      (when (set-txn-value txn dbi write-buffer rel)
        (.commit txn)))))

(comment
  (def db (create-db))
  (.close db)
  (p/create-relvar db :foo)
  (.-dbis db)

  (into [] (:foo db))
  (time (count (vec (:foo db))))

  (:bar db)

  (set! *print-length* 100)

  (c/rel-set (:foo db) (range 1e6))



  )

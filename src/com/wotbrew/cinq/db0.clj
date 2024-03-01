(ns com.wotbrew.cinq.db0)

(defn in-memory [] (volatile! (sorted-map)))

(defn read-transaction [db] (volatile! [@db (volatile! nil)]))

(defn read-write-transaction [db] (volatile! [@db db]))

(defn insert! [txn obj]
  (let [[sm ref] @txn
        last-id (first (keys (reverse sm)))
        new-id (or (some-> last-id inc) 0)]
    (vreset! txn [(assoc sm new-id (assoc obj :cinq/tid new-id)) ref])
    new-id))

(defn delete! [txn tid]
  (let [[sm ref] @txn]
    (vreset! txn [(dissoc sm tid) ref])))

(defn commit! [txn]
  (let [[sm ref] @txn] (vreset! ref sm)))

(defn tuple-seq [txn] (let [[sm] @txn] (vals sm)))

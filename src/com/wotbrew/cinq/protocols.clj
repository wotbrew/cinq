(ns com.wotbrew.cinq.protocols
  (:refer-clojure :exclude [replace]))

(defprotocol Database
  (create-relvar [db relvar-key])
  (write-transaction [db f])
  (read-transaction [db f]))

(defprotocol Transaction
  (commit [tx]))

(defprotocol Relvar
  (rel-set [relvar rel]))

(defprotocol BigCount
  (big-count [relvar]))

(defprotocol IncrementalRelvar
  (insert [relvar record])
  (delete [relvar rsn]))

(defprotocol Scannable
  (scan [rel f init]))

(defprotocol Index
  (indexed-key [idx])
  (getn [idx key])
  (get1 [idx key not-found])
  (range-scan [idx test-a a test-b b])
  (sorted-scan [idx high]))

(defprotocol Indexable
  (index [indexable indexed-key]))

(ns com.wotbrew.cinq.protocols
  (:refer-clojure :exclude [replace]))

(defprotocol Database
  (create-relvar [db relvar-key])
  (create-index [db relvar-key index-key])
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
  (range-scan [rel test-a a test-b b]))

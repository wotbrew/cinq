(ns com.wotbrew.cinq.protocols
  (:refer-clojure :exclude [replace]))

(defprotocol Database
  (create-relvar [db name])
  (open-write-tx [db])
  (open-read-tx [db]))

(defprotocol Transaction
  (commit [tx]))

(defprotocol Relvar
  (rel-set [relvar rel]))

(defprotocol IncrementalRelvar
  (insert [relvar record])
  (delete [relvar rsn])
  (replace [relvar rsn record]))

(defprotocol Scannable
  (scan [rel f init start-rsn]))

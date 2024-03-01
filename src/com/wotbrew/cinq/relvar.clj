(ns com.wotbrew.cinq.relvar
  (:require [com.wotbrew.cinq.db0 :as db0]
            [com.wotbrew.cinq.scope :as scope])
  (:import (clojure.lang IDeref Named Seqable)))

(defn tuple-seq [rv txn]
  (let [rv-name (name rv)]
    (for [obj (db0/tuple-seq txn)
          :when (= (:cinq/rv obj) rv-name)]
      obj)))

(defn insert! [rv txn m] (db0/insert! txn (assoc m :cinq/rv (name rv))))

(defn delete! [rv txn tid] (db0/delete! txn tid))

(deftype Relvar [variable-name]
  IDeref
  (deref [this] (tuple-seq this scope/*tx*))
  Seqable
  (seq [this] (tuple-seq this scope/*tx*))
  Named
  (getNamespace [_] (namespace variable-name))
  (getName [_] (name variable-name)))

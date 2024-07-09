(ns scratch2
  (:require [clojure.java.io :as io]
            [com.wotbrew.cinq :as c]
            [com.wotbrew.cinq.lmdb :as lmdb]))

(declare db)
(when (bound? #'db) (.close db) (io/delete-file "tmp/scratch.cinq" true))
(def db (lmdb/database "tmp/scratch.cinq"))

(def foo (c/create db :foo))
(def bar (c/create db :bar))

foo

(c/insert foo 1)

(c/rel-set foo (range 10))
(c/rel-set bar (range 10))

(c/update-all foo [f] (inc f))

foo

(c/update-where bar [b] (even? b) (+ 42 b))

bar

(c/q [f foo
      b bar
      :when (= f b)]
  [f b])


(c/run!
  [f foo
   b bar
   :when (= f b)]
  (c/update f (inc f))
  (c/delete b))

foo
bar

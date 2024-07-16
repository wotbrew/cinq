(ns com.wotbrew.cinq.cmp-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(defmacro prj [expr] `(c/rel-first (c/q [x# [{}]] ~expr)))

(deftest expr-test
  (are [expr result]
    (= result expr)

    (prj (< nil)) nil
    (prj (< 3)) true
    (prj (< nil nil)) nil
    (prj (< nil nil nil)) nil
    (prj (< 3 4)) true
    (prj (< 4 3)) false
    (prj (< 3 3)) false
    (prj (< 2 3 3)) false
    (prj (< 2 3 4)) true
    (prj (< 3 nil)) nil
    (prj (< 2 3 nil)) nil

    (prj (<= nil)) nil
    (prj (<= 3)) true
    (prj (<= nil nil)) nil
    (prj (<= nil nil nil)) nil
    (prj (<= 3 4)) true
    (prj (<= 4 3)) false
    (prj (<= 3 3)) true
    (prj (<= 3 3 4)) true
    (prj (<= 3 nil)) nil
    (prj (<= 2 3 nil)) nil

    (prj (> nil)) nil
    (prj (> 3)) true
    (prj (> nil nil)) nil
    (prj (> nil nil nil)) nil
    (prj (> 3 4)) false
    (prj (> 4 3)) true
    (prj (> 3 3)) false
    (prj (> 3 4 4)) false
    (prj (> 4 3 2)) true
    (prj (> 3 nil)) nil
    ;; applied as repeated bin-op
    (prj (> 2 3 nil)) false
    (prj (> 4 3 nil)) nil

    (prj (>= nil)) nil
    (prj (>= 3)) true
    (prj (>= nil nil)) nil
    (prj (>= nil nil nil)) nil
    (prj (>= 3 4)) false
    (prj (>= 4 3)) true
    (prj (>= 3 3)) true
    (prj (>= 3 4 4)) false
    (prj (>= 4 3 3)) true
    (prj (>= 3 nil)) nil
    ;; applied as repeated bin-op
    (prj (>= 2 3 nil)) false
    (prj (>= 3 3 nil)) nil))

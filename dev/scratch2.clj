(ns scratch2
  (:require [com.wotbrew.cinq :refer [q, p] :as c]))

(comment

  (q selection projection)

  (q [a [1, 2, 3]] a)

  (q [a [1, 2, 3]] (inc a))

  (q [a [1, 2, 3]
      b (range 10)
      :when (= a (inc b))]
    [a, b])

  (p [a [1, 2, 3]
      b (range 10)
      :when (= a (inc b))]
     [a, b])

  (q [a [1, 2, 3]
      b (range 10)
      :when (= a (inc b))
      :group [even (even? a)]]
    [even (c/sum (+ a b))])

  (q [a [1, 2, 3, 4]
      :when (= a (c/scalar [b (range 10) :when (= b 4)] b))]
    a)

  (p [a [1, 2, 3, 4]
      :when (= a (c/scalar [b (range 10) :when (= b 4)] b))]
    a)



  )

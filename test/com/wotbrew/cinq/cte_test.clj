(ns com.wotbrew.cinq.cte-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest basics-test
  (are [q ret]
    (= ret (vec q))

    (c/with [] []) []
    (c/with [] [1]) [1]
    (c/with [a [1]] a) [1]
    (c/with [a [1] b []] a) [1]
    (c/with [a [1] b []] b) []
    (c/with [a [] b []] [a b]) [[] []]
    (c/with [a [1] b a] b) [1]
    (c/with [a [42] a []] a) [42]
    (c/with [a [42] a [43]] a) [42, 43]
    (c/with [a [42] a [43] b a] b) [42, 43]
    (c/with [a [42] a (c/rel [a a :when (< a 44)] (inc a))] a) [42, 43, 44]))

(deftest ancestor-test
  (is (= '[["bob" "jim"]
           ["jim" "phil"]
           ["bob" "karen"]
           ["karen" "lois"]
           ["lois" "eve"]
           ["bob" "phil"]
           ["bob" "lois"]
           ["karen" "eve"]]

         (vec
           (c/with [ancestor [["bob" "jim"]
                              ["jim" "phil"]
                              ["bob" "karen"]
                              ["karen" "lois"]
                              ["lois" "eve"]]
                    ancestor (c/rel [[a b] ancestor
                                   [c d] ancestor
                                   :when (= b c)]
                                    [a d])]
             ancestor))

         (vec
           (c/with [father [["bob" "jim"]
                            ["jim" "phil"]]
                    mother [["bob" "karen"]
                            ["karen" "lois"]
                            ["lois" "eve"]]
                    parent father
                    parent mother
                    ancestor parent
                    ancestor (c/rel [[a b] ancestor
                                   [c d] ancestor
                                   :when (= b c)]
                                    [a d])]
                   ancestor))))

  (is (= [["bob" "jim" ["bob" "jim"]]
          ["jim" "dave" ["jim" "dave"]]
          ["dave" "ralph" ["dave" "ralph"]]
          ["ralph" "bob" ["ralph" "bob"]]
          ["bob" "ralph" ["bob" "ralph"]]
          ["ralph" "bob" ["ralph" "bob"]]
          ["ralph" "jim" ["ralph" "bob" "jim"]]
          ["ralph" "jim" ["ralph" "bob" "jim"]]
          ["bob" "dave" ["bob" "jim" "dave"]]
          ["jim" "ralph" ["jim" "dave" "ralph"]]
          ["dave" "bob" ["dave" "ralph" "bob"]]
          ["dave" "bob" ["dave" "ralph" "bob"]]]
         (vec
           (c/with [friend [["bob" "jim"]
                            ["jim" "dave"]
                            ["dave" "ralph"]
                            ["ralph" "bob"]
                            ["bob" "ralph"]
                            ;; thar be a cycle
                            ["ralph" "bob"]]
                    friend-path (c/rel [[a b] friend] [a b [a b]])
                    friend-path (c/rel [[a b path1] friend-path
                                      [c d path2] friend-path
                                      :when (and (= b c)
                                                 ;; cycle gets stopped by this clause
                                                 (not (some #{a} path2)))]
                                       [a d (conj path1 d)])]
                   friend-path)))))

(deftest mandlebrot-meme-test
  (is (=
        ["                                    +.
                                    ....#
                                   ..#*..
                                 ..+####+.
                            .......+####....   +
                           ..##+*##########+.++++
                          .+.##################+.
              .............+###################+.+
              ..++..#.....*#####################+.
             ...+#######++#######################.
          ....+*################################.
 #############################################...
          ....+*################################.
             ...+#######++#######################.
              ..++..#.....*#####################+.
              .............+###################+.+
                          .+.##################+.
                           ..##+*##########+.++++
                            .......+####....   +
                                 ..+####+.
                                   ..#*..
                                    ....#"]
        (vec
          (c/with [xaxis [-2.0M]
                   xaxis (c/rel [x xaxis :when (< x 1.2)] (+ x 0.05))
                   yaxis [-1.0M]
                   yaxis (c/rel [y yaxis :when (< y 1.0)] (+ y 0.1))
                   m (c/rel [x xaxis, y yaxis] [0 x y 0.0 0.0])
                   m (c/rel [[iter cx cy x y] m
                           :when (and (< (+ (* x x) (* y y)) 4.0)
                                      (< iter 28))
                           :order [cx :asc cy :asc]]
                            [(inc iter)
                        cx
                        cy
                        (+ cx (- (* x x) (* y y)))
                        (+ cy (* 2.0 x y))])
                   m2 (c/rel [[iter cx cy] m
                            :group [cx cx, cy cy]
                            :order [cx :asc cy :asc]]
                             [(c/max iter) cx cy])
                   a (c/rel [[iter cx cy] m2
                           :group [cy cy]
                           :order [cy :desc]]
                            (str/join "" (map #(let [i (min (quot % 7) 4)]
                                            (subs " .+*#" i (inc i)))
                                         iter)))]
                  (c/rel [t a
                        :group []]
                         (str/join "\n" (map str/trimr t))))))))

(deftest under-alice-test
  (is (= ["Alice"
          "...Bob"
          "...Cindy"
          "......Dave"
          "......Emma"
          "......Fred"
          "......Gail"]
         (vec
           (c/with [org [["Alice" nil]
                         ["Bob" "Alice"]
                         ["Cindy" "Alice"]
                         ["Dave" "Bob"]
                         ["Emma" "Bob"]
                         ["Fred" "Cindy"]
                         ["Gail" "Cindy"]]
                    under-alice [["Alice" 0]]
                    under-alice (c/rel [[name1 boss] org
                                      [name2 level] under-alice
                                      :when (= name2 boss)
                                      :order [level :asc]]
                                       [name1 (inc level)])]
                   (c/rel [[name level] under-alice]
                          (str (subs ".........." 0 (* level 3)) name)))))))

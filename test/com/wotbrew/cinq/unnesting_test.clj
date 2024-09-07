(ns com.wotbrew.cinq.unnesting-test
  (:require [clojure.test :refer :all]
            [com.wotbrew.cinq :as c]))

(deftest simple-unnesting-test
  (is (= '[[:scan
            lineitem
            [[l1__3
              :cinq/self
              true]
             [l1__3:orderkey
              :orderkey
              true]]]
           [:equi-semi-join
            [[:scan
              lineitem
              [[l2__2
                :cinq/self
                true]
               [l2__2:orderkey
                :orderkey
                true]]]
             [:without
              #{l2__2}]]
            (=
              l1__3:orderkey
              l2__2:orderkey)]
           [:without
            #{l1__3:orderkey}]
           [:let
            [[col__1__5
              l1__3]]]
           [:without
            #{l1__3}]
           [:project
            {col__1 col__1__5}]]
         (c/plan (c/rel [l1 lineitem
                       :when (c/exists? [l2 lineitem :when (= l1:orderkey l2:orderkey)])]
                        l1)))))


;; todo mark-join:
#_(c/p [p professors
        :when (or (c/exists? [c courses :when (= c:lecturer p:persid)]) p:sabbatical)]
       p)

;; https://cs.emis.de/LNI/Proceedings/Proceedings241/383.pdf

(defrecord Student [^long id])
(defrecord Exam [^long id ^long sid ^long grade])

(defn unnest-q1 [{:keys [students, exams]}]
  (c/rel [^Student s students
        :join [^Exam e exams (= s:id e:sid)]
        :when (= e:grade (c/scalar [^Exam e2 exams
                                    :when (= s:id e2:sid)
                                    :group []]
                           (c/min e2:grade)))]
         e:exam))

;; needs dataset
(defn unnest-q2 [{:keys [students, exams]}]
  (c/plan (c/rel [^Student s students
                ^Exam e exams
                :when (and (= s:id e:sid)
                           (or (= s:major "CS") (= s:major "Games Eng"))
                           (>= e:grade (c/scalar [^Exam e2 exams
                                                  :when (or (= s:id e2:sid)
                                                            (and (= e2:curriculum s:major)
                                                                 (> s:year e2:date)))]
                                         (+ 1 (c/avg e2:grade)))))]
                 (c/tuple :name s:name, :course e:course))))

(def unnest-dataset
  {:students (vec (for [i (range 1000)] (->Student i)))
   ;; todo hashmap equiv
   :exams (vec (for [i (range 10000)] (->Exam i (rand-int 1000) (rand-nth [0, 1, 2, 3]))))})

(comment

  (time (c/rel-count (unnest-q1 unnest-dataset)))

  )

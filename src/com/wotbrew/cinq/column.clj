(ns com.wotbrew.cinq.column
  (:import (clojure.lang ArrayIter Counted IDeref Indexed)
           (java.io Writer)))

(deftype Column [^objects data]
  IDeref
  (deref [_] data)
  Counted
  (count [_] (alength data))
  Indexed
  (nth [_ i] (aget data i))
  (nth [_ i not-found] (if (< i (alength data)) (aget data i) not-found))
  Iterable
  (iterator [_] (ArrayIter/create data)))

(defmethod print-method Column [^Column col ^Writer w]
  (print-method (vec (.-data col)) w))

(defmacro broadcast [binding expr]
  (if (empty? binding)
    `(->Column (object-array 0))
    (let [col-sym (with-meta (gensym "col") {:tag `Column})
          i-sym (gensym "i")]
      `(let ~binding
         (let [~col-sym ~(first binding)
               data-size# (count ~col-sym)
               out# (object-array data-size#)]
           (dotimes [~i-sym data-size#]
             (let ~(vec (for [[sym _] (partition 2 binding)
                              form [sym `(nth ~sym ~i-sym)]]
                          form))
               (aset out# ~i-sym ~expr)))
           (->Column out#))))))

(defmacro sum
  ([col] `(reduce + 0 ~col))
  ([binding expr] `(sum (broadcast ~binding ~expr))))

(defmacro avg [col] `(let [col# ~col] (/ (sum col#) (count col#))))

(comment

  (let [a (->Column (object-array [1, 2, 3]))
        b (->Column (object-array [3, 4, 5]))]
    (broadcast [a a, b b] (* 1.5 (+ a b b))))


  )

(ns com.wotbrew.cinq.column
  (:import (clojure.lang ArrayIter Counted IDeref Indexed)
           (java.io Writer)))

(definterface IColumn
  (getObject ^Object [^int i]))

(deftype Column [^objects data]
  IDeref
  (deref [_] data)
  Counted
  (count [_] (alength data))
  Indexed
  (nth [_ i] (aget data i))
  (nth [_ i not-found] (if (< i (alength data)) (aget data i) not-found))
  Iterable
  (iterator [_] (ArrayIter/create data))
  IColumn
  (getObject [_ i] (aget data i)))

(defn get-array ^objects [col] @col)

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
                              form [sym `(.getObject ~(with-meta sym {:tag `IColumn}) ~i-sym)]]
                          form))
               (aset out# ~i-sym ~expr)))
           (->Column out#))))))

(defmacro broadcast-reduce [binding init op expr]
  (if (empty? binding)
    `(->Column (object-array 0))
    (let [col-sym (with-meta (gensym "col") {:tag `Column})
          i-sym (gensym "i")]
      `(let ~binding
         (let [~col-sym ~(first binding)
               data-size# (count ~col-sym)
               out# (object-array data-size#)]
           (loop [ret# ~init
                  ~i-sym (int 0)]
             (if (< ~i-sym data-size#)
               (let ~(vec (for [[sym _] (partition 2 binding)
                                form [sym `(.getObject ~(with-meta sym {:tag `IColumn}) ~i-sym)]]
                            form))
                 (recur (~op ret# ~expr) (unchecked-inc-int ~i-sym)))
               ret#)))))))

(defmacro sum
  ([col] `(let [arr# (get-array ~col)]
            (loop [ret# 0
                   i# (int 0)]
              (if (< i# (alength arr#))
                (recur (+ ret# (aget arr# i#)) (unchecked-inc-int i#))
                ret#))))
  ([binding expr] `(broadcast-reduce ~binding 0 + ~expr)))

(defmacro avg
  ([col]
   `(let [col# ~col] (/ (sum col#) (count col#))))
  ([binding expr] `(avg (broadcast ~binding ~expr))))

(defmacro minimum
  ([col]
   `(reduce (fn [a# b#] (if (nil? a#) b# (if (neg? (compare a# b#)) a# b#))) nil ~col))
  ([binding expr] `(minimum (broadcast ~binding ~expr))))

(defmacro maximum
  ([col]
   `(reduce (fn [a# b#] (if (nil? a#) b# (if (pos? (compare a# b#)) a# b#))) nil ~col))
  ([binding expr] `(maximum (broadcast ~binding ~expr))))

(comment

  (let [a (->Column (object-array [1, 2, 3]))
        b (->Column (object-array [3, 4, 5]))]
    (broadcast [a a, b b] (* 1.5 (+ a b b))))


  )

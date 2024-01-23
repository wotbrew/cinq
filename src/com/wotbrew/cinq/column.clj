(ns com.wotbrew.cinq.column
  (:import (clojure.lang ArrayIter Counted IDeref Indexed)
           (java.io Writer)))

(definterface IColumn
  (getObject ^Object [^int i]))

(defn get-array ^objects [^IDeref col] (.deref col))

(deftype Column [^:unsynchronized-mutable ^objects data thunk]
  IDeref
  (deref [this] (or data (let [arr (thunk)] (set! (.-data this) arr) arr)))
  Counted
  (count [this] (alength @this))
  Indexed
  (nth [this i] (aget (get-array this) i))
  (nth [this i not-found] (let [data (get-array this)] (if (< i (alength data)) (aget data i) not-found)))
  Iterable
  (iterator [this] (ArrayIter/create (get-array this)))
  IColumn
  ;; require forcing
  (getObject [_ i] (aget data i)))

(defmethod print-method Column [^Column col ^Writer w]
   (print-method (vec (get-array col)) w))

(defmacro broadcast [binding expr]
  (if (empty? binding)
    `(->Column (object-array 0) nil)
    (let [col-sym (with-meta (gensym "col") {:tag `Column})
          i-sym (gensym "i")]
      `(let ~binding
         ;; force used arrays
         ~@(for [binding (map first (partition 2 binding))] `(get-array ~binding))
         (let [~col-sym ~(first binding)
               data-size# (count ~col-sym)
               out# (object-array data-size#)]
           (dotimes [~i-sym data-size#]
             (let ~(vec (for [[sym _] (partition 2 binding)
                              form [sym `(.getObject ~(with-meta sym {:tag `IColumn}) ~i-sym)]]
                          form))
               (aset out# ~i-sym ~expr)))
           (->Column out# nil))))))

(defmacro broadcast-reduce [binding init op expr]
  (if (empty? binding)
    `(->Column (object-array 0) nil)
    (let [col-sym (with-meta (gensym "col") {:tag `Column})
          i-sym (gensym "i")]
      `((fn bc-reduce-fn# ~(mapv first (partition 2 binding))
          (let [~col-sym ~(first binding)
                data-size# (count ~col-sym)
                out# (object-array data-size#)]
            (loop [ret# ~init
                   ~i-sym (int 0)]
              (if (< ~i-sym data-size#)
                (let ~(vec (for [[sym _] (partition 2 binding)
                                 form [sym `(aget ~(with-meta sym {:tag 'objects}) ~i-sym)]]
                             form))
                  (recur (~op ret# ~expr) (unchecked-inc-int ~i-sym)))
                ret#))))
        ~@(map (fn [[_ x]] `(get-array ~x)) (partition 2 binding))))))

(defn sum1 [col]
  (let [arr (get-array col)]
    (loop [ret (Long/valueOf 0)
           i (int 0)]
      (if (< i (alength arr))
        (recur (+ ret (aget arr i)) (unchecked-inc-int i))
        ret))))

(defmacro sum
  ([col] `(sum1 ~col))
  ([binding expr] `(broadcast-reduce ~binding (Long/valueOf 0) + ~expr)))

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

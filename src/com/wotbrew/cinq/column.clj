(ns com.wotbrew.cinq.column
  (:import (clojure.lang ArrayIter Counted IDeref ILookup Indexed RT)
           (com.wotbrew.cinq CinqUtil)
           (java.io Writer)))

(definterface IColumn
  (getObject ^Object [^int i])
  (sum ^Object []))

;; todo make fully polymorphic
(defn get-objects ^objects [^IDeref col] (if col (.deref col) (object-array 0)))
(defn get-doubles ^doubles [^IDeref col] (if col (.deref col) (double-array 0)))
(defn get-longs ^longs [^IDeref col] (if col (.deref col) (long-array 0)))

(deftype LongColumn [^:unsynchronized-mutable ^longs data thunk]
  IDeref
  (deref [this] (or data (let [arr (thunk)] (set! (.-data this) arr) arr)))
  Counted
  (count [this] (alength (get-longs this)))
  Indexed
  (nth [this i] (aget (get-longs this) i))
  (nth [this i not-found] (let [data (get-longs this)] (if (< i (alength data)) (aget data i) not-found)))
  Iterable
  (iterator [this] (ArrayIter/createFromObject (get-longs this)))
  ILookup
  (valAt [_x _k] nil)
  (valAt [_x _k not-found] not-found)
  IColumn
  ;; require forcing
  (getObject [_ i] (aget data i))
  (sum [this]
    (let [arr (get-longs this)]
      (loop [ret 0
             i 0]
        (if (< i (alength arr))
          (recur (+ ret (aget arr i)) (unchecked-inc i))
          (RT/box ret))))))

(deftype DoubleColumn [^:unsynchronized-mutable ^doubles data thunk]
  IDeref
  (deref [this] (or data (let [arr (thunk)] (set! (.-data this) arr) arr)))
  Counted
  (count [this] (alength (get-doubles this)))
  Indexed
  (nth [this i] (aget (get-doubles this) i))
  (nth [this i not-found] (let [data (get-doubles this)] (if (< i (alength data)) (aget data i) not-found)))
  Iterable
  (iterator [this] (ArrayIter/createFromObject (get-doubles this)))
  ILookup
  (valAt [_ _] nil)
  (valAt [_x _k not-found] not-found)
  IColumn
  ;; require forcing
  (getObject [_ i] (aget data i))
  (sum [this]
    (let [arr (get-doubles this)]
      (loop [ret 0.0
             i 0]
        (if (< i (alength arr))
          (recur (+ ret (aget arr i)) (unchecked-inc i))
          (RT/box ret))))))

(deftype Column [^:unsynchronized-mutable ^objects data thunk]
  IDeref
  (deref [this] (or data (let [arr (thunk)] (set! (.-data this) arr) arr)))
  Counted
  (count [this] (alength (get-objects this)))
  Indexed
  (nth [this i] (aget (get-objects this) i))
  (nth [this i not-found] (let [data (get-objects this)] (if (< i (alength data)) (aget data i) not-found)))
  Iterable
  (iterator [this] (ArrayIter/create (get-objects this)))
  ILookup
  (valAt [x k] (.valAt x k nil))
  (valAt [x k _not-found]
    (Column. nil (fn []
                   (let [arr (get-objects x)]
                     (amap arr idx ret (get (aget arr idx) k))))))
  IColumn
  ;; require forcing
  (getObject [_ i] (aget data i))
  (sum [this]
    (let [arr (get-objects this)]
      (loop [ret (Long/valueOf 0)
             i 0]
        (if (< i (alength arr))
          (let [o (aget arr i)]
            (if (nil? o)
              (recur ret (unchecked-inc i))
              (recur (+ ret o) (unchecked-inc i))))
          ret)))))

(defmethod print-method IColumn [^IColumn col ^Writer w]
  (print-method (mapv (fn [i] (.getObject col (int i))) (range (count col))) w))

(prefer-method print-method IColumn IDeref)

(defn broadcast-bind-type [sym]
  (condp = (:tag (meta sym))
    `DoubleColumn :double
    `Column :object
    (::bind-type (meta sym) :unknown)))

(defn retag-col-sym [sym]
  (case (broadcast-bind-type sym)
    :double (vary-meta sym assoc :tag 'doubles, ::bind-type :double)
    :object (vary-meta sym assoc :tag 'objects, ::bind-type :object)
    :unknown sym))

(defn broadcast-binding [binding]
  (vec (for [[sym expr] (partition 2 binding)
             form (case (broadcast-bind-type sym)
                    :object [(retag-col-sym sym) `(get-objects ~expr)]
                    :double [(retag-col-sym sym) `(get-doubles ~expr)]
                    :unknown [sym `(doto ~expr (deref))])]
         form)))

(defn let-binding [binding]
  (vec (for [[sym expr] (partition 2 binding)
             form [(with-meta sym {}) expr]]
         form)))

(defn broadcast-size [sym]
  (case (broadcast-bind-type sym)
    :unknown `(count ~sym)
    (:double :object) `(alength ~sym)))

(defn broadcast-get [sym i]
  (case (broadcast-bind-type sym)
    :unknown `(.getObject ~(with-meta sym {:tag `IColumn}) ~i)
    (:double :object) `(aget ~sym ~i)))

(defmacro broadcast
  ([binding expr] `(broadcast ->Column object-array true ~binding ~expr))
  ([out-col out-array box binding expr]
   (let [binding (broadcast-binding binding)]
     (if (empty? binding)
       `(~out-col (~out-array 0) nil)
       (let [col-sym (first binding)
             i-sym (gensym "i")]
         `(let ~(let-binding binding)
            (let [data-size# ~(broadcast-size col-sym)
                  out# (~out-array data-size#)]
              (dotimes [~i-sym data-size#]
                (let ~(vec (for [[sym _] (partition 2 binding)
                                 form [(with-meta sym {}) (broadcast-get sym i-sym)]]
                             form))
                  (aset out# ~i-sym ~(if box `(RT/box ~expr) expr))))
              (~out-col out# nil))))))))

(defmacro broadcast-reduce [binding init op expr]
  (if (empty? binding)
    `(->Column (object-array 0) nil)
    (let [binding (broadcast-binding binding)
          col-sym (first binding)
          i-sym (gensym "i")]
      `((fn bc-reduce-fn# ~(mapv first (partition 2 binding))
          (let [data-size# ~(broadcast-size col-sym)]
            (loop [ret# ~init
                   ~i-sym 0]
              (if (< ~i-sym data-size#)
                (let ~(vec (for [[sym _] (partition 2 binding)
                                 form [(with-meta sym {}) (broadcast-get sym i-sym)]]
                             form))
                  (recur (~op ret# ~expr) (unchecked-inc ~i-sym)))
                ret#))))
        ~@(map (fn [[_ x]] x) (partition 2 binding))))))

(defn sum1 [^IColumn col] (if col (.sum col) 0))

(defn sum-default [binding expr]
  ;; todo infer based on return of expr, for now using bindings but this is incorrect
  (let [types (for [[sym] (partition 2 binding)]
                (case (broadcast-bind-type sym)
                  :object :o
                  :double :d
                  :unknown :o))]
    (cond
      (some #{:o} types)
      `(Long/valueOf 0)

      (some #{:d} types)
      0.0

      :else `(Long/valueOf 0))))

(defn simple-sum? [binding expr]
  (and (simple-symbol? expr) (= [expr expr] binding)))

(defmacro sum
  ([col] `(sum1 ~col))
  ([binding expr]
   (if (simple-sum? binding expr)
     `(sum1 ~(first binding))
     `(broadcast-reduce ~binding ~(sum-default binding expr) ~`CinqUtil/sumStep ~expr))))

(defmacro avg
  ([col] `(let [col# ~col] (if (< 0 (count col#)) (/ (sum col#) (count col#)) 0.0)))
  ([binding expr]
   (if (simple-sum? binding expr)
     `(avg ~(first binding))
     ;; finding broadcast returns is an inference problem
     `(avg (broadcast ~binding ~expr)))))

(defmacro minimum
  ([col]
   `(reduce (fn [a# b#] (if (nil? a#) b# (if (neg? (compare a# b#)) a# b#))) nil ~col))
  ([binding expr] `(minimum (broadcast ~binding ~expr))))

(defmacro maximum
  ([col]
   `(reduce (fn [a# b#] (if (nil? a#) b# (if (pos? (compare a# b#)) a# b#))) nil ~col))
  ([binding expr] `(maximum (broadcast ~binding ~expr))))

(defn count-some1 [^IColumn col]
  (let [size (count col)]
    (loop [ret 0
           i 0]
      (if (< i size)
        (let [o (.getObject col i)]
          (if (nil? o)
            (recur ret (unchecked-inc i))
            (recur (unchecked-inc ret) (unchecked-inc i))))
        ret))))

(defmacro count-some
  ([col] `(count-some1 ~col))
  ([binding expr] `(sum ~binding (if (nil? ~expr) 0 1))))

(comment

  (let [a (->Column (object-array [1, 2, 3]))
        b (->Column (object-array [3, 4, 5]))]
    (broadcast [a a, b b] (* 1.5 (+ a b b))))

  )

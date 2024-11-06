(ns com.wotbrew.cinq.npred
  (:require [com.wotbrew.cinq.nio-codec :as codec])
  (:import (java.nio ByteBuffer)))

(declare eval-bc-next)

(defn eval-bc-eq [^ByteBuffer prog ^ByteBuffer val symbol-list]
  (let [olimit (.limit prog)
        object-size (.getInt prog)]
    (.limit prog (+ (.position prog) object-size))
    (let [prog-tid (.getLong prog (.position prog))
          _ (.mark prog)
          ret (codec/compare-bufs* prog-tid prog val symbol-list)]
      (.limit prog olimit)
      (.reset prog)
      (.position prog (+ (.position prog) object-size))
      (= 0 ret))))

(defn- focus-key [^ByteBuffer key-buffer ^ByteBuffer val-buffer]
  (let [_ (.mark val-buffer)
        tid (.getLong val-buffer)
        ret
        (cond
          (= codec/t-nil tid) false
          (= codec/t-small-map tid)
          (let [len (.getInt val-buffer)
                key-size (.getInt val-buffer)
                _val-size (.getInt val-buffer)
                offset-pos (.position val-buffer)
                offset-table-size (unchecked-multiply-int len 4)
                key-start-pos (unchecked-add-int offset-pos offset-table-size)
                val-start-pos (unchecked-add-int key-start-pos key-size)]
            (if (= 0 len)
              false
              (do
                (.position val-buffer (+ (.position val-buffer) (* len 4)))
                (loop []
                  ;; todo much easier if dynamic objects include their size after their t.
                  ;; (codec/decode-size)
                  ;; (codec/skip-object)
                  )

                )))
          ;; todo t-big-map
          :else false)]
    (.reset val-buffer)
    ret))

(defn eval-bc-key [^ByteBuffer prog heap ^ByteBuffer val symbol-list]
  (let [prog-limit (.limit prog)
        key-len (.getInt prog)
        _ (.limit prog key-len)]
    (let [o-pos (.position val)
          o-limit (.limit val)
          key-found (focus-key prog val)
          _ (.position prog (+ (.position prog) key-len))
          _ (.limit prog prog-limit)
          ret (and key-found (eval-bc-next prog heap val symbol-list))]
      (.position val o-pos)
      (.limit val o-limit)
      ret)))

(defn eval-bc-or [^ByteBuffer prog ^objects heap ^ByteBuffer val symbol-list]
  (let [n (.getInt prog)]
    (loop [i 0]
      (if (= n i)
        false
        (if (eval-bc-next prog heap val symbol-list)
          true
          (recur (unchecked-inc i)))))))

(defn eval-bc-and [^ByteBuffer prog ^objects heap ^ByteBuffer val symbol-list]
  (let [n (.getInt prog)]
    (loop [i 0]
      (if (= n i)
        true
        (if (eval-bc-next prog heap val symbol-list)
          (recur (unchecked-inc i))
          false)))))

(defn eval-bc-next [^ByteBuffer prog ^objects heap ^ByteBuffer val symbol-list]
  (case (.get prog)
    0 (not (eval-bc-next prog heap val symbol-list))
    1 (eval-bc-and prog heap val symbol-list)
    2 (eval-bc-or prog heap val symbol-list)
    3 (eval-bc-eq prog val symbol-list)
    #_#_ 4 (e-pred-lt prog val)
    #_#_ 5 (e-pred-lte prog val)
    #_#_ 6 (e-pred-gt prog val)
    #_#_ 7 (e-pred-gte prog val)
    #_#_ 8 (e-pred-in prog heap val)
    #_#_ 9 (eval-bc-key prog heap val)
    ))

(ns idea)

;; Clojure environment is the database environment
;; problems:
;; That would suggest one set of relvars per process. No chance of multiple database files.

;; threads.

;; open transaction model
;; intentional transactions.

;; dynamic vars
;; sub-environments
;; careful with threads and lazy-ness (queries in lazy seqs though? why?)
;; still get the sense you are 'in clojure' and not sending messages to an object, or interacting with a handle.

(defmacro def-var [& _])
(defmacro def-view [& _])
(defmacro q [& _])

;; the repl may give you an implied environment, call (use-global file) or (use-global :tmp)

;; schema can intern a defrecord whose type can be specialised for in scan (in memory specialisation)
;; or possibly a custom scanner (where relation value is file or something), will need to change hints to use these fields
(def-var customers {:name "customer_entity" :schema {:id int?}, :size 1.0})
(def-var orders {:schema {:customer-id int?}, :size 2.0})

;;
(def-view order-summary
          [o orders c customers
           :when (= c:order-id o:order-id)]
          {:order-id o:id
           :customer (str c:firstname " " c:lastname)})

;; macroexpansion of view contents (if possible set up dependency to force a recompile if view definition changes)
;; - types would be lost in closure capture if done manually? perhaps we could use a clojure lambda and find the fields?
(q [os order-summary] (str os:customer " - " os:order-id))

(defmacro database [& transaction])

(defmacro set-var [relvar relation])

(def my-database
  ;; database form will create a new database, a new context.
  (database
    ;; full transaction permits arbitrary code
    ;; always a snapshot.

    ;; var assignment
    ;; internally will this not delete all tuples?
    ;; supporting snapshot?
    (c/assign customers [])
    (c/assign orders [])

    ;; can take the relation value of a var with @
    ;; snapshot at tx?
    (let [cust0 @customers])

    ;; within the transaction the customers relation will have changed
    (c/insert customers {:id 42, :firstname "Dan", :lastname "Stone"})
    ;; can probably use a query or collection as well
    (c/insert customers (q [c [{:id 42, :firstname "Dan", :lastname "Stone"}]] ($select :id c:id :firstname c:firstname :lastname c:lastname)))

    ;; internally, all visited tuples need to yield their tuple-id for fast modification
    ;; updates could update more than relation variable
    (c/update [c customers
             :when (= c:id 42)]
            :age (or c:age 42))

    ;; delete needs to use a row alias in selection position as to disambiguate
    (c/delete [c customers :when (= c:id 42)] c)

    ;; internal tuple id delete
    (c/delete-tuple orders 342444)

    ))

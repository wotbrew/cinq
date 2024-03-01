(ns scratch2
  (:require [com.wotbrew.cinq :as c]))

(c/def-var users)
(c/def-var orders)
(c/def-var products)

(c/use (c/mem-db)

  (c/insert users {:name "Dan", :age 42})
  (c/insert users {:name "Jon", :age 41})
  (c/insert users {:name "Jeremy", :age 36})

  (c/run! [u users :when (= u:age 42)] (c/delete-tuple users u:cinq/tid))

  @users
  )

(def db
  (c/mem-db

    (c/assign users [{:id "dan" :name "Dan"}])
    (c/assign orders [{:id 0, :user "dan", :delivery-date #inst "2024-02-29", :items {"eggs" 2, "bacon" 1}}])
    (c/assign products [{:id "eggs", :name "Eggs", :price 3.14}])

    ))

(c/do! db
  (c/insert products {:id "bacon", :name "Bacon", :price 3.99})
  (c/q [o orders
        :join [u users (= u:id o:user)]]
    {:name u:name
     :price (c/scalar [[item qty] o:items
                       :join [p products (= p:id item)]
                       :group []]
                      (c/sum (* p:price qty)))})
  )

(c/use db (c/q [p @products] p:name))

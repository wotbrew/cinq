# cinq

![tests](https://github.com/wotbrew/cinq/actions/workflows/tests.yml/badge.svg)
[![Clojars Project](https://img.shields.io/clojars/v/com.wotbrew/cinq.svg?include_prereleases)](https://clojars.org/com.wotbrew/cinq)

(C)lojure (In)tegrated (Q)uery extends Clojure on the JVM to direct and immediate relational programming against both collections and durable LMDB backed variables.

- **Embedded Query**
  - Supports (outer) joins, sorts, aggregations and recursive common table expressions (CTEs).
- **Single-file Databases**  
  - ACID transactions
  - Indexes
- **Clojure integrated**
  - Use Clojure expressions, functions within queries and transactions
  - Queries inherit the local clojure environment (no parameter placeholders, feels like a `for` loop).
  - Relations are first-class, printable, support reduce so you can use them with core collection functions
  
> **note** cinq is under development, many basic things do not work - and it will receive breaking API changes. SNAPSHOT only, use at your own risk.

```clojure 
;; project.clj
[com.wotbrew/cinq "0.1.0-SNAPSHOT"]
;; deps.edn
com.wotbrew/cinq {:mvn/version "0.1.0-SNAPSHOT"}
```

## Getting started (LMDB)

Eventually, LMDB support will be externalized into a seperate library, but for now its an optional namespace you can include. 

Add `lmdb-java` to your dependencies: 

```clojure
[org.lmdbjava/lmdbjava "0.9.0"]
org.lmdbjava/lmdbjava {:mvn/version "0.9.0"}
```

Ensure the following JVM options are set, as `lmdb-java` uses `sun.misc.Unsafe`. 

```
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

Require `cinq`, `lmdb` support is currently provided as another namespace in the `cinq` jar.
```clojure 
(require '[com.wotbrew.cinq :as c])
(require '[com.wotbrew.cinq.lmdb :as lmdb])
```

Create a new database

```clojure
(def db (lmdb/database "mydb.cinq"))
```

Create a new relational variable (think table in SQL).

```clojure
(def employees (c/create db :employees))
```

Set the variables initial value

```clojure
(c/rel-set employees [{:id 1, :name "Alice", :department "Engineering"}
                      {:id 2, :name "Bob", :department "HR"}])
```

Perform a query

```clojure 
(c/q [e employees
      :when (= "Engineering" (:department e))]
  e)
;; =>
[{:id 1, :name "Alice", :department "Engineering"}]
```

Modify the database

```clojure 
(c/insert employees {:id 3, :name "Charlie", :department "Marketing"})
```

### Next steps

- See `c/write` for serialized exclusive write transactions
- See `c/read` for consistent snapshot read transactions
- See `c/index` to create indexes
  - `c/lookup`, `c/range`, `c/top-k`, `c/bottom-k`, `c/asc`, `c/desc` to use them.
- See `c/run` for issuing effects in a query, such as deletes or updates
- See `c/delete`, `c/replace`, `c/update` for further CRUD

## Query

With `cinq` the programmer writes queries in the style of a clojure `for` loop. A common way to query will be to use the `q` macro.

```clojure 
(require '[com.wotbrew.cinq :as c])

(def orders 
  [{:customer-id 0, :gross 3.0, :discount 0.0}
   {:customer-id 1, :gross 40.0, :discount 1.00}])

(def customers 
  [{:id 0, :country-code "GB"}
   {:id 1, :country-code "GB"}])

(c/q [c customers 
      o orders 
      :when (= (:customer-id o) (:id c))
      :group [country (:country-code c)]]
  {:country-code country 
   :revenue (c/sum (- (:gross o) (:discount o)))})
;; =>
[{:country-code "GB"
  :revenue 42.0}]
```

You can use this macro anywhere in your code. It looks at the query, runs it through a relational optimiser (at compile time), and emits an eager loop that will materialize the result-set as a vector.

### Differences with `clojure.core/for`

- eager, not lazy (`q` returns a vector, there is a Reducable verson `rel` for more control of this)
- supports different keyword operators, alongside `:let` and `:when` such as: `:join`, `:left-join`, `:group`, `:order`, `:limit`
- query is optimised ahead of codegen, predicate push-down, de-nesting sub queries, join optimisation
- emitted loop is likely entirely fused with minimal megamorphic dispatch 
- when intermediate tuples need to be materialized (such as for joins or certain grouping operations) custom struct types are compiled with appropriate scalar field hints.
- `nil` has different equality behaviour, for certain rewrites to possible, nil cannot be equal to nil, so within a cinq query `(= nil anything)` will aways return false. (You can still use `nil?`). You may later be able to configure this behaviour.

### Differences with a 'normal database query'

- query is not quoted, it inherits locals and parameters from the Clojure environment.
- planner performs 'generally good' optimisations only, the user is responsible for join order and index selection.
- code is planned and compiled just once, new plans are not generated over time.
- there is no difference between querying collections and querying database tables for the user of this library.

## Database

In `cinq` a database is a mapping of names to relational variables. 

You can use `com.wotbrew.cinq.lmdb` for a database that lives in a single file. SQLite style.

This allows `cinq` to work with very large relations.

```clojure
(require '[com.wotbrew.cinq.lmdb :as lmdb])

(def db (lmdb/database "test.cinq"))

;; create a new relation variable (relvar). 
(c/create db :customers)

;; to get the :customers relvar, look it up in the map. The relvar starts empty
(:customers db)
;; =>
#cinq/rel []

;; One way to change the value associated with the relvar is to replace it with rel-set
(c/rel-set (:customers db) [{:name "Bob"} {:name "Alice"}])
;; =>
#cinq/rel [{:name "Bob"} {:name "Alice"}]

;; Query as if it is a normal collection
(c/q [c (:customers db) :when (str/starts-with? c:name "A")] c)
;; => 
[{:name "Alice"}]

;; lmdb database come with a few initial relations for statistics and what not
(:lmdb/variables db)
;; => 
#cinq/rel [:lmdb/stat, :lmdb/variables, :lmdb/symbols, :customers]

;; databases are Closeable, though the process exiting is also fine, LMDB is pretty good.
(.close db)
```

- relvars are reducable, so you can use `reduce`, `transduce` `into` and `vec`, `set` on them.
- Intentionally no `.iterator`, `seq` (otherwise memory management would be more fun).
- relvars can contains anything that can be encoded, not just maps (see [encoding](#encoding)) e.g a relation of bare ints or keywords would be fine.
- relvars can be very big, remember to set `*print-length*`.

### CRUD

#### `rel-set`

Give the relvar a new value.

```clojure 
(c/rel-set (:customers db) [{:name "Dave"}])
;; => #cinq/rel [{:name "Dave"}]
```

#### Insert

Add a row into a relation variable, the row can be anything [encodable](#encoding). It returns a new `row sequence number` or `rsn` (that is probably not going to be relevant for some time!). 

```clojure 
(c/insert (:customers db) [{:name "Jikl"}]) ;; => 2
```

#### Replace 

Use `c/replace` to swap out a tuple in a `c/run` body. This lets you target any tuple in a query and replace its value.

```clojure 
(c/run 
  [c (:customers db) 
   :when (= "Jikl" c:name)]
  (c/replace c (assoc c :name "Jill")))
```

#### Update

Use `c/update` to apply a function to a tuple and replace its value with the result. Usable in a `c/run` body.

```clojure 
(c/run
  [c (:customers db)
   :when (= "Jikl" c:name)]
  (c/update c assoc :name "Jill"))
```

#### Delete

Use `c/delete` to delete a targeted tuple. Usable in a `c/run` body.

```clojure 
(c/run 
  [c (:customers db)
   :when (= "Jill" c:name)]
  (c/delete c))
```

You can mix and match update, delete, replace, and any other arbitrary side effect in a single run expression.

### Transactions

Transactions are a bit like databases.

They map names to transactional relvars.

Reads of these relvars will remain consistent with each other within the transaction. 
Any changes will be committed together across all relvars at the end of the transaction.

#### `read`

```clojure 
(c/read [tx db] (c/q [c (:customers tx) :when (= 42 c:id)] c))
```

#### `write`
```clojure 
(c/write [tx db] (c/insert (:customers tx) {:name "Jeremy"}))
```

## Encoding

Supports most clojure data. No meta. Not everything.

Working right now:

- nil, booleans, ints, longs, doubles, floats.
- keyword, symbol, string
- maps, sets, vectors
- java.util.Date, java.util.UUID

Collections must not be mutated as you are writing them otherwise you can corrupt your database. I plan to put in guards against this (throw an exception).

Anything else is TBD

## License

Copyright 2024 Dan Stone (wotbrew)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# cinq

Language integrated query and ACID relational database for Clojure.

- Very fast (for the JVM) query compiler. Emits unboxed math, fused loops - applies relational planner optimisations such as sub query de-nesting.
- Optional single-file databases with full ACID transactions.
- Integrated with Clojure and the REPL, reified relations, query clojure things, use clojure expressions directly, write transactions in clojure.
- Relation-first programming style, where relations are first-class objects that you operate on - as opposed to a 'send your query to the database' style).

## Query

With `cinq` the programmer writes queries in the style of a clojure `for` loop.

In the below example `lineitem` might be a collection (say in a vector) or a relation variable, containing a larger-than-memory relation, sourced from a database. 

```clojure 
(q [l lineitem
    :when (<= l:shipdate #inst "1998-09-02")
    :group [returnflag l:returnflag, linestatus l:linestatus]
    :order [returnflag :asc, linestatus :asc]]
  {:l_returnflag returnflag
   :l_linestatus linestatus
   :sum_qty (c/sum l:quantity)
   :sum_base_price (c/sum l:extendedprice)
   :sum_disc_price (c/sum (* l:extendedprice (- 1.0 l:discount)))
   :sum_charge (c/sum (* l:extendedprice (- 1.0 l:discount) (+ 1.0 l:tax)))
   :avg_qty (c/avg l:quantity)
   :avg_price (c/avg l:extendedprice)
   :avg_disc (c/avg l:discount)
   :count_order (c/count)})
```

These queries go through extensive optimisation at compile time to emit very efficient code. 

The optional `a:foo`, `a:ns/foo` are quick lookups that desugar to `(:foo a)`, `(:ns/foo a)` this avoid noise when you have a lot of joins. I like it, but its optional, feel free to destructure/use kw lookups instead. Both still get optimised away (to e.g register/offset/field lookups) if they can be.

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
#cinq/rel [{:name "Alice"}]

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

#### `insert`

Add a row into a relation variable, the row can be anything [encodable](#encoding). It returns a new `row sequence number` or `rsn` (that is probably not going to be relevant for some time!). 

```clojure 
(c/insert (:customers db) [{:name "Jikl"}]) ;; => 2
```

#### `update`

Update takes a query that identifies the rows you want to replace (always the first binding), and an expr that returns the new value.

Returns the number of affected rows.

```clojure 
(c/update [c (:customers db) :when (= "Jikl" c:name)] (assoc c :name "Jill"))
```

#### `delete`

Delete takes a query that identifies the rows to delete. The first binding is assumed to be the relvar you are deleting from.

Returns the number of affected rows.

```clojure 
(c/delete [c (:customers db) :when (= "Jill" c:name)])
```

### Transactions

Transactions are a bit like databases.

They map names to transactional relvars.

Reads of these relvars will remain consistent with each other within the transaction. 
Any changes will be committed together across all relvars at the end of the transaction.

#### `read`

```clojure 
(c/read [tx db] (:customers tx))
```

#### `write`
```clojure 
(c/write [tx db] (c/insert (:customers tx) {:name "Jeremy"}))
```

## Encoding

Supports most clojure data. No meta. Not everything.

Working right now:

- nil, bool, ints, longs, doubles, floats.
- keyword, symbol, string
- maps, sets, vectors (or seq/lists, they both get encoded as vectors)
- java.util.Date

Collections must not be mutated as you are writing them otherwise you can corrupt your database. I plan to put in guards against this (throw an exception).

Anything else is TBD

## How does it deal with `x`? 

See [todo.txt](todo.txt) for some thoughts

## Usage

Not yet.

## Plans that may or may not happen 

- relic2 (incremental view maintenance)

## License

Copyright 2024 Dan Stone (wotbrew)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

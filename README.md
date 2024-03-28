# cinq

Language integrated query for Clojure.

`cinq` allows the programmer to write queries of collections in the style of a clojure `for` loop.

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

- Extremely efficient collection processing, fused loops, one-pass summarization and unboxed math.
- Joins, aggregation, nested sub queries, database-level query as a macro.
- Relational planner and optimiser, push-down, de-correlation, join algorithm selection.
- Query arbitrary collections, optimisations for collections of records.
- Use arbitrary clojure code in queries, planner is sensitive to things like nil-safety.

## Usage

Not yet.

## Plans that may or may not happen 

- language integrated database (disk, bigger than memory)
- relic2 (incremental view maintenance)

## License

Copyright 2024 Dan Stone (wotbrew)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

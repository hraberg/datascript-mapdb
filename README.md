# datascript-mapdb

Datascript backed by MapDB, based on an idea by
[Ernestas Lisauskas](https://github.com/ernestas).

Experimental, uses `with-redefs` to monkey-patch Datascript and
replace its in-memory B+ tree implementation. Not tested except for
the most trivial cases, performance unknown.

## Usage

```clojure
  ;; Taken from https://github.com/tonsky/datascript
  (with-mapdb [mapdb (new-mapdb-file "test-db")]
    (let [schema {:aka {:db/cardinality :db.cardinality/many}}
          conn   (create-mapdb-conn mapdb schema)]
      (d/transact! conn [{ :db/id -1
                          :name  "Maksim"
                          :age   45
                          :aka   ["Maks Otto von Stirlitz", "Jack Ryan"] } ])
      (d/q '[ :find  ?n ?a
             :where [?e :aka "Maks Otto von Stirlitz"]
             [?e :name ?n]
             [?e :age  ?a] ]
             @conn)))
```

## References

* https://github.com/tonsky/datascript
* http://www.mapdb.org/doc/btreemap.html

## License

Copyright © 2016 Håkan Råberg

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

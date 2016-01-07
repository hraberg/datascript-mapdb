(ns datascript-mapdb.core
  (:require [datascript.btset :as btset]
            [datascript.core :as d]
            [datascript.db :as db])
  (:import [org.mapdb BTreeKeySerializer$BasicKeySerializer Serializer DBMaker]
           [java.io DataInput DataOutput File]
           [java.util NavigableSet]))

;; Uses with-redefs to replace datascript.btset with a
;; org.mapdb.BTreeMap backed set stored on disk.

(def datascript-cmp->index {db/cmp-datoms-eavt :eavt
                            db/cmp-datoms-aevt :aevt
                            db/cmp-datoms-avet :avet})

(def edn-mapdb-serializer
  (proxy [Serializer] []
    (serialize [^DataOutput out value]
      (.writeUTF out (pr-str value)))

    (deserialize [^DataInput in available]
      (read-string (.readUTF in)))

    (isTrusted [] true)))

(defn mapdb-btset-by [mapdb cmp]
  (.treeSet mapdb (name (datascript-cmp->index cmp))
            (BTreeKeySerializer$BasicKeySerializer. edn-mapdb-serializer cmp)))

(defn mapdb-btset-slice
  ([set key]
   (mapdb-btset-slice set key key))
  ([^NavigableSet set key-from key-to]
   (.subSet set key-from true key-to true)))

(defn mapdb-btset-conj [set key _]
  (doto set (.add key)))

(defn mapdb-btset-disj [set key _]
  (doto set (.remove key)))

(defn mapdb-btset-bindings [mapdb]
  {#'btset/btset-by (partial mapdb-btset-by mapdb)
   #'btset/slice mapdb-btset-slice
   #'btset/btset-conj mapdb-btset-conj
   #'btset/btset-disj mapdb-btset-disj})

(defn install-mapdb-btset-bindings! [mapdb]
  (doseq [[k v] (mapdb-btset-bindings mapdb)]
    (alter-var-root k (constantly v))))

(defn mapdb-metadata [mapdb]
  (.treeMap mapdb "metadata"))

(defn write-mapdb-metadata [mapdb {:keys [max-eid max-tx]}]
  (doto (mapdb-metadata mapdb)
    (.put "max-eid" max-eid)
    (.put "max-tx" max-tx)))

(defn read-mapdb-metadata [mapdb]
  (let [{:strs [max-eid max-tx]
         :or {max-eid 0 max-tx db/tx0}} (mapdb-metadata mapdb)]
    {:max-eid max-eid
     :max-tx max-tx}))

;;; Public API

(defn create-mapdb-conn
  ([mapdb]
   (create-mapdb-conn mapdb db/default-schema))
  ([mapdb schema]
   (doto (d/create-conn schema)
     (swap! merge (read-mapdb-metadata mapdb))
     (d/listen! (fn [{:keys [db-after]}]
                  (write-mapdb-metadata mapdb db-after))))))

(defn with-mapdb-fn [mapdb f]
  (with-redefs-fn (mapdb-btset-bindings mapdb)
    #(with-open [mapdb mapdb]
       (f))))

(defmacro with-mapdb [[name init] & body]
  `(let [~name ~init]
     (with-mapdb-fn ~name
       #(do ~@body))))

(defn new-mapdb-file [file]
  (-> (DBMaker/newFileDB (File. file))
      .transactionDisable
      .fileMmapEnable
      .make))

(comment
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
           @conn))))

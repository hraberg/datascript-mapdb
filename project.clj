(defproject datascript-mapdb "0.1.0-SNAPSHOT"
  :description "Datascript backed by MapDB"
  :url "http://github.com/hraberg/datascript-mapdb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0-alpha5"]
                 [datascript "0.13.2"]
                 [org.mapdb/mapdb "2.0-beta12"]]
  :pedantic? :abort)

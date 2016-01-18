(ns datascript-mapdb.art-test
  (:require [datascript-mapdb.art :as art]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.test :refer [deftest is]])
  (:import [datascript_mapdb.art Node4 Node16 Node48 Node256]))

(deftest grow-nodes
  (loop [n 0
         tree (art/art-make-tree)]
    (let [tree (art/art-insert tree n n)]
      (is (= n (art/art-lookup tree n)))
      (instance?
       (cond
         (< n 4) Node4
         (< n 16) Node16
         (< n 48) Node48
         (< n 256) Node256)
       tree)
      (when (< n 255)
        (recur (inc n) tree)))))

(defn insert-all [keys]
  (reduce art/art-insert (art/art-make-tree) keys))

(defn lookup-all [tree keys]
  (map (partial art/art-lookup tree) keys))

(deftest shared-paths
  (let [keys ["a" "ab" "ac" "aba" "b" "ba"]]
    (is (= keys (-> (insert-all keys)
                    (lookup-all keys))))))

(deftest uuids
  (let [uuids (-> (io/resource "uuid.txt")
                  io/reader
                  line-seq)]
    (is (= 100000 (count uuids)))
    (is (= uuids (-> (insert-all uuids)
                     (lookup-all uuids))))))

;; words.txt
;; {Leaf 235886
;;  Node4 111616
;;  Node16 12181
;;  Node48 458
;;  Node256 1}

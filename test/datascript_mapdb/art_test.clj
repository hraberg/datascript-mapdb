(ns datascript-mapdb.art-test
  (:require [datascript-mapdb.art :as art]
            [clojure.java.io :as io]
            [clojure.walk :as w]
            [clojure.test :refer [deftest is]]))

(deftest grow-nodes
  (loop [n 0
         tree (art/art-make-tree)]
    (let [key (byte-array [n])
          tree (art/art-insert tree key n)]
      (is (= n (art/art-lookup tree key)))
      (is (= (condp > n
               4 "Node4"
               16 "Node16"
               48 "Node48"
               256 "Node256")
             (.getSimpleName (class tree))))
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

(deftest non-existent-keys
  (let [keys ["aa" "ab" "aba" "abc" "ad"]]
    (is (every? nil?
                (-> (insert-all keys)
                    (lookup-all ["b" "ac" "aabc" "de" "a"]))))))

(defn read-lines [resource]
  (-> (io/resource resource)
      io/reader
      line-seq))

(deftest uuids
  (let [uuids (read-lines "uuid.txt")
        tree (insert-all uuids)]
    (is (= 100000 (count uuids)))
    (is (= uuids (lookup-all tree uuids)))
    (is (= "00026bda-e0ea-4cda-8245-522764e9f325" (art/art-minimum tree)))
    (is (= "ffffcb46-a92e-4822-82af-a7190f9c1ec5" (art/art-maximum tree)))))

(def object-array-class (class (object-array 0)))

(deftest words
  (let [words (read-lines "words.txt")
        tree (insert-all words)
        counts (volatile! {})]
    (is (= 235886 (count words)))
    (is (= words (lookup-all tree words)))

    (is (= "A" (art/art-minimum tree)))
    (is (= "zythum" (art/art-maximum tree)))

    (w/prewalk #(do (when (record? %)
                      (vswap! counts update (.getSimpleName (class %)) (fnil inc 0)))
                    (cond-> %
                      (instance? object-array-class %) vec))
               tree)

    (is (= {"Leaf" 235886
            "Node4" 111616
            "Node16" 12181
            "Node48" 458
            "Node256" 1}
           @counts))))

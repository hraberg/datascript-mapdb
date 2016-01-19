(ns datascript-mapdb.art-test
  (:require [datascript-mapdb.art :as art]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]))

(deftest grow-nodes
  (loop [n 0
         tree (art/art-make-tree)]
    (let [key (byte-array [n])
          tree (art/art-insert tree key n)]
      (is (= n (art/art-lookup tree key)))
      (is (= (condp > n
               4 (class art/empty-node4)
               16 (class art/empty-node16)
               48 (class art/empty-node48)
               256 (class art/empty-node256))
             (class tree)))
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
  (let [uuids (read-lines "uuid.txt")]
    (is (= 100000 (count uuids)))
    (is (= uuids (-> (insert-all uuids)
                     (lookup-all uuids))))))

(deftest words
  (let [words (read-lines "words.txt")]
    (is (= 235886 (count words)))
    (is (= words (-> (insert-all words)
                     (lookup-all words))))))

;; words.txt
;; {Leaf 235886
;;  Node4 111616
;;  Node16 12181
;;  Node48 458
;;  Node256 1}

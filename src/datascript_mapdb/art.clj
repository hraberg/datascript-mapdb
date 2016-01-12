(ns datascript-mapdb.art
  (:import [java.util Arrays]))

;;; Persistent Adaptive Radix Tree
;; see http://www3.informatik.tu-muenchen.de/~leis/papers/ART.pdf

(defprotocol ARTNode
  (lookup [this key-byte])
  (insert [this key-byte value]))

(defrecord Leaf [key value])

(defn key-position ^long [^long count ^"[B" keys ^long key-byte]
  (Arrays/binarySearch keys 0 count (byte key-byte)))

(defn lookup-helper [^long count keys ^objects nodes key-byte]
  (let [pos (key-position count keys key-byte)]
    (when-not (neg? pos)
      (aget nodes pos))))

(defn grow-helper [^long count ^"[B" keys ^objects nodes node]
  (loop [idx 0
         node node]
    (if (< idx count)
      (recur (inc idx) (insert node (aget keys idx) (aget nodes idx)))
      node)))

(defn insert-helper [count ^"[B" keys ^objects nodes key-byte value make-node grow-node]
  (let [pos (key-position count keys key-byte)
        new-keys (aclone keys)
        new-nodes (aclone nodes)
        node-size (clojure.core/count keys)]
    (if (neg? pos)
      (if (< (long count) node-size)
        (let [pos (-> pos inc Math/abs)]
          (aset new-keys pos (byte key-byte))
          (aset new-nodes pos value)
          (loop [idx (inc pos)]
            (when (< idx node-size)
              (aset new-keys idx (aget keys (dec idx)))
              (aset new-nodes idx (aget nodes (dec idx)))
              (recur (inc idx))))
          (make-node (inc (long count)) new-keys new-nodes))
        (insert (grow-helper count keys nodes (grow-node)) key-byte value))
      (make-node count
                 (doto new-keys (aset pos (byte key-byte)))
                 (doto new-nodes (aset pos value))))))

(declare make-node16 make-node48 make-node256)

(defrecord Node4 [^long count ^"[B" keys ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (lookup-helper count keys nodes key-byte))

  (insert [this key-byte value]
    (insert-helper count keys nodes key-byte value ->Node4 make-node16)))

(defrecord Node16 [^long count ^"[B" keys ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (lookup-helper count keys nodes key-byte))

  (insert [this key-byte value]
    (insert-helper count keys nodes key-byte value ->Node16 make-node48)))

(defrecord Node48 [^long count ^"[B" key-index ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (let [pos (aget key-index (byte key-byte))]
      (when (< pos (clojure.core/count nodes))
        (aget nodes pos))))

  (insert [this key-byte value]
    (let [idx (aget key-index key-byte)
          idx (byte (if (= (clojure.core/count nodes) idx)
                      (inc count)
                      idx))
          node-size (clojure.core/count nodes)]
      (if (< idx node-size)
        (make-node48 idx
                     (doto (aclone key-index) (aset (byte key-byte) idx))
                     (doto (aclone nodes) (aset idx value)))
        (loop [key 0
               node (make-node256)]
          (if (< key (clojure.core/count key-index))
            (let [idx (aget key-index key)]
              (recur (inc key) (cond-> node
                                 (< idx node-size) (insert key (aget nodes idx)))))
            (insert node key-byte value)))))))

(defrecord Node256 [^long count ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (aget nodes key-byte))

  (insert [this key-byte value]
    (->Node256 (cond-> count
                 (nil? (aget nodes key-byte)) inc)
               (doto (aclone nodes) (aset key-byte value)))))

(defn make-node4 []
  (->Node4 0 (byte-array 4 (byte 4)) (object-array 4)))

(defn make-node16 []
  (->Node16 0 (byte-array 16 (byte 16)) (object-array 16)))

(defn make-node48 []
  (->Node48 0 (byte-array 256 (byte 48)) (object-array 48)))

(defn make-node256 []
  (->Node256 0 (object-array 256)))

;;; Public API

(defn art-make-tree []
  (make-node4))

(defn art-lookup [tree ^"[B" key-bytes]
  (loop [idx 0
         node tree]
    (if (instance? Leaf node)
      (let [^Leaf leaf node]
        (when (Arrays/equals key-bytes (bytes (.key leaf)))
          (.value leaf)))
      (when node
        (recur (inc idx) (lookup node (aget key-bytes idx)))))))

(defn art-insert [tree ^"[B" key-bytes value]
  (loop [idx 0
         node tree]
    (let [child (lookup node (aget key-bytes idx))]
      (if (and (satisfies? ARTNode child)
               (< (clojure.core/count key-bytes) (dec idx)))
        (recur (inc idx) child)
        (insert node (aget key-bytes idx) (->Leaf key-bytes value))))))

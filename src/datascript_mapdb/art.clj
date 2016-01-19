(ns datascript-mapdb.art
  (:require [clojure.java.io :as io]
            [clojure.string :as s])
  (:import [java.util Arrays Date]
           [java.nio ByteBuffer ByteOrder]))

;;; Persistent Adaptive Radix Tree
;; http://www3.informatik.tu-muenchen.de/~leis/papers/ART.pdf
;; https://github.com/armon/libart
;; https://github.com/kellydunn/go-art
;; https://github.com/ankurdave/part.git

;; TODO: implement path compression.
;; Not suitable for Datascript indexes as they use Datoms as keys.

(defprotocol ARTNode
  (lookup [this key-byte])
  (insert [this key-byte value]))

(defprotocol ARTKey
  (to-key-bytes [this]))

(defn key-position ^long [^long size ^bytes keys ^long key-byte]
  (Arrays/binarySearch keys 0 size (byte key-byte)))

(defn lookup-helper [^long size keys ^objects nodes ^long key-byte]
  (let [pos (key-position size keys key-byte)]
    (when-not (neg? pos)
      (aget nodes pos))))

(defn make-gap [^long size ^long pos src dest]
  (System/arraycopy src pos dest (inc pos) (- size pos)))

(defn grow-helper [^long size ^bytes keys ^objects nodes node]
  (loop [idx 0
         node node]
    (if (< idx size)
      (recur (inc idx) (insert node (aget keys idx) (aget nodes idx)))
      node)))

(defn insert-helper [size ^bytes keys ^objects nodes key-byte value make-node empty-larger-node]
  (let [size (long size)
        pos (key-position size keys key-byte)
        new-key? (neg? pos)]
    (if (and new-key? (= size (count keys)))
      (insert (grow-helper size keys nodes empty-larger-node) key-byte value)
      (let [pos (cond-> pos
                  new-key? (-> inc Math/abs))
            new-keys (aclone keys)
            new-nodes (aclone nodes)]
        (when new-key?
          (make-gap size pos keys new-keys)
          (make-gap size pos nodes new-nodes))
        (make-node (cond-> size
                     new-key? inc)
                   (doto new-keys (aset pos (byte key-byte)))
                   (doto new-nodes (aset pos value)))))))

(declare empty-node16 empty-node48 empty-node256)

(defrecord Node4 [^long size ^bytes keys ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (lookup-helper size keys nodes key-byte))

  (insert [this key-byte value]
    (insert-helper size keys nodes key-byte value ->Node4 empty-node16)))

(defrecord Node16 [^long size ^bytes keys ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (lookup-helper size keys nodes key-byte))

  (insert [this key-byte value]
    (insert-helper size keys nodes key-byte value ->Node16 empty-node48)))

(defrecord Node48 [^long size ^bytes key-index ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (let [key-int (Byte/toUnsignedInt key-byte)
          pos (aget key-index key-int)]
      (when-not (neg? pos)
        (aget nodes pos))))

  (insert [this key-byte value]
    (let [key-int (Byte/toUnsignedInt key-byte)
          pos (aget key-index key-int)
          new-key? (neg? pos)
          pos (if new-key?
                (byte size)
                pos)]
      (if (< pos (count nodes))
        (->Node48 (cond-> size
                    new-key? inc)
                  (doto (aclone key-index) (aset key-int pos))
                  (doto (aclone nodes) (aset pos value)))
        (loop [key 0
               node empty-node256]
          (if (< key (count key-index))
            (let [pos (aget key-index key)]
              (recur (inc key) (cond-> node
                                 (not (neg? pos)) (insert key (aget nodes pos)))))
            (insert node key-byte value)))))))

(defrecord Node256 [^long size ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (let [key-int (Byte/toUnsignedInt key-byte)]
      (aget nodes key-int)))

  (insert [this key-byte value]
    (let [key-int (Byte/toUnsignedInt key-byte)]
      (->Node256 (cond-> size
                   (nil? (aget nodes key-int)) inc)
                 (doto (aclone nodes) (aset key-int value))))))

(def empty-node4 (->Node4 0 (byte-array 4) (object-array 4)))

(def empty-node16 (->Node16 0 (byte-array 16) (object-array 16)))

(def empty-node48 (->Node48 0 (byte-array 256 (byte -1)) (object-array 48)))

(def empty-node256 (->Node256 0 (object-array 256)))

(defrecord Leaf [^bytes key value])

(defn leaf-matches-key? [^Leaf leaf ^bytes key-bytes]
  (Arrays/equals key-bytes (bytes (.key leaf))))

;; Path compression will change how this works.
;; Will just add the common prefix without in-between nodes.
(defn leaf-insert-helper [^Leaf leaf depth ^bytes key-bytes value]
  ((fn step [^long depth]
     (let [new-key-byte (aget key-bytes depth)
           old-key-byte (aget (bytes (.key leaf)) depth)]
       (if (= new-key-byte old-key-byte)
         (insert empty-node4 new-key-byte (step (inc depth)))
         (-> empty-node4
             (insert new-key-byte (->Leaf key-bytes value))
             (insert old-key-byte leaf))))) depth))

(defn leaf? [node]
  (instance? Leaf node))

(defn key-bytes-to-str [^bytes key]
  (String. key 0 (dec (count key)) "UTF-8"))

(defn key-bytes-to-long [^bytes key]
  (.getLong (ByteBuffer/wrap key)))

(extend-protocol ARTKey
  (class (byte-array 0))
  (to-key-bytes [this]
    this)

  ;; Strings needs to be 0 terminated, see IV. CONSTRUCTING BINARY-COMPARABLE KEYS
  ;; Should work for UTF-8, all non ASCII bytes have the highest bit set.
  ;; http://stackoverflow.com/a/6907327
  String
  (to-key-bytes [this]
    (let [bytes (.getBytes this "UTF-8") ]
      (Arrays/copyOf bytes (inc (count bytes)))))

  Long
  (to-key-bytes [this]
    (-> (ByteBuffer/allocate Long/BYTES)
        (.putLong this)
        .array))

  Date
  (to-key-bytes [this]
    (to-key-bytes (str (.toInstant this))))

  Object
  (to-key-bytes [this]
    (to-key-bytes (str this))))

;;; Public API

(defn art-make-tree []
  empty-node4)

(defn art-lookup [tree key]
  (let [key-bytes (bytes (to-key-bytes key))]
    (loop [depth 0
           node tree]
      (if (leaf? node)
        (when (leaf-matches-key? node key-bytes)
          (.value ^Leaf node))
        (when (and node (< depth (count key-bytes)))
          ;; Need to check that prefix matches before recur.
          (recur (inc depth) (lookup node (aget key-bytes depth))))))))

(defn art-insert
  ([tree key]
   (art-insert tree key key))
  ([tree key value]
   (let [key-bytes (bytes (to-key-bytes key))]
     ((fn step [^long depth node]
        (let [key-byte (aget key-bytes depth)
              child (lookup node key-byte)
              new-child (if (and child (not (leaf? child)))
                          ;; Needs to take prefix into account and
                          ;; potentially split and introduce
                          ;; intermediate parent.
                          (step (inc depth) child)
                          (if (or (nil? child) (leaf-matches-key? child key-bytes))
                            (->Leaf key-bytes value)
                            (leaf-insert-helper child (inc depth) key-bytes value)))]
          (insert node key-byte new-child)))
      0 (or tree (art-make-tree))))))

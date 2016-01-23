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

;; NOTE: Not suitable for Datascript indexes as they use Datoms as keys.

(defprotocol ARTNode
  (lookup [this key-byte])
  (insert [this key-byte value])
  (^bytes prefix [this]))

(defprotocol ARTBaseNode
  (key-position [this key-byte]))

(defprotocol ARTKey
  (^bytes to-key-bytes [this]))

;; Inspired by the SIMD version in the paper.
;; See https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
;; This version uses 9 bit 'bytes' to support full bytes, 0-255.
;; TODO: doesn't seem to be faster in practice?
;;       wire up in insert as well.

(defn key-position-binary-search ^long [^long size ^bytes keys ^long key-byte]
  (Arrays/binarySearch keys 0 size (byte key-byte)))

(defn make-gap [^long size ^long pos src dest]
  (System/arraycopy src pos dest (inc pos) (- size pos)))

(defn grow-helper [^long size ^bytes keys ^objects nodes node]
  (loop [idx 0
         node node]
    (if (< idx size)
      (recur (inc idx) (insert node (aget keys idx) (aget nodes idx)))
      node)))

(defn insert-helper [{:keys [^long size ^bytes keys ^objects nodes prefix] :as node}
                     key-byte value make-node empty-larger-node]
  (let [pos (long (key-position node key-byte))
        new-key? (neg? pos)]
    (if (and new-key? (= size (count keys)))
      (insert (grow-helper size keys nodes (assoc empty-larger-node :prefix prefix)) key-byte value)
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
                   (doto new-nodes (aset pos value))
                   prefix)))))

(declare empty-node16 empty-node48 empty-node256)

(defrecord Node4 [^long size ^bytes keys ^objects nodes ^bytes prefix]
  ARTNode
  (lookup [this key-byte]
    (let [pos (long (key-position this key-byte))]
      (when-not (neg? pos)
        (aget nodes pos))))

  (insert [this key-byte value]
    (insert-helper this key-byte value ->Node4 empty-node16))

  (prefix [this]
    prefix)

  ARTBaseNode
  (key-position [this key-byte]
    (let [x (bit-or (bit-shift-left (Byte/toUnsignedLong (aget keys 0)) 27)
                    (bit-shift-left (Byte/toUnsignedLong (aget keys 1)) 18)
                    (bit-shift-left (Byte/toUnsignedLong (aget keys 2)) 9)
                    (Byte/toUnsignedLong (aget keys 3)))
          ones 2r000000001000000001000000001000000001
          two-five-sixths 2r100000000100000000100000000100000000
          idx (Long/bitCount (bit-and (unchecked-subtract x (unchecked-multiply ones (Byte/toUnsignedLong key-byte)))
                                      (bit-and (bit-not x) two-five-sixths)))]
      (if (and (< idx size) (= key-byte (aget keys idx)))
        idx
        (unchecked-subtract -1 (min idx size))))))

(defrecord Node16 [^long size ^bytes keys ^objects nodes ^bytes prefix]
  ARTNode
  (lookup [this key-byte]
    (let [pos (long (key-position this key-byte))]
      (when-not (neg? pos)
        (aget nodes pos))))

  (insert [this key-byte value]
    (insert-helper this key-byte value ->Node16 empty-node48))

  (prefix [this]
    prefix)

  ARTBaseNode
  (key-position [this key-byte]
    (let [x1 (bit-or (bit-shift-left (Byte/toUnsignedLong (aget keys 0)) 54)
                     (bit-shift-left (Byte/toUnsignedLong (aget keys 1)) 45)
                     (bit-shift-left (Byte/toUnsignedLong (aget keys 2)) 36)
                     (bit-shift-left (Byte/toUnsignedLong (aget keys 3)) 27)
                     (bit-shift-left (Byte/toUnsignedLong (aget keys 4)) 18)
                     (bit-shift-left (Byte/toUnsignedLong (aget keys 5)) 9)
                     (Byte/toUnsignedLong (aget keys 6)))
          x2 (bit-or (bit-shift-left (Byte/toUnsignedLong (aget keys 7)) 54)
                     (bit-shift-left (Byte/toUnsignedLong (aget keys 8)) 45)
                     (bit-shift-left (Byte/toUnsignedLong (aget keys 9)) 36)
                     (bit-shift-left (Byte/toUnsignedLong (aget keys 10)) 27)
                     (bit-shift-left (Byte/toUnsignedLong (aget keys 11)) 18)
                     (bit-shift-left (Byte/toUnsignedLong (aget keys 12)) 9)
                     (Byte/toUnsignedLong (aget keys 13)))
          x3 (bit-or (bit-shift-left (Byte/toUnsignedLong (aget keys 14)) 9)
                     (Byte/toUnsignedLong (aget keys 15)))
          key-byte (Byte/toUnsignedLong key-byte)
          ones 2r000000001000000001000000001000000001000000001000000001000000001
          two-five-sixths 2r100000000100000000100000000100000000100000000100000000100000000
          idx (+ (Long/bitCount (bit-and (unchecked-subtract x1 (unchecked-multiply ones key-byte))
                                         (bit-and (bit-not x1) two-five-sixths)))
                 (Long/bitCount (bit-and (unchecked-subtract x2 (unchecked-multiply ones key-byte))
                                         (bit-and (bit-not x2) two-five-sixths)))
                 (Long/bitCount (bit-and (unchecked-subtract x3 (unchecked-multiply 2r000000001000000001 key-byte))
                                         (bit-and (bit-not x3) 2r100000000100000000))))]
      (if (and (< idx size) (= key-byte (aget keys idx)))
        idx
        (unchecked-subtract -1 (min idx size))))))

(defrecord Node48 [^long size ^bytes key-index ^objects nodes ^bytes prefix]
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
                  (doto (aclone nodes) (aset pos value))
                  prefix)
        (loop [key 0
               node (assoc empty-node256 :prefix prefix)]
          (if (< key (count key-index))
            (let [pos (aget key-index key)]
              (recur (inc key) (cond-> node
                                 (not (neg? pos)) (insert key (aget nodes pos)))))
            (insert node key-byte value))))))

  (prefix [this]
    prefix))

(defrecord Node256 [^long size ^objects nodes ^bytes prefix]
  ARTNode
  (lookup [this key-byte]
    (let [key-int (Byte/toUnsignedInt key-byte)]
      (aget nodes key-int)))

  (insert [this key-byte value]
    (let [key-int (Byte/toUnsignedInt key-byte)]
      (->Node256 (cond-> size
                   (nil? (aget nodes key-int)) inc)
                 (doto (aclone nodes) (aset key-int value))
                 prefix)))

  (prefix [this]
    prefix))

(def empty-node4 (->Node4 0 (byte-array 4 (byte -1)) (object-array 4) (byte-array 0)))

(def empty-node16 (->Node16 0 (byte-array 16 (byte -1)) (object-array 16) (byte-array 0)))

(def empty-node48 (->Node48 0 (byte-array 256 (byte -1)) (object-array 48) (byte-array 0)))

(def empty-node256 (->Node256 0 (object-array 256) (byte-array 0)))

(defrecord Leaf [^bytes key value])

(defn leaf-matches-key? [^Leaf leaf ^bytes key-bytes]
  (Arrays/equals key-bytes (bytes (.key leaf))))

(defn leaf-insert-helper [^Leaf leaf depth ^bytes key-bytes value]
  (let [prefix-start (long depth)]
    (loop [depth prefix-start]
      (let [new-key-byte (aget key-bytes depth)
            old-key-byte (aget (bytes (.key leaf)) depth)]
        (if (= new-key-byte old-key-byte)
          (recur (inc depth))
          (-> empty-node4
              (insert new-key-byte (->Leaf key-bytes value))
              (insert old-key-byte leaf)
              (assoc :prefix (Arrays/copyOfRange key-bytes prefix-start depth))))))))

(defn leaf? [node]
  (instance? Leaf node))

(defn common-prefix-length ^long [^bytes key-bytes ^bytes prefix ^long depth]
  (loop [idx 0]
    (if (and (< idx (count prefix))
             (= (aget key-bytes (+ depth idx)) (aget prefix idx)))
      (recur (inc idx))
      idx)))

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
  (let [key-bytes (to-key-bytes key)]
    (loop [depth 0
           node tree]
      (if (leaf? node)
        (when (leaf-matches-key? node key-bytes)
          (.value ^Leaf node))
        (when node
          (let [prefix (prefix node)
                common-prefix-length (common-prefix-length key-bytes prefix depth)
                depth (+ depth common-prefix-length)]
            (when (= common-prefix-length (count prefix))
              (recur (inc depth) (lookup node (aget key-bytes depth))))))))))

(defn art-insert
  ([tree key]
   (art-insert tree key key))
  ([tree key value]
   (let [key-bytes (to-key-bytes key)]
     ((fn step [^long depth node]
        (let [prefix (prefix node)
              common-prefix-length (common-prefix-length key-bytes prefix depth)
              depth (+ depth common-prefix-length)]
          (if (= common-prefix-length (count prefix))
            (let [key-byte (aget key-bytes depth)
                  child (lookup node key-byte)
                  new-child (if (and child (not (leaf? child)))
                              (step (inc depth) child)
                              (if (or (nil? child) (leaf-matches-key? child key-bytes))
                                (->Leaf key-bytes value)
                                (leaf-insert-helper child (inc depth) key-bytes value)))]
              (insert node key-byte new-child))
            (-> empty-node4
                (assoc :prefix (Arrays/copyOfRange prefix 0 common-prefix-length))
                (insert (aget key-bytes depth) (->Leaf key-bytes value))
                (insert (aget prefix common-prefix-length)
                        (assoc node :prefix (Arrays/copyOfRange prefix (inc common-prefix-length) (count prefix))))))))
      0 (or tree (art-make-tree))))))

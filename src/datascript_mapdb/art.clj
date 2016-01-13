(ns datascript-mapdb.art
  (:import [java.util Arrays]
           [java.nio ByteBuffer ByteOrder]))

;;; Persistent Adaptive Radix Tree
;; see http://www3.informatik.tu-muenchen.de/~leis/papers/ART.pdf

(defprotocol ARTNode
  (lookup [this key-byte])
  (insert [this key-byte value]))

(defprotocol ARTKey
  (to-key-bytes [this]))

(defn key-position ^long [^long size ^"[B" keys ^long key-byte]
  (Arrays/binarySearch keys 0 size (byte key-byte)))

(defn lookup-helper [^long size keys ^objects nodes ^long key-byte]
  (let [pos (key-position size keys key-byte)]
    (when-not (neg? pos)
      (aget nodes pos))))

(defn insert-into-node-helper [size pos ^"[B" keys ^objects nodes key-byte value make-node]
  (let [size (long size)
        pos (long pos)
        new-keys (aclone keys)
        new-nodes (aclone nodes)]
    (aset new-keys pos (byte key-byte))
    (aset new-nodes pos value)
    (System/arraycopy keys pos new-keys (inc pos) (- size pos))
    (System/arraycopy nodes pos new-nodes (inc pos) (- size pos))
    (make-node (inc size) new-keys new-nodes)))

(defn grow-helper [^long size ^"[B" keys ^objects nodes node]
  (loop [idx 0
         node node]
    (if (< idx size)
      (recur (inc idx) (insert node (aget keys idx) (aget nodes idx)))
      node)))

(defn insert-helper [size ^"[B" keys ^objects nodes key-byte value make-node empty-larger-node]
  (let [size (long size)
        pos (key-position size keys key-byte)]
    (if (neg? pos)
      (if (< size (count keys))
        (insert-into-node-helper size (-> pos long inc Math/abs) keys nodes key-byte value make-node)
        (insert (grow-helper size keys nodes empty-larger-node) key-byte value))
      (make-node size
                 (doto (aclone keys) (aset pos (byte key-byte)))
                 (doto (aclone nodes) (aset pos value))))))

(declare empty-node16 empty-node48 empty-node256)

;; Path compression should happen here when size is one.
(defrecord Node4 [^long size ^"[B" keys ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (lookup-helper size keys nodes key-byte))

  (insert [this key-byte value]
    (insert-helper size keys nodes key-byte value ->Node4 empty-node16)))

(defrecord Node16 [^long size ^"[B" keys ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (lookup-helper size keys nodes key-byte))

  (insert [this key-byte value]
    (insert-helper size keys nodes key-byte value ->Node16 empty-node48)))

(defrecord Node48 [^long size ^"[B" key-index ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (let [pos (aget key-index (byte key-byte))]
      (when-not (neg? pos)
        (aget nodes pos))))

  (insert [this key-byte value]
    (let [key-int (Byte/toUnsignedInt key-byte)
          idx (aget key-index key-int)
          capacity (count nodes)
          new-key? (neg? idx)
          idx (byte (if new-key?
                      size
                      idx))]
      (if (< idx capacity)
        (->Node48 (cond-> idx
                    new-key? inc)
                  (doto (aclone key-index) (aset key-int idx))
                  (doto (aclone nodes) (aset idx value)))
        (loop [key 0
               node empty-node256]
          (if (< key (count key-index))
            (let [idx (aget key-index key)]
              (recur (inc key) (cond-> node
                                 (< idx capacity) (insert key (aget nodes idx)))))
            (insert node key-byte value)))))))

(defrecord Node256 [^long size ^objects nodes]
  ARTNode
  (lookup [this key-byte]
    (aget nodes key-byte))

  (insert [this key-byte value]
    (let [key-int (Byte/toUnsignedInt key-byte)]
      (->Node256 (cond-> size
                   (nil? (aget nodes key-int)) inc)
                 (doto (aclone nodes) (aset key-int value))))))

(def empty-node4 (->Node4 0 (byte-array 4) (object-array 4)))

(def empty-node16 (->Node16 0 (byte-array 16) (object-array 16)))

(def empty-node48 (->Node48 0 (byte-array 256 (byte -1)) (object-array 48)))

(def empty-node256 (->Node256 0 (object-array 256)))

(defrecord Leaf [^"[B" key value])

(defn leaf-matches-key? [^Leaf leaf ^"[B" key-bytes]
  (Arrays/equals key-bytes (bytes (.key leaf))))

;; Path compression will change how this works.
(defn leaf-insert-helper [^Leaf leaf idx ^"[B" key-bytes value]
  ((fn step [^long idx]
     (let [new-key-byte (aget key-bytes idx)
           old-key-byte (aget (bytes (.key leaf)) idx)]
       (if (= new-key-byte old-key-byte)
         (insert empty-node4 new-key-byte (step (inc idx)))
         (-> empty-node4
             (insert new-key-byte (->Leaf key-bytes value))
             (insert old-key-byte leaf))))) idx))

(extend-protocol ARTKey
  String
  ;; Strings needs to be 0 terminated, see IV. CONSTRUCTING BINARY-COMPARABLE KEYS
  ;; TODO: this probably breaks down for wider chars.
  (to-key-bytes [this]
    (let [bytes (.getBytes this "UTF-8") ]
      (Arrays/copyOf bytes (inc (count bytes)))))

  Long
  (to-key-bytes [this]
    (-> (ByteBuffer/allocate Long/BYTES)
        (.putLong this)
        .array))

  Object
  (to-key-bytes [this]
    this))

;;; Public API

(defn art-make-tree []
  empty-node4)

(defn art-lookup [tree key]
  (let [key-bytes (bytes (to-key-bytes key))]
    (loop [idx 0
           node tree]
      (if (instance? Leaf node)
        (let [^Leaf leaf node]
          (when (leaf-matches-key? leaf key-bytes)
            (.value leaf)))
        (when (and node (< idx (count key-bytes)))
          (recur (inc idx) (lookup node (aget key-bytes idx))))))))

(defn art-insert [tree key value]
  (let [key-bytes (bytes (to-key-bytes key))]
    ((fn step [^long idx node]
       (insert
        node
        (aget key-bytes idx)
        (let [child (lookup node (aget key-bytes idx))]
          (if (and (satisfies? ARTNode child)
                   (< (inc idx) (count key-bytes)))
            (step (inc idx) child)
            (if (or (nil? child) (leaf-matches-key? child key-bytes))
              (->Leaf key-bytes value)
              (leaf-insert-helper child (inc idx) key-bytes value))))))
     0 (or tree (art-make-tree)))))

(comment
  (-> (art-make-tree)
      (art-insert "foo" "boo")
      (art-insert "bar" "boz")
      (art-lookup "bar"))

  (-> (art-make-tree)
      (art-insert 42 "boo")
      (art-insert 64 "baz")
      (art-insert 63 "baz")
      (art-lookup 63)))

(ns datascript-mapdb.art
  (:import [java.util Arrays]
           [java.nio ByteBuffer ByteOrder]))

;;; Persistent Adaptive Radix Tree
;; see http://www3.informatik.tu-muenchen.de/~leis/papers/ART.pdf

(defprotocol ARTNode
  (lookup [this key-byte])
  (insert [this key-byte value]))

(defn key-position ^long [^long size ^"[B" keys ^long key-byte]
  (Arrays/binarySearch keys 0 size (byte key-byte)))

(defn lookup-helper [^long size keys ^objects nodes ^long key-byte]
  (let [pos (key-position size keys key-byte)]
    (when-not (neg? pos)
      (aget nodes pos))))

(defn grow-helper [^long size ^"[B" keys ^objects nodes node]
  (loop [idx 0
         node node]
    (if (< idx size)
      (recur (inc idx) (insert node (aget keys idx) (aget nodes idx)))
      node)))

(defn insert-helper [size ^"[B" keys ^objects nodes key-byte value make-node empty-larger-node]
  (let [pos (key-position size keys key-byte)
        new-keys (aclone keys)
        new-nodes (aclone nodes)
        capacity (count keys)]
    (if (neg? pos)
      (if (< (long size) capacity)
        (let [pos (-> pos inc Math/abs)]
          (aset new-keys pos (byte key-byte))
          (aset new-nodes pos value)
          (loop [idx (inc pos)]
            (when (< idx capacity)
              (aset new-keys idx (aget keys (dec idx)))
              (aset new-nodes idx (aget nodes (dec idx)))
              (recur (inc idx))))
          (make-node (inc (long size)) new-keys new-nodes))
        (insert (grow-helper size keys nodes empty-larger-node) key-byte value))
      (make-node size
                 (doto new-keys (aset pos (byte key-byte)))
                 (doto new-nodes (aset pos value))))))

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
      (when (< pos (count nodes))
        (aget nodes pos))))

  (insert [this key-byte value]
    (let [idx (aget key-index key-byte)
          capacity (count nodes)
          new-key? (= capacity idx)
          idx (byte (if new-key?
                      size
                      idx))]
      (if (< idx capacity)
        (->Node48 (cond-> idx
                    new-key? inc)
                  (doto (aclone key-index) (aset (byte key-byte) idx))
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
    (->Node256 (cond-> size
                 (nil? (aget nodes key-byte)) inc)
               (doto (aclone nodes) (aset key-byte value)))))

(def empty-node4 (->Node4 0 (byte-array 4 (byte 4)) (object-array 4)))

(def empty-node16 (->Node16 0 (byte-array 16 (byte 16)) (object-array 16)))

(def empty-node48 (->Node48 0 (byte-array 256 (byte 48)) (object-array 48)))

(def empty-node256 (->Node256 0 (object-array 256)))

(defrecord Leaf [^"[B" key value])

(defn leaf-matches-key? [^Leaf leaf ^"[B" key-bytes]
  (Arrays/equals key-bytes (bytes (.key leaf))))

;; Path compression will change how this works.
(defn leaf-insert-helper [^Leaf leaf idx ^"[B" key-bytes value]
  ((fn step [^long idx]
     (let [node empty-node4
           new-key-byte (aget key-bytes idx)
           old-key-byte (aget (bytes (.key leaf)) idx)]
       (if (= new-key-byte old-key-byte)
          (insert node new-key-byte (step (inc idx)))
          (-> node
              (insert new-key-byte (->Leaf key-bytes value))
              (insert old-key-byte leaf))))) idx))

;;; Public API

(defn art-make-tree []
  empty-node4)

(defn art-lookup [tree ^"[B" key-bytes]
  (loop [idx 0
         node tree]
    (if (instance? Leaf node)
      (let [^Leaf leaf node]
        (when (leaf-matches-key? leaf key-bytes)
          (.value leaf)))
      (when (and node (< idx (count key-bytes)))
        (recur (inc idx) (lookup node (aget key-bytes idx)))))))

(defn art-insert [tree ^"[B" key-bytes value]
  (loop [idx 0
         node (or tree (art-make-tree))]
    (let [child (some-> node (lookup (aget key-bytes idx)))]
      (if (and (satisfies? ARTNode child)
               (< (inc idx) (count key-bytes)))
        (recur (inc idx) child)
        (insert node (aget key-bytes idx)
                (if (or (nil? child) (leaf-matches-key? child key-bytes))
                  (->Leaf key-bytes value)
                  (leaf-insert-helper child (inc idx) key-bytes value)))))))

;; Strings needs to be 0 terminated, see IV. CONSTRUCTING BINARY-COMPARABLE KEYS

(defn str-to-key-bytes ^bytes [^String s]
  (let [str-bytes (.getBytes s "UTF-8") ]
    (Arrays/copyOf str-bytes (inc (count str-bytes)))))

(defn art-lookup-str [tree ^String key]
  (art-lookup tree (str-to-key-bytes key)))

(defn art-insert-str [tree ^String key value]
  (art-insert tree (str-to-key-bytes key) value))

(defn long-to-key-bytes ^bytes [^long x]
  (-> (ByteBuffer/allocate Long/BYTES)
      (.putLong x)
      .array))

(defn art-lookup-long [tree ^long key]
  (art-lookup tree (long-to-key-bytes key)))

(defn art-insert-long [tree ^long key value]
  (art-insert tree (long-to-key-bytes key) value))

(comment
  (-> (art-make-tree)
      (art-insert-str "foo" "boo")
      (art-insert-str "bar" "baz")
      (art-lookup-str "foo"))

  (-> (art-make-tree)
      (art-insert-long 42 "boo")
      (art-insert-long 64 "baz")
      (art-lookup-long 64)))

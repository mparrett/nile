;; Blocks and heaps, as nile-heap.h: a heap is a list of blocks, and a chunk is
;; up to CHUNK_MAX_LEN blocks that point at one another through eoc. A chunk is
;; what workers move between heaps, so a heap's head is a chunk's first block.

(define (BLOCK_SIZE int) 512)
(define (CHUNK_MAX_LEN int) 16)

(define-struct nile_Block
  ((next struct nile_Block *)
   (eoc  struct nile_Block *)
   (i    int)))

;; Pushes block b on heap h, starting a new chunk when the head's is full. Returns
;; 1 when the push completed a chunk that the heap already had a successor for.
(define-function nile_Heap_push int ((h struct nile_Block * *) (b struct nile_Block *))
  (let (((head struct nile_Block *) (indir h)))
    (set (. (indir b) next) head)
    (if (and (!= 0 (cast head long)) (< (. (indir head) i) CHUNK_MAX_LEN))
	(let ()
	  (set (. (indir b) eoc) (. (indir head) eoc))
	  (set (. (indir b) i) (+ (. (indir head) i) 1)))
	(let ()
	  (set (. (indir b) eoc) b)
	  (set (. (indir b) i) 1)))
    (set (indir h) b)
    (if (and (= (. (indir b) i) CHUNK_MAX_LEN)
	     (!= 0 (cast (. (indir (. (indir b) eoc)) next) long)))
	(return 1)
	(return 0))))

;; Puts the chunk c, whose first block is c, on the front of heap h.
(define-function nile_Heap_push_chunk void ((h struct nile_Block * *) (c struct nile_Block *))
  (set (. (indir (. (indir c) eoc)) next) (indir h))
  (set (indir h) c))

;; Takes the head block off heap h and returns it, or 0 when the heap is empty.
(define-function nile_Heap_pop (struct nile_Block *) ((h struct nile_Block * *))
  (let (((b struct nile_Block *) (indir h)))
    (if (!= 0 (cast b long))
	(set (indir h) (. (indir b) next)))
    (return b)))

;; Takes the head chunk off heap h and returns its first block, or 0 when empty.
(define-function nile_Heap_pop_chunk (struct nile_Block *) ((h struct nile_Block * *))
  (let (((b struct nile_Block *) (indir h)))
    (if (!= 0 (cast b long))
	(set (indir h) (. (indir (. (indir b) eoc)) next)))
    (return b)))

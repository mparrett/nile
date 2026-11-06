;; Deques of nodes, as nile-deque.h. A node is anything whose first two fields
;; are a next link and a type; processes and buffers will both be nodes.

(define-struct nile_Node
  ((next struct nile_Node *)
   (type int)))

(define-struct nile_Deque
  ((head struct nile_Node *)
   (tail struct nile_Node *)
   (n    int)))

(define-function nile_Deque_push_head void ((d struct nile_Deque *) (nd struct nile_Node *))
  (set (. (indir nd) next) (. (indir d) head))
  (set (. (indir d) head) nd)
  (if (= 0 (cast (. (indir d) tail) long))
      (set (. (indir d) tail) nd))
  (set (. (indir d) n) (+ (. (indir d) n) 1)))

(define-function nile_Deque_push_tail void ((d struct nile_Deque *) (nd struct nile_Node *))
  (set (. (indir nd) next) 0)
  (if (!= 0 (cast (. (indir d) tail) long))
      (set (. (indir (. (indir d) tail)) next) nd)
      (set (. (indir d) head) nd))
  (set (. (indir d) tail) nd)
  (set (. (indir d) n) (+ (. (indir d) n) 1)))

(define-function nile_Deque_pop_head (struct nile_Node *) ((d struct nile_Deque *))
  (let (((nd struct nile_Node *) (. (indir d) head)))
    (if (!= 0 (cast nd long))
	(let ()
	  (set (. (indir d) head) (. (indir nd) next))
	  (if (= 0 (cast (. (indir d) head) long))
	      (set (. (indir d) tail) 0))
	  (set (. (indir d) n) (- (. (indir d) n) 1))))
    (return nd)))

(define-function nile_Deque_pop_tail (struct nile_Node *) ((d struct nile_Deque *))
  (let (((nd struct nile_Node *) (. (indir d) tail)))
    (if (!= 0 (cast nd long))
	(let ()
	  (if (= (cast (. (indir d) head) long) (cast nd long))
	      (let ()
		(set (. (indir d) head) 0)
		(set (. (indir d) tail) 0))
	      (let (((prev struct nile_Node *) (. (indir d) head)))
		(while (!= (cast (. (indir prev) next) long) (cast nd long))
		  (set prev (. (indir prev) next)))
		(set (. (indir prev) next) 0)
		(set (. (indir d) tail) prev)))
	  (set (. (indir d) n) (- (. (indir d) n) 1))))
    (return nd)))

;; The kernel interface, as nile.k declares it (RFC "The kernel interface"): what a
;; kernel calls to make a process, read its input and write its output. A work
;; function gets (p, in, i, m, out, j, n): the head input buffer's data with the
;; position i and end m, and the current output buffer's data with the position j
;; and limit n. It calls advance_input when i == m and advance_output when j == n,
;; and ends with return, which saves both positions.

;; Floats in a buffer: the block after its header, with the whole elements of
;; the process's output quantum rounded down (nile_Process_limit).
(define-function nile_Buffer_capacity int ()
  (return (cast (/ (- (cast BLOCK_BYTES long) (cast (sizeof struct nile_Buffer) long)) (cast 4 long))
		int)))

(define-function nile_Buffer_data (float *) ((b struct nile_Buffer *))
  (return (cast (+ (cast b long) (cast (sizeof struct nile_Buffer) long)) float *)))

;; A fresh empty buffer from p's heap; 0 when none is free.
(define-function nile_Buffer (struct nile_Buffer *) ((p struct nile_Process *))
  (let (((nd struct nile_Node *) (nile_Process_alloc_node p)))
    (if (= 0 (cast nd long))
	(return (cast 0 struct nile_Buffer *)))
    (let (((b struct nile_Buffer *) (cast nd struct nile_Buffer *)))
      (set (. (indir b) next) (cast 0 struct nile_Node *))
      (set (. (indir b) type) NILE_BUFFER_TYPE)
      (set (. (indir b) head) 0)
      (set (. (indir b) tail) 0)
      (set (. (indir b) capacity) (nile_Buffer_capacity))
      (return b))))

;; The variable record: the rest of p's block after its record, lock and deque.
(define-function nile_Process_memory (void *) ((p struct nile_Process *))
  (return (cast (+ (cast p long) (nile_Process_vars_offset)) void *)))

;; A process with the given quanta, variable record and work functions, as a
;; child of parent; 0 when the parent's heap is out of blocks, or when a quantum,
;; the variable record or the block can't hold what it asks for (BAD_ARG).
(define-function nile_Process (struct nile_Process *)
  ((parent struct nile_Process *) (in_quantum int) (out_quantum int) (sizeof_vars int)
   (prologue nile_Process_work_function_t)
   (body nile_Process_work_function_t)
   (epilogue nile_Process_work_function_t))
  (if (= 0 (cast parent long))
      (return (cast 0 struct nile_Process *)))
  (if (or (< in_quantum 1) (< out_quantum 1)
	  (> in_quantum (nile_Buffer_capacity)) (> out_quantum (nile_Buffer_capacity))
	  (> (cast sizeof_vars long) (- BLOCK_BYTES (nile_Process_vars_offset))))
      (let ()
	(nile_Process_fail parent NILE_STATUS_BAD_ARG)
	(return (cast 0 struct nile_Process *))))
  (let (((p struct nile_Process *) (cast (nile_Process_alloc_node parent) struct nile_Process *)))
    (if (!= 0 (cast p long))
	(nile_Process_setup p parent in_quantum out_quantum sizeof_vars prologue body epilogue))
    (return p)))

;; Moves the input to the next buffer, first handing back the used one. Returns 1
;; to stop the kernel: on an error or quota, with the used buffer kept so that
;; return can save the kernel's position in it; or when the input is used up.
(define-function nile_Process_advance_input int
  ((p struct nile_Process *) (in float * *) (i int *) (m int *))
  (if (!= 0 (nile_Process_stop_wanted p))
      (return 1))
  ;; i == m: the head buffer is used up, so its head meets its tail before it is dropped.
  (let (((h struct nile_Buffer *) (nile_Process_head p)))
    (if (!= 0 (cast h long))
	(set (. (indir h) head) (. (indir h) tail))))
  (nile_Process_drop_used p)
  (let (((b struct nile_Buffer *) (nile_Process_head p)))
    (if (= 0 (cast b long))
	(let ()
	  (nile_Process_set_state p NILE_NOT_BLOCKED)
	  (return 1)))
    (set (indir in) (nile_Buffer_data b))
    (set (indir i) (. (indir b) head))
    (set (indir m) (. (indir b) tail))
    (return 0)))

;; The output buffer is full: hands it on and moves to a fresh one. With no block
;; free the output goes to a scratch float that the runtime discards, and the
;; next advance_input stops the kernel. Never fails, as the kernel calls it
;; mid-statement.
(define-function nile_Process_advance_output void
  ((p struct nile_Process *) (out float * *) (j int *) (n int *))
  (let (((cur struct nile_Buffer *) (. (indir p) out)))
    (if (= 0 (cast cur long))
	(let ()
	  (set (indir out) (addressof (. (indir p) scratch)))
	  (set (indir j) 0)
	  (set (indir n) 1))
	(let (((nb struct nile_Buffer *) (cast 0 struct nile_Buffer *)))
	  (set (. (indir cur) tail) (indir j))
	  (set (. (indir p) out) (cast 0 struct nile_Buffer *))
	  (nile_Process_enqueue_output p cur)
	  (set nb (nile_Buffer p))
	  (if (= 0 (cast nb long))
	      (let ()
		(set (indir out) (addressof (. (indir p) scratch)))
		(set (indir j) 0)
		(set (indir n) 1))
	      (let ()
		(set (. (indir p) out) nb)
		(set (indir out) (nile_Buffer_data nb))
		(set (indir j) 0)
		(set (indir n) (nile_Process_limit p))))))))

;; The kernel's last call: saves its position in the input and output buffers
;; and returns the thread's status, which the runtime acts on after the call.
(define-function nile_Process_return int
  ((p struct nile_Process *) (i int) (j int) (status int))
  (let (((head struct nile_Buffer *) (nile_Process_head p)))
    (if (!= 0 (cast head long))
	(set (. (indir head) head) i))
    (if (!= 0 (cast (. (indir p) out) long))
	(set (. (indir (. (indir p) out)) tail) j))
    (return (nile_Thread_status_of (. (indir p) thread)))))

;; Pushes back at a buffer's start (Nile's <<): a new empty buffer goes in front of the
;; current one, and in, i and m are set to its end, so the kernel writes downward from
;; there. With no block free the kernel gets the scratch float (one slot, discarded), and
;; the next advance_input stops it on the error the failed allocation has set.
(define-function nile_Process_prefix_input void
  ((p struct nile_Process *) (in float * *) (i int *) (m int *))
  (let (((b struct nile_Buffer *) (nile_Buffer p)))
    (if (= 0 (cast b long))
	(let ()
	  (set (indir in) (addressof (. (indir p) scratch)))
	  (set (indir i) 1)
	  (set (indir m) 1))
	(let ()
	  (set (. (indir b) head) (. (indir b) capacity))
	  (set (. (indir b) tail) (. (indir b) capacity))
	  (nile_lock (. (indir p) lock))
	  (nile_Deque_push_head (. (indir p) input) (cast b struct nile_Node *))
	  (nile_unlock (. (indir p) lock))
	  (set (indir in) (nile_Buffer_data b))
	  (set (indir i) (. (indir b) capacity))
	  (set (indir m) (. (indir b) capacity))))))

;; Saves the kernel's positions, i in the input buffer and j in the output, then places the
;; pipeline sub between p and its consumer (nile_Process_swap). Returns the thread's status.
(define-function nile_Process_reroute int
  ((p struct nile_Process *) (i int) (j int) (sub struct nile_Process *))
  (let (((head struct nile_Buffer *) (nile_Process_head p)))
    (if (!= 0 (cast head long))
	(set (. (indir head) head) i))
    (if (!= 0 (cast (. (indir p) out) long))
	(set (. (indir (. (indir p) out)) tail) j))
    (return (nile_Process_swap p sub))))

(export nile_Process nile_Process_memory nile_Process_advance_output
	nile_Process_advance_input nile_Process_prefix_input nile_Process_return
	nile_Process_reroute)

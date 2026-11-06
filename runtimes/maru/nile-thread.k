;; The worker pool, as nile.c and nile-thread.h. Each worker has one mutex and
;; one condition variable, where the C runtime has a spin lock and pauses. The
;; liaison is the caller's thread, threads[nthreads]; threads[0] runs on it
;; during nile_sync, and threads[1 .. nthreads-1] are OS threads.
;;
;; Typed Maru can't embed a struct by value, so every record the pool needs
;; sits in the memory it was given, and the thread record points at its own.
;; Processes are Task 7's: until then no process is queued, so a worker only
;; waits. The root process is one record of its own, a block that points at
;; the liaison; Task 7 gives nile_Process its full record.

(load "nile.k")

(define-struct nile_Thread
  ((lock         struct nile_Mutex *)
   (wake         struct nile_Cond *)
   (q            struct nile_Deque *)
   (osthread     struct nile_OSThread *)
   (private_heap struct nile_Block *)
   (public_heap  struct nile_Block *)
   (index        int)
   (threads      struct nile_Thread *)
   (nthreads     int)
   (memory       char *)
   (nbytes       long)
   (ngated       int)
   (status       int)))

(define-struct nile_Root
  ((next    struct nile_Node *)
   (type    int)
   (thread  struct nile_Thread *)
   (nblocks int)))

(define (dprintf (int int char * ...) *) (extern dprintf))

;; BLOCK_SIZE as a long, for address arithmetic (typed Maru won't mix int and long).
(define (BLOCK_BYTES long) 512)

;; The i-th record of size bytes from base.
(define-function nile_offset long ((base long) (size long) (i int))
  (return (+ base (* size (cast i long)))))

(define-function nile_Thread_at (struct nile_Thread *) ((threads struct nile_Thread *) (i int))
  (return (cast (nile_offset (cast threads long) (cast (sizeof struct nile_Thread) long) i)
		struct nile_Thread *)))

(define-function nile_Thread_init void
  ((t struct nile_Thread *) (index int) (threads struct nile_Thread *) (nthreads int)
   (memory char *) (nbytes long)
   (lock struct nile_Mutex *) (wake struct nile_Cond *)
   (q struct nile_Deque *) (osthread struct nile_OSThread *))
  (pthread_mutex_init lock (cast 0 void *))
  (pthread_cond_init wake (cast 0 void *))
  (set (. (indir q) head) (cast 0 struct nile_Node *))
  (set (. (indir q) tail) (cast 0 struct nile_Node *))
  (set (. (indir q) n) 0)
  (set (. (indir t) lock) lock)
  (set (. (indir t) wake) wake)
  (set (. (indir t) q) q)
  (set (. (indir t) osthread) osthread)
  (set (. (indir t) private_heap) (cast 0 struct nile_Block *))
  (set (. (indir t) public_heap) (cast 0 struct nile_Block *))
  (set (. (indir t) index) index)
  (set (. (indir t) threads) threads)
  (set (. (indir t) nthreads) nthreads)
  (set (. (indir t) memory) memory)
  (set (. (indir t) nbytes) nbytes)
  (set (. (indir t) ngated) 0)
  (set (. (indir t) status) NILE_STATUS_OK))

;; The thread a process is active on: the liaison, for the root.
(define-function nile_Process_thread (struct nile_Thread *) ((p struct nile_Process *))
  (return (. (indir (cast p struct nile_Root *)) thread)))

;; The worker's thread body. Task 7 runs queued processes here; until then
;; it waits on its condition variable until it has work or is shut down.
(define-function nile_Thread_main (void *) ((arg void *))
  (let (((t struct nile_Thread *) (cast arg struct nile_Thread *)))
    (nile_lock (. (indir t) lock))
    (while (and (= (. (indir t) status) NILE_STATUS_OK)
		(= 0 (. (indir (. (indir t) q)) n)))
      (nile_wait (. (indir t) wake) (. (indir t) lock)))
    (nile_unlock (. (indir t) lock))
    (return (cast 0 void *))))

;; Takes the head chunk off t's public heap under t's lock; 0 when it has none.
(define-function nile_Thread_pop_public_chunk (struct nile_Block *) ((t struct nile_Thread *))
  (nile_lock (. (indir t) lock))
  (let (((c struct nile_Block *) (nile_Heap_pop_chunk (addressof (. (indir t) public_heap)))))
    (nile_unlock (. (indir t) lock))
    (return c)))

;; The i-th victim of t's steal order, as nile.c's nile_Thread_steal: victims
;; alternate above and below t's index, with the liaison last (i = nthreads).
(define-function nile_Thread_victim (struct nile_Thread *) ((t struct nile_Thread *) (i int))
  (let ((j (+ (. (indir t) index) (. (indir t) nthreads)))
	(k 1))
    (while (<= k i)
      (if (= (% k 2) (% (. (indir t) index) 2))
	  (set j (- j k))
	  (set j (+ j k)))
      (set k (+ k 1)))
    (return (nile_Thread_at (. (indir t) threads)
			    (% (+ (% j (. (indir t) nthreads)) (. (indir t) nthreads))
			       (. (indir t) nthreads))))))

;; A chunk from another worker's public heap, or from the liaison's last.
(define-function nile_Thread_steal_chunk (struct nile_Block *) ((t struct nile_Thread *))
  (let (((c struct nile_Block *) (cast 0 struct nile_Block *))
	(i 1))
    (while (and (= 0 (cast c long)) (< i (. (indir t) nthreads)))
      (set c (nile_Thread_pop_public_chunk (nile_Thread_victim t i)))
      (set i (+ i 1)))
    (if (= 0 (cast c long))
	(set c (nile_Thread_pop_public_chunk
		(nile_Thread_at (. (indir t) threads) (. (indir t) nthreads)))))
    (return c)))

;; A chunk for t's allocator: its own public heap first, then a steal. When
;; there is none, every worker is marked OUT_OF_MEMORY and woken.
(define-function nile_Thread_alloc_chunk (struct nile_Block *) ((t struct nile_Thread *))
  (let (((c struct nile_Block *) (nile_Thread_pop_public_chunk t)))
    (if (= 0 (cast c long))
	(set c (nile_Thread_steal_chunk t)))
    (if (= 0 (cast c long))
	(let ((i 0))
	  (while (<= i (. (indir t) nthreads))
	    (let (((o struct nile_Thread *) (nile_Thread_at (. (indir t) threads) i)))
	      (nile_lock (. (indir o) lock))
	      (set (. (indir o) status) NILE_STATUS_OUT_OF_MEMORY)
	      (nile_broadcast (. (indir o) wake))
	      (nile_unlock (. (indir o) lock)))
	    (set i (+ i 1)))))
    (return c)))

(define-function nile_Thread_free_chunk void ((t struct nile_Thread *) (c struct nile_Block *))
  (nile_lock (. (indir t) lock))
  (nile_Heap_push_chunk (addressof (. (indir t) public_heap)) c)
  (nile_unlock (. (indir t) lock)))

;; Starts a runtime in memory: the thread records, their locks, conditions,
;; queues and OS thread handles, then the root block, then blocks dealt
;; round-robin to the threads' public heaps. Returns 0 when memory can't hold
;; the records, the root block and a chunk per worker.
(define-function nile_startup (struct nile_Process *) ((memory char *) (nbytes int) (nthreads int))
  (let (((m long) (cast memory long))
	((start long) (* BLOCK_BYTES (/ (+ (cast memory long) (- BLOCK_BYTES 1)) BLOCK_BYTES)))
	((nt long) (cast (+ nthreads 1) long))
	((off_lock long) 0)
	((off_wake long) 0)
	((off_q long) 0)
	((off_os long) 0)
	((header long) 0)
	((root_at long) 0)
	((blocks_at long) 0))
    (set off_lock (* nt (cast (sizeof struct nile_Thread) long)))
    (set off_wake (+ off_lock (* nt (cast (sizeof struct nile_Mutex) long))))
    (set off_q (+ off_wake (* nt (cast (sizeof struct nile_Cond) long))))
    (set off_os (+ off_q (* nt (cast (sizeof struct nile_Deque) long))))
    (set header (+ off_os (* nt (cast (sizeof struct nile_OSThread) long))))
    (set root_at (+ start (* BLOCK_BYTES (/ (+ header (- BLOCK_BYTES 1)) BLOCK_BYTES))))
    (set blocks_at (+ root_at BLOCK_BYTES))
    (if (< (cast nbytes long)
	   (+ (- blocks_at m) (* (cast nthreads long) (* (cast CHUNK_MAX_LEN long) BLOCK_BYTES))))
	(return (cast 0 struct nile_Process *)))
    (let (((threads struct nile_Thread *) (cast start struct nile_Thread *))
	  ((root struct nile_Root *) (cast root_at struct nile_Root *))
	  ((end long) (+ m (cast nbytes long)))
	  ((block long) blocks_at)
	  ((nblocks int) 0)
	  (i 0))
      (while (<= i nthreads)
	(nile_Thread_init (nile_Thread_at threads i) i threads nthreads memory (cast nbytes long)
			  (cast (nile_offset (+ start off_lock) (cast (sizeof struct nile_Mutex) long) i)
				struct nile_Mutex *)
			  (cast (nile_offset (+ start off_wake) (cast (sizeof struct nile_Cond) long) i)
				struct nile_Cond *)
			  (cast (nile_offset (+ start off_q) (cast (sizeof struct nile_Deque) long) i)
				struct nile_Deque *)
			  (cast (nile_offset (+ start off_os) (cast (sizeof struct nile_OSThread) long) i)
				struct nile_OSThread *))
	(set i (+ i 1)))
      (set (. (indir root) next) (cast 0 struct nile_Node *))
      (set (. (indir root) type) 1)
      (set (. (indir root) thread) (nile_Thread_at threads nthreads))
      (set i 0)
      (while (<= (+ block BLOCK_BYTES) end)
	(nile_Heap_push (addressof (. (indir (nile_Thread_at threads (+ 1 (% i nthreads)))) public_heap))
			(cast block struct nile_Block *))
	(set block (+ block BLOCK_BYTES))
	(set nblocks (+ nblocks 1))
	(set i (+ i 1)))
      (set (. (indir root) nblocks) nblocks)
      (set i 1)
      (while (< i nthreads)
	(let (((t struct nile_Thread *) (nile_Thread_at threads i)))
	  (pthread_create (. (indir t) osthread) (cast 0 void *)
			  (cast nile_Thread_main void *) (cast t void *)))
	(set i (+ i 1)))
      (return (cast root struct nile_Process *)))))

;; Returns the status the liaison's thread holds.
(define-function nile_status int ((p struct nile_Process *))
  (let (((t struct nile_Thread *) (nile_Process_thread p))
	((s int) 0))
    (nile_lock (. (indir t) lock))
    (set s (. (indir t) status))
    (nile_unlock (. (indir t) lock))
    (return s)))

(define-function nile_error int ((p struct nile_Process *))
  (return (!= NILE_STATUS_OK (nile_status p))))

;; Waits until no pipeline is gated on the liaison. Nothing gates in Task 6,
;; so this returns at once; Task 7 signals the condition when a gate clears.
(define-function nile_sync void ((p struct nile_Process *))
  (let (((t struct nile_Thread *) (nile_Process_thread p)))
    (nile_lock (. (indir t) lock))
    (while (!= 0 (. (indir t) ngated))
      (nile_wait (. (indir t) wake) (. (indir t) lock)))
    (nile_unlock (. (indir t) lock))))

;; Stops every worker, wakes each one under its own lock, joins the OS threads,
;; and returns the memory the runtime was started in.
(define-function nile_shutdown (char *) ((p struct nile_Process *))
  (let (((t struct nile_Thread *) (nile_Process_thread p))
	((n int) (. (indir (nile_Process_thread p)) nthreads))
	(i 0))
    (while (<= i n)
      (let (((o struct nile_Thread *) (nile_Thread_at (. (indir t) threads) i)))
	(nile_lock (. (indir o) lock))
	(set (. (indir o) status) NILE_STATUS_SHUTTING_DOWN)
	(nile_broadcast (. (indir o) wake))
	(nile_unlock (. (indir o) lock)))
      (set i (+ i 1)))
    (set i 1)
    (while (< i n)
      (let (((o struct nile_Thread *) (nile_Thread_at (. (indir t) threads) i)))
	(pthread_join (. (indir (. (indir o) osthread)) w0) (cast 0 void *)))
      (set i (+ i 1)))
    (return (. (indir t) memory))))

;; The blocks on a heap: each chunk's head counts the blocks in its chunk.
(define-function nile_Heap_size int ((h struct nile_Block *))
  (let ((n 0)
	((b struct nile_Block *) h))
    (while (!= 0 (cast b long))
      (set n (+ n (. (indir b) i)))
      (set b (. (indir (. (indir b) eoc)) next)))
    (return n)))

;; Reports to stderr when the blocks in the heaps aren't all the blocks the
;; runtime carved, as nile.c's nile_print_leaks does.
(define-function nile_print_leaks void ((p struct nile_Process *))
  (let (((t struct nile_Thread *) (nile_Process_thread p))
	((nblocks int) (. (indir (cast p struct nile_Root *)) nblocks))
	((n int) 0)
	(i 0))
    (while (<= i (. (indir t) nthreads))
      (let (((o struct nile_Thread *) (nile_Thread_at (. (indir t) threads) i)))
	(nile_lock (. (indir o) lock))
	(set n (+ n (nile_Heap_size (. (indir o) private_heap))))
	(set n (+ n (nile_Heap_size (. (indir o) public_heap))))
	(nile_unlock (. (indir o) lock)))
      (set i (+ i 1)))
    (if (!= n nblocks)
	(dprintf 2 "# blocks not in a heap: %d\n" (- nblocks n)))))

;; The public calls are external symbols, so a kernel or driver reaches them through
;; nile.k's externs (dlsym) and not only through this file's own references.
(export nile_startup nile_shutdown nile_sync nile_status nile_error nile_print_leaks)

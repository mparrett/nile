;; The worker pool, as nile.c and nile-thread.h. Each worker has one mutex and
;; one condition variable where the C runtime has a spin lock and pauses. The
;; liaison is the caller's thread, threads[nthreads]; the nthreads workers are
;; OS threads, threads[0 .. nthreads-1], and the liaison runs processes itself
;; during nile_sync and while a pour waits for its consumer.
;;
;; Typed Maru can't embed a struct by value, so every record the pool needs
;; sits in the memory it was given, and a record points at the pieces it owns.
;; The records of the pool and of a process refer to one another, so they are
;; defined together here, ahead of the code that uses them (RFC "Data model").

(define (BLOCK_BYTES long) 512)

;; The floats in a process's scratch area: what a kernel writes, and reads back, when no block is free (nile-kernel.k).
(define (SCRATCH_FLOATS int) 16)

;; One buffer of a stream: a node, then floats from nile_Buffer_data on.
(define-struct nile_Buffer
  ((next     struct nile_Node *)
   (type     int)
   (head     int)
   (tail     int)
   (capacity int)))

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
   (nblocks      int)
   (ngated       int)
   (nlive        int)
   (ntick        int)
   (waiting      int)
   (status       int)
   (handed       int)     ; 1 when the running process rerouted itself: its work function stops there
   (stopped      int)     ; 1 when the running process stopped for its consumer's quota, until after_work reads it
   (dropped      int)     ; buffers with data in them freed by processes on this thread that had no consumer
   (requeued     int)     ; processes this thread queued while they ran, or after they were freed
   (reading      struct nile_Buffer *)))   ; the input buffer the running kernel reads, 0 for none

;; A process record. Its lock and input deque sit in the block after the record,
;; and the variable record after those; see nile_Process_memory in nile-kernel.k.
(define-struct nile_Process
  ((next         struct nile_Node *)
   (type         int)
   (thread       struct nile_Thread *)
   (lock         struct nile_Mutex *)
   (heap         struct nile_Block *)
   (input        struct nile_Deque *)
   (in_quantum   int)
   (out_quantum  int)
   (sizeof_vars  int)
   (prologue     nile_Process_work_function_t)
   (body         nile_Process_work_function_t)
   (epilogue     nile_Process_work_function_t)
   (state        int)
   (producer     struct nile_Process *)
   (consumer     struct nile_Process *)
   (gatee        struct nile_Process *)
   (parent       struct nile_Process *)
   (out          struct nile_Buffer *)
   ;; SCRATCH_FLOATS floats the kernel writes when no block is free (nile_Process_scratch): contiguous, last in the record.
   (scratch0 float)
   (scratch1 float)
   (scratch2 float)
   (scratch3 float)
   (scratch4 float)
   (scratch5 float)
   (scratch6 float)
   (scratch7 float)
   (scratch8 float)
   (scratch9 float)
   (scratch10 float)
   (scratch11 float)
   (scratch12 float)
   (scratch13 float)
   (scratch14 float)
   (scratch15 float)))

(define (dprintf (int int char * ...) *) (extern dprintf))

;; The i-th record of size bytes from base.
(define-function nile_offset long ((base long) (size long) (i int))
  (return (+ base (* size (cast i long)))))

(define-function nile_Thread_at (struct nile_Thread *) ((threads struct nile_Thread *) (i int))
  (return (cast (nile_offset (cast threads long) (cast (sizeof struct nile_Thread) long) i)
		struct nile_Thread *)))

;; The liaison is the record after the workers'.
(define-function nile_Thread_liaison (struct nile_Thread *) ((t struct nile_Thread *))
  (return (nile_Thread_at (. (indir t) threads) (. (indir t) nthreads))))

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
  (set (. (indir t) nblocks) 0)
  (set (. (indir t) ngated) 0)
  (set (. (indir t) nlive) 0)
  (set (. (indir t) ntick) 0)
  (set (. (indir t) waiting) 0)
  (set (. (indir t) handed) 0)
  (set (. (indir t) stopped) 0)
  (set (. (indir t) dropped) 0)
  (set (. (indir t) requeued) 0)
  (set (. (indir t) reading) (cast 0 struct nile_Buffer *))
  (set (. (indir t) status) NILE_STATUS_OK))

;; The thread's status, read under its lock.
(define-function nile_Thread_status_of int ((t struct nile_Thread *))
  (let (((s int) 0))
    (nile_lock (. (indir t) lock))
    (set s (. (indir t) status))
    (nile_unlock (. (indir t) lock))
    (return s)))

;; Takes the head chunk off t's public heap under t's lock; 0 when it has none.
(define-function nile_Thread_pop_public_chunk (struct nile_Block *) ((t struct nile_Thread *))
  (nile_lock (. (indir t) lock))
  (let (((c struct nile_Block *) (nile_Heap_pop_chunk (addressof (. (indir t) public_heap)))))
    (nile_unlock (. (indir t) lock))
    (return c)))

;; The i-th victim of t's steal order, as nile.c's nile_Thread_steal: victims
;; alternate above and below t's index, so i = 1 .. nthreads covers every worker
;; and, for the liaison, worker 0 as well.
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
    (while (and (= 0 (cast c long)) (<= i (. (indir t) nthreads)))
      (set c (nile_Thread_pop_public_chunk (nile_Thread_victim t i)))
      (set i (+ i 1)))
    (if (= 0 (cast c long))
	(set c (nile_Thread_pop_public_chunk (nile_Thread_liaison t))))
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

;; ---------------------------------------------------------------- queues

;; Pops the head of victim's queue under its lock; 0 when it is empty.
(define-function nile_Thread_steal_from_q (struct nile_Node *) ((v struct nile_Thread *))
  (let (((nd struct nile_Node *) (cast 0 struct nile_Node *)))
    (nile_lock (. (indir v) lock))
    (set nd (nile_Deque_pop_head (. (indir v) q)))
    (nile_unlock (. (indir v) lock))
    (return nd)))

;; Pops the tail of t's own queue under its lock; 0 when it is empty.
(define-function nile_Thread_pop_own (struct nile_Node *) ((t struct nile_Thread *))
  (let (((nd struct nile_Node *) (cast 0 struct nile_Node *)))
    (nile_lock (. (indir t) lock))
    (set nd (nile_Deque_pop_tail (. (indir t) q)))
    (nile_unlock (. (indir t) lock))
    (return nd)))

;; Takes one process from another worker, in steal order, then from the liaison.
(define-function nile_Thread_steal_process (struct nile_Node *) ((t struct nile_Thread *))
  (let (((nd struct nile_Node *) (cast 0 struct nile_Node *))
	(i 1))
    (while (and (= 0 (cast nd long)) (<= i (. (indir t) nthreads)))
      (set nd (nile_Thread_steal_from_q (nile_Thread_victim t i)))
      (set i (+ i 1)))
    (if (= 0 (cast nd long))
	(set nd (nile_Thread_steal_from_q (nile_Thread_liaison t))))
    (return nd)))

;; Wakes one worker that is waiting for work, if any, after a push.
(define-function nile_Thread_wake_one void ((t struct nile_Thread *))
  (let ((i 0)
	(done 0)
	(n (. (indir t) nthreads)))
    (while (and (= done 0) (< i n))
      (let (((o struct nile_Thread *) (nile_Thread_at (. (indir t) threads) i)))
	(nile_lock (. (indir o) lock))
	(if (. (indir o) waiting)
	    (let ()
	      (nile_signal (. (indir o) wake))
	      (set done 1)))
	(nile_unlock (. (indir o) lock)))
      (set i (+ i 1)))))

;; Queues nd on t, counting it live on the liaison first, so the count never
;; reads zero while work is on its way; then wakes t, or another waiting worker. A process queued while it
;; runs (it holds a thread) or after it was freed (its block's type is gone) is counted (nile_requeued).
(define-function nile_Thread_append_to_q void ((t struct nile_Thread *) (nd struct nile_Node *))
  (let (((l struct nile_Thread *) (nile_Thread_liaison t))
	((p struct nile_Process *) (cast nd struct nile_Process *)))
    (if (or (!= NILE_PROCESS_TYPE (. (indir p) type)) (!= 0 (cast (. (indir p) thread) long)))
	(let ()
	  (dprintf 2 "# requeued %lx type %d thread %lx state %d\n" (cast p long) (. (indir p) type)
		   (cast (. (indir p) thread) long) (. (indir p) state))
	  (nile_lock (. (indir t) lock))
	  (set (. (indir t) requeued) (+ (. (indir t) requeued) 1))
	  (nile_unlock (. (indir t) lock))))
    (nile_lock (. (indir l) lock))
    (set (. (indir l) nlive) (+ (. (indir l) nlive) 1))
    (nile_unlock (. (indir l) lock)))
  (nile_lock (. (indir t) lock))
  (nile_Deque_push_tail (. (indir t) q) nd)
  (if (. (indir t) waiting)
      (nile_signal (. (indir t) wake)))
  (nile_unlock (. (indir t) lock))
  (nile_Thread_wake_one t))

;; A queued process has finished its run: the liaison's live count drops, and
;; the liaison wakes when nothing is live.
(define-function nile_Thread_finished void ((t struct nile_Thread *))
  (let (((l struct nile_Thread *) (nile_Thread_liaison t)))
    (nile_lock (. (indir l) lock))
    (set (. (indir l) nlive) (- (. (indir l) nlive) 1))
    (if (and (= 0 (. (indir l) nlive)) (. (indir l) waiting))
	(nile_signal (. (indir l) wake)))
    (nile_unlock (. (indir l) lock))))

;; A gated pipeline has finished: the liaison's gate count drops.
(define-function nile_Thread_gate_done void ((t struct nile_Thread *))
  (let (((l struct nile_Thread *) (nile_Thread_liaison t)))
    (nile_lock (. (indir l) lock))
    (set (. (indir l) ngated) (- (. (indir l) ngated) 1))
    (if (and (= 0 (. (indir l) ngated)) (. (indir l) waiting))
	(nile_signal (. (indir l) wake)))
    (nile_unlock (. (indir l) lock))))

;; Something the liaison may be waiting for has changed: its tick moves, and
;; it wakes if it is waiting.
(define-function nile_Thread_poke void ((l struct nile_Thread *))
  (nile_lock (. (indir l) lock))
  (set (. (indir l) ntick) (+ (. (indir l) ntick) 1))
  (if (. (indir l) waiting)
      (nile_signal (. (indir l) wake)))
  (nile_unlock (. (indir l) lock)))

;; Waits for work or shutdown: an idle worker sleeps on its condition variable,
;; checking its queue under the lock first so a push can't be missed.
(define-function nile_Thread_idle void ((t struct nile_Thread *))
  (nile_lock (. (indir t) lock))
  (while (and (= 0 (. (indir (. (indir t) q)) n))
	      (!= NILE_STATUS_SHUTTING_DOWN (. (indir t) status)))
    (set (. (indir t) waiting) 1)
    (nile_wait (. (indir t) wake) (. (indir t) lock))
    (set (. (indir t) waiting) 0))
  (nile_unlock (. (indir t) lock)))

;; The liaison waits until its tick moves from seen; a poke after the read
;; of seen is never missed, because both happen under the liaison's lock.
(define-function nile_Thread_sleep_from void ((l struct nile_Thread *) (seen int))
  (nile_lock (. (indir l) lock))
  (if (= seen (. (indir l) ntick))
      (let ()
	(set (. (indir l) waiting) 1)
	(nile_wait (. (indir l) wake) (. (indir l) lock))
	(set (. (indir l) waiting) 0)))
  (nile_unlock (. (indir l) lock)))

;; Runs nd, then the rest of t's own queue, until it is empty or t is shut down.
(define-function nile_Thread_work void ((t struct nile_Thread *) (nd struct nile_Node *))
  (while (!= 0 (cast nd long))
    (nile_Process_run (cast nd struct nile_Process *) t)
    (nile_Thread_finished t)
    (if (= NILE_STATUS_SHUTTING_DOWN (nile_Thread_status_of t))
	(set nd (cast 0 struct nile_Node *))
	(set nd (nile_Thread_pop_own t)))))

;; The worker's thread body: run its own queue, steal when it is empty, and
;; wait when there is nothing to steal either, until shut down.
(define-function nile_Thread_main (void *) ((arg void *))
  (let (((t struct nile_Thread *) (cast arg struct nile_Thread *))
	((nd struct nile_Node *) (cast 0 struct nile_Node *)))
    (nile_sched_enter (. (indir t) index))
    (while (!= NILE_STATUS_SHUTTING_DOWN (nile_Thread_status_of t))
      (set nd (nile_Thread_pop_own t))
      (if (= 0 (cast nd long))
	  (set nd (nile_Thread_steal_process t)))
      (if (= 0 (cast nd long))
	  (nile_Thread_idle t)
	  (nile_Thread_work t nd)))
    (nile_sched_leave)
    (return (cast 0 void *))))

;; ---------------------------------------------------------------- runtime calls

;; Starts a runtime in memory: the thread records, their locks, conditions,
;; queues and OS thread handles, then the root process's block, then blocks dealt
;; round-robin to the workers' public heaps. Returns 0 when memory can't hold
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
	  ((root struct nile_Process *) (cast root_at struct nile_Process *))
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
      (set nile_blocks_from blocks_at)
      (set nile_blocks_to end)
      (nile_Process_setup root root 1 1 0 (cast 0 nile_Process_work_function_t)
			  (cast 0 nile_Process_work_function_t) (cast 0 nile_Process_work_function_t))
      (set i 0)
      (while (<= (+ block BLOCK_BYTES) end)
	(nile_Heap_push (addressof (. (indir (nile_Thread_at threads (% i nthreads))) public_heap))
			(cast block struct nile_Block *))
	(set block (+ block BLOCK_BYTES))
	(set nblocks (+ nblocks 1))
	(set i (+ i 1)))
      (set i 0)
      (while (<= i nthreads)
	(set (. (indir (nile_Thread_at threads i)) nblocks) nblocks)
	(set i (+ i 1)))
      (nile_Process_activate root (nile_Thread_at threads nthreads))
      (nile_sched_slot nthreads 1)
      (set nile_sched_dump nile_print_threads_of)
      (set nile_sched_dump_arg (cast (nile_Thread_at threads nthreads) void *))
      (set i 0)
      (while (< i nthreads)
	(let (((t struct nile_Thread *) (nile_Thread_at threads i)))
	  (nile_sched_slot i 0)
	  (pthread_create (. (indir t) osthread) (cast 0 void *)
			  (cast nile_Thread_main void *) (cast t void *)))
	(set i (+ i 1)))
      (return root))))

;; Returns the status the liaison's thread holds.
(define-function nile_status int ((p struct nile_Process *))
  (return (nile_Thread_status_of (nile_Process_thread p))))

(define-function nile_error int ((p struct nile_Process *))
  (return (!= NILE_STATUS_OK (nile_status p))))

;; Runs queued processes, the liaison's own and any it can steal, until nothing
;; is live, then returns once no pipeline is gated; it also returns when the
;; runtime has an error, since the processes stop on their own then.
(define-function nile_sync void ((p struct nile_Process *))
  (let (((t struct nile_Thread *) (nile_Process_deactivate p))
	((done int) 0)
	((nd struct nile_Node *) (cast 0 struct nile_Node *)))
    (while (= done 0)
      (set nd (nile_Thread_pop_own t))
      (if (= 0 (cast nd long))
	  (set nd (nile_Thread_steal_process t)))
      (if (!= 0 (cast nd long))
	  (nile_Thread_work t nd)
	  (let ()
	    (nile_lock (. (indir t) lock))
	    (if (or (!= NILE_STATUS_OK (. (indir t) status))
		    (and (= 0 (. (indir t) nlive)) (= 0 (. (indir t) ngated)))
		    (< 0 (nile_deadlocks)))		; a schedule that deadlocked (a test's): nothing will run
		(set done 1)
		(let ()
		  (set (. (indir t) waiting) 1)
		  (nile_wait (. (indir t) wake) (. (indir t) lock))
		  (set (. (indir t) waiting) 0)))
	    (nile_unlock (. (indir t) lock)))))
    (nile_Process_activate p t)))

;; Stops every worker, wakes each one under its own lock, joins the OS threads,
;; and returns the memory the runtime was started in.
(define-function nile_shutdown (char *) ((p struct nile_Process *))
  (let* (((t struct nile_Thread *) (nile_Process_deactivate p))
	 ((n int) (. (indir t) nthreads))
	 (i 0))
    (nile_unschedule)
    (while (<= i n)
      (let (((o struct nile_Thread *) (nile_Thread_at (. (indir t) threads) i)))
	(nile_lock (. (indir o) lock))
	(set (. (indir o) status) NILE_STATUS_SHUTTING_DOWN)
	(nile_broadcast (. (indir o) wake))
	(nile_unlock (. (indir o) lock)))
      (set i (+ i 1)))
    (set i 0)
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
;; runtime carved, as nile.c's nile_print_leaks does. The root's heap counts
;; too, so the root is deactivated while the count runs.
(define-function nile_print_leaks void ((p struct nile_Process *))
  (let* (((t struct nile_Thread *) (nile_Process_deactivate p))
	((nblocks int) (. (indir t) nblocks))
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
	(dprintf 2 "# blocks not in a heap: %d\n" (- nblocks n)))
    (nile_Process_activate p t)))

;; The public calls are external symbols, so a kernel or driver reaches them through
;; nile.k's externs (dlsym) and not only through this file's own references.
;; The buffers with data in them that processes with no consumer have freed, on every thread. A pipeline's
;; last kernel may make output nobody reads, as in C, but one that ends in a Capture, which makes none, drops
;; nothing: anything here is output lost on the way, which sync would otherwise report as success.
(define-function nile_dropped int ((p struct nile_Process *))
  (let* (((t struct nile_Thread *) (nile_Process_thread p))
	 ((n int) 0)
	 (i 0))
    (while (<= i (. (indir t) nthreads))
      (let (((o struct nile_Thread *) (nile_Thread_at (. (indir t) threads) i)))
	(nile_lock (. (indir o) lock))
	(set n (+ n (. (indir o) dropped)))
	(nile_unlock (. (indir o) lock)))
      (set i (+ i 1)))
    (return n)))

;; The processes queued while they ran or after they were freed (nile_Thread_append_to_q), on every thread.
(define-function nile_requeued int ((p struct nile_Process *))
  (let* (((t struct nile_Thread *) (nile_Process_thread p))
	 ((n int) 0)
	 (i 0))
    (while (<= i (. (indir t) nthreads))
      (let (((o struct nile_Thread *) (nile_Thread_at (. (indir t) threads) i)))
	(nile_lock (. (indir o) lock))
	(set n (+ n (. (indir o) requeued)))
	(nile_unlock (. (indir o) lock)))
      (set i (+ i 1)))
    (return n)))

;; The blocks not back in a heap, as nile_print_leaks counts them: after a sync with nothing held for a later
;; pour, a process left blocked still has its block, so this is what a stranded pipeline leaves.
(define-function nile_unreturned int ((p struct nile_Process *))
  (let* (((t struct nile_Thread *) (nile_Process_deactivate p))
	((nblocks int) (. (indir t) nblocks))
	((n int) 0)
	(i 0))
    (while (<= i (. (indir t) nthreads))
      (let (((o struct nile_Thread *) (nile_Thread_at (. (indir t) threads) i)))
	(nile_lock (. (indir o) lock))
	(set n (+ n (nile_Heap_size (. (indir o) private_heap))))
	(set n (+ n (nile_Heap_size (. (indir o) public_heap))))
	(nile_unlock (. (indir o) lock)))
      (set i (+ i 1)))
    (nile_Process_activate p t)
    (return (- nblocks n))))

;; The last funnel poured (nile_Funnel_pour), so a test's dump can walk its pipeline.
(define (nile_last_funnel struct nile_Process *))
;; The blocks nile_startup carved, first and past the last, so a test can list the processes still alive.
(define (nile_blocks_from long))
(define (nile_blocks_to long))

;; The liaison's counts and every thread's queue, to stderr: what a test prints when a schedule went wrong.
(define-function nile_print_threads void ((t struct nile_Thread *))
  (let ((i 0)
	((p struct nile_Process *) nile_last_funnel)
	((k int) 0))
    (while (and (!= 0 (cast p long)) (< k 12))
      (dprintf 2 "# process %lx state %d input %d producer %lx consumer %lx thread %lx\n" (cast p long) (. (indir p) state)
	       (. (indir (. (indir p) input)) n) (cast (. (indir p) producer) long) (cast (. (indir p) consumer) long)
	       (cast (. (indir p) thread) long))
      (set p (. (indir p) consumer))
      (set k (+ k 1)))
    (dprintf 2 "# nlive %d ngated %d status %d tick %d\n" (. (indir t) nlive) (. (indir t) ngated) (. (indir t) status) (. (indir t) ntick))
    (while (<= i (. (indir t) nthreads))
      (let (((o struct nile_Thread *) (nile_Thread_at (. (indir t) threads) i)))
	(dprintf 2 "# thread %d: queued %d waiting %d wake %lx\n" i (. (indir (. (indir o) q)) n) (. (indir o) waiting) (cast (. (indir o) wake) long)))
      (set i (+ i 1)))))

(define-function nile_print_threads_of void ((arg void *))
  (nile_print_threads (cast arg struct nile_Thread *)))

(define-function nile_print_state void ((p struct nile_Process *))
  (let* (((t struct nile_Thread *) (nile_Process_thread p))
	 (i 0))
    (dprintf 2 "# nlive %d ngated %d status %d\n" (. (indir t) nlive) (. (indir t) ngated) (. (indir t) status))
    (while (<= i (. (indir t) nthreads))
      (let (((o struct nile_Thread *) (nile_Thread_at (. (indir t) threads) i)))
	(dprintf 2 "# thread %d: queued %d waiting %d\n" i (. (indir (. (indir o) q)) n) (. (indir o) waiting)))
      (set i (+ i 1)))))

(export nile_print_state)
(export nile_startup nile_shutdown nile_sync nile_status nile_error nile_print_leaks nile_dropped nile_requeued
	nile_unreturned nile_schedule nile_schedule_plan nile_deadlocks nile_handoffs)

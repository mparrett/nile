;; Processes, as nile-process.c: the scheduler states, pipes, gates and the
;; quotas that stop a producer from running ahead of its consumer. A process
;; runs one work function on the input buffers it is given and the output
;; buffer it writes; the kernel calls the interface in nile-kernel.k.
;;
;; A process runs on one thread at a time. Its input deque and state are read
;; by its producer and consumer, so they are read and written under its lock;
;; a thread holds at most one lock at a time.

(define (NILE_NOT_BLOCKED int) 0)
(define (NILE_BLOCKED_ON_PRODUCER int) 1)
(define (NILE_BLOCKED_ON_CONSUMER int) 2)
(define (NILE_BLOCKED_ON_GATE int) 3)

(define (NILE_BUFFER_TYPE int) 1)
(define (NILE_PROCESS_TYPE int) 2)

(define (INPUT_QUOTA int) 5)
(define (INPUT_MAX int) 10)

;; The process's lock, input deque and variables follow its record in its block.
(define-function nile_Process_lock_at (struct nile_Mutex *) ((p struct nile_Process *))
  (return (cast (+ (cast p long) (cast (sizeof struct nile_Process) long)) struct nile_Mutex *)))

(define-function nile_Process_deque_at (struct nile_Deque *) ((p struct nile_Process *))
  (return (cast (+ (cast p long)
		   (+ (cast (sizeof struct nile_Process) long) (cast (sizeof struct nile_Mutex) long)))
		struct nile_Deque *)))

;; Bytes from a process's block to its variable record.
(define-function nile_Process_vars_offset long ()
  (return (+ (cast (sizeof struct nile_Process) long)
	     (+ (cast (sizeof struct nile_Mutex) long) (cast (sizeof struct nile_Deque) long)))))

(define-function nile_Deque_init void ((d struct nile_Deque *))
  (set (. (indir d) head) (cast 0 struct nile_Node *))
  (set (. (indir d) tail) (cast 0 struct nile_Node *))
  (set (. (indir d) n) 0))

;; A process with no body passes its input through, then is removed (the RFC's "missing body"). A
;; process's body is never 0 until swap or a reroute sets it so, which marks it for removal;
;; without this default, a process with only a prologue would be removed before the prologue ran.
(define-function nile_Process_default_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let ((status 0))
    (while (= status 0)
      (while (< i m)
	(if (= j n)
	    (nile_Process_advance_output p (addressof out) (addressof j) (addressof n)))
	(set (indir out j) (indir in i))
	(set j (+ j 1))
	(set i (+ i 1)))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

;; Fills in a process record in its block. The work functions may be 0.
(define-function nile_Process_setup void
  ((p struct nile_Process *) (parent struct nile_Process *)
   (in_quantum int) (out_quantum int) (sizeof_vars int)
   (prologue nile_Process_work_function_t)
   (body nile_Process_work_function_t)
   (epilogue nile_Process_work_function_t))
  (set (. (indir p) next) (cast 0 struct nile_Node *))
  (set (. (indir p) type) NILE_PROCESS_TYPE)
  (set (. (indir p) thread) (cast 0 struct nile_Thread *))
  (set (. (indir p) lock) (nile_Process_lock_at p))
  (pthread_mutex_init (nile_Process_lock_at p) (cast 0 void *))
  (set (. (indir p) heap) (cast 0 struct nile_Block *))
  (set (. (indir p) input) (nile_Process_deque_at p))
  (nile_Deque_init (nile_Process_deque_at p))
  (set (. (indir p) in_quantum) in_quantum)
  (set (. (indir p) out_quantum) out_quantum)
  (set (. (indir p) sizeof_vars) sizeof_vars)
  (set (. (indir p) prologue) prologue)
  (if (= 0 (cast body long))
      (set body nile_Process_default_body))
  (set (. (indir p) body) body)
  (set (. (indir p) epilogue) epilogue)
  (set (. (indir p) state) NILE_BLOCKED_ON_PRODUCER)
  (set (. (indir p) producer) p)
  (set (. (indir p) consumer) (cast 0 struct nile_Process *))
  (set (. (indir p) gatee) (cast 0 struct nile_Process *))
  (set (. (indir p) parent) parent)
  (set (. (indir p) out) (cast 0 struct nile_Buffer *))
  (set (. (indir p) scratch) (cast 0.0 float)))

(define-function nile_Process_thread (struct nile_Thread *) ((p struct nile_Process *))
  (return (. (indir p) thread)))

;; The process's state, read under its lock; -1 for no process.
(define-function nile_Process_state_of int ((p struct nile_Process *))
  (let (((s int) -1))
    (if (!= 0 (cast p long))
	(let ()
	  (nile_lock (. (indir p) lock))
	  (set s (. (indir p) state))
	  (nile_unlock (. (indir p) lock))))
    (return s)))

(define-function nile_Process_set_state void ((p struct nile_Process *) (s int))
  (nile_lock (. (indir p) lock))
  (set (. (indir p) state) s)
  (nile_unlock (. (indir p) lock)))

;; The producer and consumer, read under the process's lock.
(define-function nile_Process_producer_of (struct nile_Process *) ((p struct nile_Process *))
  (let (((pr struct nile_Process *) (cast 0 struct nile_Process *)))
    (nile_lock (. (indir p) lock))
    (set pr (. (indir p) producer))
    (nile_unlock (. (indir p) lock))
    (return pr)))

(define-function nile_Process_consumer_of (struct nile_Process *) ((p struct nile_Process *))
  (let (((c struct nile_Process *) (cast 0 struct nile_Process *)))
    (nile_lock (. (indir p) lock))
    (set c (. (indir p) consumer))
    (nile_unlock (. (indir p) lock))
    (return c)))

;; The number of buffers waiting on p's input, under its lock.
(define-function nile_Process_input_n int ((p struct nile_Process *))
  (let (((n int) 0))
    (nile_lock (. (indir p) lock))
    (set n (. (indir (. (indir p) input)) n))
    (nile_unlock (. (indir p) lock))
    (return n)))

;; The buffer at the head of p's input, or 0.
(define-function nile_Process_head (struct nile_Buffer *) ((p struct nile_Process *))
  (let (((h struct nile_Node *) (cast 0 struct nile_Node *)))
    (nile_lock (. (indir p) lock))
    (set h (. (indir (. (indir p) input)) head))
    (nile_unlock (. (indir p) lock))
    (return (cast h struct nile_Buffer *))))

;; Out of the output buffer's capacity, the floats a buffer takes whole elements of p's output.
(define-function nile_Process_limit int ((p struct nile_Process *))
  (let ((q (. (indir p) out_quantum)))
    (return (* (/ (nile_Buffer_capacity) q) q))))

;; A node from p's heap, taking a chunk from its thread when the heap is empty; 0 when none.
(define-function nile_Process_alloc_node (struct nile_Node *) ((p struct nile_Process *))
  (let (((v struct nile_Block *) (nile_Heap_pop (addressof (. (indir p) heap)))))
    (if (= 0 (cast v long))
	(let (((c struct nile_Block *) (nile_Thread_alloc_chunk (. (indir p) thread))))
	  (if (!= 0 (cast c long))
	      (nile_Heap_push_chunk (addressof (. (indir p) heap)) c))
	  (set v (nile_Heap_pop (addressof (. (indir p) heap))))))
    (return (cast v struct nile_Node *))))

(define-function nile_Process_free_node void ((p struct nile_Process *) (nd struct nile_Node *))
  (if (nile_Heap_push (addressof (. (indir p) heap)) (cast nd struct nile_Block *))
      (nile_Thread_free_chunk (. (indir p) thread)
			      (nile_Heap_pop_chunk (addressof (. (indir p) heap))))))

;; Records a failed call on the thread p runs on: the liaison, for a call made outside a run.
(define-function nile_Process_fail void ((p struct nile_Process *) (status int))
  (let (((t struct nile_Thread *) (. (indir p) thread)))
    (nile_lock (. (indir t) lock))
    (set (. (indir t) status) status)
    (nile_unlock (. (indir t) lock))))

;; ---------------------------------------------------------------- public pipes

;; Connects p1's output to p2's input, after p1's chain; returns p1 (as C's
;; nile_Process_pipe). Either being 0 connects nothing, so a failed allocation
;; upstream carries on as the C runtime does.
(define-function nile_Process_pipe (struct nile_Process *) ((p1 struct nile_Process *) (p2 struct nile_Process *))
  (if (or (= 0 (cast p1 long)) (= 0 (cast p2 long)))
      (return p1))
  (let ((pi p1))
    (while (!= 0 (cast (. (indir pi) consumer) long))
      (set pi (. (indir pi) consumer)))
    (set (. (indir pi) consumer) p2)
    (set (. (indir p2) producer) pi)
    (return p1)))

;; The chain of ps[0 .. n-1], piped in order; returns ps[0], or 0 if one is 0.
(define-function nile_Process_pipeline (struct nile_Process *) ((ps struct nile_Process * *) (n int))
  (let ((j 1))
    (if (or (= n 0) (= 0 (cast (indir ps 0) long)))
	(return (cast 0 struct nile_Process *)))
    (while (< j n)
      (if (= 0 (cast (indir ps j) long))
	  (return (cast 0 struct nile_Process *)))
      (nile_Process_pipe (indir ps 0) (indir ps j))
      (set j (+ j 1)))
    (return (indir ps 0))))

;; Makes gatee wait until gater finishes: the liaison counts the gate.
(define-function nile_Process_gate void ((gater struct nile_Process *) (gatee struct nile_Process *))
  (if (and (!= 0 (cast gater long)) (!= 0 (cast gatee long)))
      (let (((l struct nile_Thread *) (nile_Thread_liaison (. (indir (. (indir gatee) parent)) thread))))
	(nile_lock (. (indir l) lock))
	(set (. (indir l) ngated) (+ (. (indir l) ngated) 1))
	(nile_unlock (. (indir l) lock))
	(set (. (indir gater) gatee) gatee)
	(nile_Process_set_state gatee NILE_BLOCKED_ON_GATE))))

;; Pours n floats into the pipeline starting at p. A count that isn't a whole
;; number of p's input elements is BAD_ARG, and nothing is fed.
(define-function nile_Process_feed void ((p struct nile_Process *) (data float *) (n int))
  (if (!= 0 (cast p long))
      (let (((parent struct nile_Process *) (. (indir p) parent)))
	(if (!= 0 (% n (. (indir p) in_quantum)))
	    (nile_Process_fail parent NILE_STATUS_BAD_ARG)
	    (let (((f struct nile_Process *) (nile_Funnel parent)))
	      (nile_Process_pipe f p)
	      (nile_Funnel_pour f data n 1))))))

(define-function nile_Process_launch void ((ps struct nile_Process * *) (nps int) (data float *) (ndata int))
  (let (((head struct nile_Process *) (nile_Process_pipeline ps nps)))
    (if (!= 0 (cast head long))
	(nile_Process_feed head data ndata))))

;; ---------------------------------------------------------------- scheduling

;; 1 when p should stay blocked: its producer is still making output, so p waits for it.
(define-function nile_Process_block_on_producer int ((p struct nile_Process *))
  (let (((pr struct nile_Process *) (nile_Process_producer_of p))
	((pstate int) 0))
    (if (= 0 (cast pr long))
	(return 0))
    (nile_Process_set_state p NILE_BLOCKED_ON_PRODUCER)
    (set pstate (nile_Process_state_of pr))
    (return (!= pstate NILE_BLOCKED_ON_CONSUMER))))

;; Puts p on the thread's queue when it can run, or leaves it blocked. A process
;; with no body is passed through and removed.
(define-function nile_Process_schedule (struct nile_Block *)
  ((p struct nile_Process *) (t struct nile_Thread *) (heap struct nile_Block *))
  (if (= 0 (cast (. (indir p) body) long))
      (return (nile_Process_remove p t heap)))
  (if (= (cast (. (indir p) body) long) (cast nile_Funnel_body long))
      (let ()
	(nile_Process_set_state p NILE_NOT_BLOCKED)
	(nile_Thread_poke (nile_Thread_liaison t)))
      (if (or (>= (nile_Process_input_n p) INPUT_QUOTA)
	      (= 0 (nile_Process_block_on_producer p)))
	  (let ()
	    (nile_Process_set_state p NILE_NOT_BLOCKED)
	    (nile_Thread_append_to_q t (cast p struct nile_Node *)))))
  (return heap))

;; Schedules p on t's private heap.
(define-function nile_Process_schedule_on void ((p struct nile_Process *) (t struct nile_Thread *))
  (set (. (indir t) private_heap) (nile_Process_schedule p t (. (indir t) private_heap))))

;; Removes p from its pipeline: its input goes to its consumer, its producer is
;; joined to its consumer, and its block goes back to the heap.
(define-function nile_Process_remove (struct nile_Block *)
  ((p struct nile_Process *) (t struct nile_Thread *) (heap struct nile_Block *))
  (let (((producer struct nile_Process *) (nile_Process_producer_of p))
	((consumer struct nile_Process *) (nile_Process_consumer_of p))
	((cstate int) 0)
	((input struct nile_Deque *) (. (indir p) input)))
    (if (!= 0 (cast (. (indir p) gatee) long))
	(let ()
	  (nile_Thread_gate_done t)
	  (set heap (nile_Process_schedule (. (indir p) gatee) t heap))))
    (if (!= 0 (cast producer long))
	(let ()
	  (nile_lock (. (indir producer) lock))
	  (set (. (indir producer) consumer) consumer)
	  (nile_unlock (. (indir producer) lock))))
    (if (!= 0 (cast consumer long))
	(let ()
	  (nile_lock (. (indir consumer) lock))
	  (while (> (. (indir input) n) 0)
	    (nile_Deque_push_tail (. (indir consumer) input) (nile_Deque_pop_head input)))
	  (set (. (indir consumer) producer) producer)
	  (set cstate (. (indir consumer) state))
	  (nile_unlock (. (indir consumer) lock))
	  (nile_Heap_push (addressof heap) (cast p struct nile_Block *))
	  (if (= cstate NILE_BLOCKED_ON_PRODUCER)
	      (set heap (nile_Process_schedule consumer t heap))))
	(let ()
	  (set (. (indir p) thread) t)
	  (set (. (indir p) heap) heap)
	  (while (> (. (indir input) n) 0)
	    (nile_Process_free_node p (nile_Deque_pop_head input)))
	  (set heap (. (indir p) heap))
	  (nile_Heap_push (addressof heap) (cast p struct nile_Block *))
	  (if (and (!= 0 (cast producer long))
		   (= NILE_BLOCKED_ON_CONSUMER (nile_Process_state_of producer)))
	      (set heap (nile_Process_schedule producer t heap)))))
    (return heap)))

;; Hands a buffer to p's consumer's input, or frees it when it is empty or p has no consumer.
(define-function nile_Process_enqueue_output void ((p struct nile_Process *) (b struct nile_Buffer *))
  (let (((c struct nile_Process *) (nile_Process_consumer_of p)))
    (if (or (= 0 (cast c long)) (= (. (indir b) head) (. (indir b) tail)))
	(nile_Process_free_node p (cast b struct nile_Node *))
	(let ()
	  (nile_lock (. (indir c) lock))
	  (nile_Deque_push_tail (. (indir c) input) (cast b struct nile_Node *))
	  (nile_unlock (. (indir c) lock))))))

;; 1 when the consumer is at its input quota and wants its producer to wait.
(define-function nile_Process_quota_hit int ((c struct nile_Process *))
  (if (= 0 (cast c long))
      (return 0))
  (let (((n int) (nile_Process_input_n c))
	((st int) (nile_Process_state_of c)))
    (return (and (>= n (- INPUT_QUOTA 1))
		 (or (= st NILE_BLOCKED_ON_PRODUCER) (> n INPUT_MAX))))))

;; Hands p's output buffer on and forgets it.
(define-function nile_Process_flush void ((p struct nile_Process *))
  (let (((b struct nile_Buffer *) (. (indir p) out)))
    (if (!= 0 (cast b long))
	(let ()
	  (set (. (indir p) out) (cast 0 struct nile_Buffer *))
	  (nile_Process_enqueue_output p b)))))

;; Gives p's heap and output back to its thread and returns that thread.
(define-function nile_Process_deactivate (struct nile_Thread *) ((p struct nile_Process *))
  (let (((t struct nile_Thread *) (. (indir p) thread)))
    (nile_Process_flush p)
    (set (. (indir t) private_heap) (. (indir p) heap))
    (set (. (indir p) thread) (cast 0 struct nile_Thread *))
    (set (. (indir p) heap) (cast 0 struct nile_Block *))
    (return t)))

;; Takes t's private heap for p.
(define-function nile_Process_activate void ((p struct nile_Process *) (t struct nile_Thread *))
  (set (. (indir p) thread) t)
  (set (. (indir p) heap) (. (indir t) private_heap))
  (set (. (indir t) private_heap) (cast 0 struct nile_Block *)))

;; Pops the input buffer at the head, which the kernel has used up, and schedules
;; the producer when this process had been at quota and the producer waits on it.
(define-function nile_Process_pop_input void ((p struct nile_Process *))
  (let (((pr struct nile_Process *) (nile_Process_producer_of p))
	((pstate int) 0)
	((n int) 0)
	((head struct nile_Node *) (cast 0 struct nile_Node *)))
    (if (!= 0 (cast pr long))
	(set pstate (nile_Process_state_of pr)))
    (nile_lock (. (indir p) lock))
    (set n (. (indir (. (indir p) input)) n))
    (set head (nile_Deque_pop_head (. (indir p) input)))
    (nile_unlock (. (indir p) lock))
    (if (!= 0 (cast head long))
	(nile_Process_free_node p head))
    (if (and (= n INPUT_QUOTA) (!= 0 (cast pr long)) (= pstate NILE_BLOCKED_ON_CONSUMER))
	(set (. (indir p) heap) (nile_Process_schedule pr (. (indir p) thread) (. (indir p) heap))))))

;; Pops the head input buffer if the kernel has used it up (its head meets its tail).
(define-function nile_Process_drop_used void ((p struct nile_Process *))
  (let (((b struct nile_Buffer *) (nile_Process_head p)))
    (if (and (!= 0 (cast b long)) (= (. (indir b) head) (. (indir b) tail)))
	(nile_Process_pop_input p))))

;; 1 when the kernel must stop at this buffer boundary: the runtime has an
;; error, or the consumer is at quota (the process then waits on it).
(define-function nile_Process_stop_wanted int ((p struct nile_Process *))
  (if (!= NILE_STATUS_OK (nile_Thread_status_of (. (indir p) thread)))
      (return 1))
  (if (nile_Process_quota_hit (nile_Process_consumer_of p))
      (let ()
	(nile_Process_set_state p NILE_BLOCKED_ON_CONSUMER)
	(return 1)))
  (return 0))

;; The process has stopped at a buffer boundary: an error or quota backs it up
;; to its consumer; otherwise it has run out of input.
(define-function nile_Process_handle_backpressure void ((p struct nile_Process *))
  (let (((c struct nile_Process *) (nile_Process_consumer_of p))
	((t struct nile_Thread *) (nile_Process_deactivate p))
	((cstate int) 0))
    (nile_Process_set_state p NILE_BLOCKED_ON_CONSUMER)
    (set cstate (nile_Process_state_of c))
    (if (= cstate NILE_BLOCKED_ON_PRODUCER)
	(nile_Process_schedule_on c t))))

;; The process has no input left: it waits on its producer, or runs its epilogue
;; and is removed, or is removed when it has no producer.
(define-function nile_Process_handle_out_of_input void ((p struct nile_Process *))
  (let (((t struct nile_Thread *) (nile_Process_deactivate p))
	((pr struct nile_Process *) (nile_Process_producer_of p)))
    (if (= 0 (nile_Process_block_on_producer p))
	(if (!= 0 (cast pr long))
	    (nile_Process_schedule_on pr t)
	    (if (> (nile_Process_input_n p) 0)
		(nile_Process_schedule_on p t)
		(if (!= 0 (cast (. (indir p) epilogue) long))
		    (nile_Process_epilogue p t)
		    (set (. (indir t) private_heap) (nile_Process_remove p t (. (indir t) private_heap)))))))))

;; Runs p's epilogue on its own output once its input is gone, then removes it. An
;; epilogue that waits for another process (Cat, Zip) deactivates p itself and sets handed,
;; and p is left there.
(define-function nile_Process_epilogue void ((p struct nile_Process *) (t struct nile_Thread *))
  (nile_Process_activate p t)
  (let (((out struct nile_Buffer *) (nile_Buffer p)))
    (if (= 0 (cast out long))
	(nile_Process_deactivate p)
	(let ()
	  (set (. (indir p) out) out)
	  (set (. (indir t) handed) 0)
	  ((. (indir p) epilogue) p (cast 0 float *) 0 0
	   (nile_Buffer_data out) (. (indir out) tail) (nile_Process_limit p))
	  (if (= 0 (. (indir t) handed))
	      (if (!= NILE_STATUS_OK (nile_Thread_status_of t))
		  (nile_Process_deactivate p)
		  (let ((t2 (nile_Process_deactivate p)))
		    (set (. (indir t2) private_heap)
			 (nile_Process_remove p t2 (. (indir t2) private_heap))))))))))

;; The last process of the chain that starts at p. The chain is private to the caller until it's joined.
(define-function nile_Process_chain_end (struct nile_Process *) ((p struct nile_Process *))
  (let ((q p))
    (while (!= 0 (cast (. (indir q) consumer) long))
      (set q (. (indir q) consumer)))
    (return q)))

;; Makes b the consumer of a: a's output goes to b's input. Each end is written under its own lock.
(define-function nile_Process_join void ((a struct nile_Process *) (b struct nile_Process *))
  (nile_lock (. (indir a) lock))
  (set (. (indir a) consumer) b)
  (nile_unlock (. (indir a) lock))
  (nile_lock (. (indir b) lock))
  (set (. (indir b) producer) a)
  (nile_unlock (. (indir b) lock)))

;; Places sub between p and p's consumer, as C's nile_Process_swap, for a kernel that has
;; saved its positions (nile_Process_reroute). p's output so far goes to its old consumer,
;; which deactivate flushes before the relink; the rest of p's input goes to sub, and sub's
;; output to the old consumer. p's body is cleared, so it is removed now, or by its
;; producer's schedule when it has to wait on that producer. Returns the thread's status.
(define-function nile_Process_swap int ((p struct nile_Process *) (sub struct nile_Process *))
  (let (((t struct nile_Thread *) (nile_Process_deactivate p))
	((c struct nile_Process *) (nile_Process_consumer_of p)))
    (set (. (indir t) handed) 1)
    (if (!= 0 (cast sub long))
	(let ()
	  (if (!= 0 (cast c long))
	      (nile_Process_join (nile_Process_chain_end sub) c))
	  (nile_Process_join p sub)))
    (nile_lock (. (indir p) lock))
    (set (. (indir p) body) (cast 0 nile_Process_work_function_t))
    (nile_unlock (. (indir p) lock))
    (if (= 0 (nile_Process_block_on_producer p))
	(set (. (indir t) private_heap) (nile_Process_remove p t (. (indir t) private_heap))))
    (return (nile_Thread_status_of t))))

;; After the kernel returns: an error ends the run; otherwise quota or running
;; out of input decides what happens next.
(define-function nile_Process_after_work void ((p struct nile_Process *) (t struct nile_Thread *))
  (nile_Process_drop_used p)
  (if (!= NILE_STATUS_OK (nile_Thread_status_of t))
      (nile_Process_deactivate p)
      (if (= NILE_BLOCKED_ON_CONSUMER (nile_Process_state_of p))
	  (nile_Process_handle_backpressure p)
	  (nile_Process_handle_out_of_input p))))

;; Calls p's work function on its head input buffer and its current output buffer. A
;; prologue or body that reroutes p has handed it on (handed is set), and then p is left alone.
(define-function nile_Process_work void ((p struct nile_Process *) (t struct nile_Thread *))
  (let (((in float *) (cast 0 float *))
	((i int) 0)
	((m int) 0)
	((head struct nile_Buffer *) (nile_Process_head p)))
    (if (!= 0 (cast head long))
	(let ()
	  (set in (nile_Buffer_data head))
	  (set i (. (indir head) head))
	  (set m (. (indir head) tail))))
    (set (. (indir t) handed) 0)
    (if (!= 0 (cast (. (indir p) prologue) long))
	(let ()
	  ((. (indir p) prologue) p (cast 0 float *) 0 0
	   (nile_Buffer_data (. (indir p) out)) (. (indir (. (indir p) out)) tail) (nile_Process_limit p))
	  (set (. (indir p) prologue) (cast 0 nile_Process_work_function_t))))
    (if (= 0 (. (indir t) handed))
	(if (!= NILE_STATUS_OK (nile_Thread_status_of t))
	    (nile_Process_deactivate p)
	    (if (= 0 (cast (. (indir p) body) long))
		(let ((t2 (nile_Process_deactivate p)))
		  (set (. (indir t2) private_heap)
		       (nile_Process_remove p t2 (. (indir t2) private_heap))))
		(let ()
		  (nile_Process_set_state p NILE_NOT_BLOCKED)
		  ((. (indir p) body) p in i m
		   (nile_Buffer_data (. (indir p) out)) (. (indir (. (indir p) out)) tail) (nile_Process_limit p))
		  ;; A reroute has already deactivated p and handed it on; p is no longer ours to
		  ;; look at (it may be reused once removed).
		  (if (= 0 (. (indir t) handed))
		      (nile_Process_after_work p t))))))))

;; Runs p on thread t until it blocks, is removed, or hands its output on.
(define-function nile_Process_run void ((p struct nile_Process *) (t struct nile_Thread *))
  (nile_Process_activate p t)
  (let (((out struct nile_Buffer *) (nile_Buffer p)))
    (if (= 0 (cast out long))
	(nile_Process_deactivate p)
	(let ()
	  (set (. (indir p) out) out)
	  (nile_Process_work p t)))))

;; The public pipe, gate and feed calls, which kernels and drivers reach through dlsym.
(export nile_Process_pipe nile_Process_pipeline nile_Process_feed nile_Process_gate nile_Process_launch)

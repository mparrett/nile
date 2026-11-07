;; The built-in processes, as nile-builtins.c, against the kernel interface:
;; PassThrough, Capture, Funnel with its pour, Reverse, SortBy, Dup, Cat, DupCat,
;; Zip and DupZip.

;; PassThrough has no body, so the runtime passes its input through and removes it.
(define-function nile_PassThrough (struct nile_Process *) ((parent struct nile_Process *) (quantum int))
  (return (nile_Process parent quantum quantum 0
			(cast 0 nile_Process_work_function_t)
			(cast 0 nile_Process_work_function_t)
			(cast 0 nile_Process_work_function_t))))

;; Capture: copies each float it reads into data, counting them in n, up to size.
(define-struct nile_Capture_vars
  ((data float *)
   (n    int *)
   (size int)))

(define-function nile_Capture_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((v struct nile_Capture_vars *) (nile_Process_memory p))
	((status int) 0))
    (while (= status 0)
      (while (< i m)
	(if (< (indir (. (indir v) n)) (. (indir v) size))
	    (set (indir (. (indir v) data) (indir (. (indir v) n))) (indir in i)))
	(set (indir (. (indir v) n)) (+ (indir (. (indir v) n)) 1))
	(set i (+ i 1)))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

(define-function nile_Capture (struct nile_Process *)
  ((parent struct nile_Process *) (data float *) (n int *) (size int))
  (let* (((p struct nile_Process *)
	  (nile_Process parent 1 1 (sizeof struct nile_Capture_vars)
			(cast 0 nile_Process_work_function_t)
			nile_Capture_body
			(cast 0 nile_Process_work_function_t))))
    (if (!= 0 (cast p long))
	(let (((v struct nile_Capture_vars *) (nile_Process_memory p)))
	  (set (. (indir v) data) data)
	  (set (. (indir v) n) n)
	  (set (. (indir v) size) size)))
    (return p)))

;; Funnel: the source of a pipeline poured from outside. Its variables are the
;; floats still to send, and where it is in them.
(define-struct nile_Funnel_vars
  ((pos   int)
   (count int)
   (data  float *)))

;; Writes floats from the pour's data into output buffers until the data is
;; done or the runtime stops it (an error, or its consumer at quota).
(define-function nile_Funnel_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((v struct nile_Funnel_vars *) (nile_Process_memory p))
	((stop int) 0))
    (while (and (= stop 0) (< (. (indir v) pos) (. (indir v) count)))
      (if (= j n)
	  (let ()
	    (nile_Process_advance_output p (addressof out) (addressof j) (addressof n))
	    (set stop (nile_Process_stop_wanted p))))
      (if (= stop 0)
	  (let ()
	    (set (indir out j) (indir (. (indir v) data) (. (indir v) pos)))
	    (set j (+ j 1))
	    (set (. (indir v) pos) (+ (. (indir v) pos) 1)))))
    (return (nile_Process_return p i j stop))))

(define-function nile_Funnel (struct nile_Process *) ((parent struct nile_Process *))
  (return (nile_Process parent 1 1 (sizeof struct nile_Funnel_vars)
			(cast 0 nile_Process_work_function_t)
			nile_Funnel_body
			(cast 0 nile_Process_work_function_t))))

;; Waits on the liaison, running work, while the funnel p is held at its
;; consumer's quota: it returns when the consumer releases p or the runtime errs.
(define-function nile_Funnel_wait_unblocked void ((t struct nile_Thread *) (p struct nile_Process *))
  (let (((seen int) 0)
	((go int) 1)
	((nd struct nile_Node *) (cast 0 struct nile_Node *)))
    (while go
      ;; the tick before the state: a release after this read moves the tick past seen, so sleep_from won't
      ;; sleep through it. Read after the state, a release between the two was lost, and the liaison slept
      ;; with its funnel free and nothing else to wake it.
      (nile_lock (. (indir t) lock))
      (set seen (. (indir t) ntick))
      (nile_unlock (. (indir t) lock))
      (set go (and (= NILE_BLOCKED_ON_CONSUMER (nile_Process_state_of p))
		   (= NILE_STATUS_OK (nile_Thread_status_of t))))
      (if go
	  (let ()
	    (set nd (nile_Thread_pop_own t))
	    (if (= 0 (cast nd long))
		(set nd (nile_Thread_steal_process t)))
	    (if (!= 0 (cast nd long))
		(let ()
		  (nile_Process_run (cast nd struct nile_Process *) t)
		  (nile_Thread_finished t))
		(nile_Thread_sleep_from t seen)))))))

;; Pours n floats into funnel p. The liaison runs the funnel itself, and waits
;; whenever the consumer holds it at quota. eos marks the last pour: the
;; funnel then has no producer and is removed when it has sent its data.
(define-function nile_Funnel_pour void
  ((p struct nile_Process *) (data float *) (n int) (eos int))
  (if (!= 0 (cast p long))
      (let* (((init struct nile_Process *) (. (indir p) parent))
	     ((liaison struct nile_Thread *) (nile_Process_deactivate init))
	     ((v struct nile_Funnel_vars *) (nile_Process_memory p))
	     ((c struct nile_Process *) (nile_Process_consumer_of p))
	     ((go int) 1))
	(set (. (indir v) pos) 0)
	(set (. (indir v) count) n)
	(set (. (indir v) data) data)
	(if (= eos 1)
	    (set (. (indir p) producer) (cast 0 struct nile_Process *)))
	(if (!= 0 (cast c long))
	    (set (. (indir p) out_quantum) (. (indir c) in_quantum)))
	(while (= go 1)
	  (nile_Process_run p liaison)
	  (if (or (>= (. (indir v) pos) n) (!= NILE_STATUS_OK (nile_Thread_status_of liaison)))
	      (set go 0)
	      (if (= NILE_BLOCKED_ON_CONSUMER (nile_Process_state_of p))
		  (nile_Funnel_wait_unblocked liaison p))))
	(nile_Process_activate init liaison))))

;; Reverse: a barrier. Its input is held in buffers filled from their top down, so that
;; each buffer reads back reversed, and the newest buffer is first in the list. Nothing
;; is output until the epilogue, which hands the buffers on in that order.
(define-struct nile_Reverse_vars
  ((cur  struct nile_Buffer *)      ; the buffer being filled from its top down
   (done struct nile_Buffer *)))    ; full buffers, newest first, linked by next

;; Retires the buffer being filled to the list and takes a fresh one from p's heap as the
;; new cur; 0 when no block is free (cur is then 0 too).
(define-function nile_Reverse_next (struct nile_Buffer *)
  ((p nile_Process_t *) (v struct nile_Reverse_vars *))
  (if (!= 0 (cast (. (indir v) cur) long))
      (let ()
	(set (. (indir (. (indir v) cur)) next) (cast (. (indir v) done) struct nile_Node *))
	(set (. (indir v) done) (. (indir v) cur))))
  (set (. (indir v) cur) (nile_Buffer p))
  (if (!= 0 (cast (. (indir v) cur) long))
      (let ()
	(set (. (indir (. (indir v) cur)) head) (nile_Process_limit p))
	(set (. (indir (. (indir v) cur)) tail) (nile_Process_limit p))))
  (return (. (indir v) cur)))

;; Writes each whole element of the input window into the buffers, from the top down, one
;; element at a time; i moves on through the window, and a full buffer is retired first.
(define-function nile_Reverse_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((v struct nile_Reverse_vars *) (nile_Process_memory p))
	((q int) (. (indir p) in_quantum))
	((status int) 0))
    (while (= status 0)
      (while (<= (+ i q) m)
	(if (or (= 0 (cast (. (indir v) cur) long)) (= 0 (. (indir (. (indir v) cur)) head)))
	    (if (= 0 (cast (nile_Reverse_next p v) long))
		(return (nile_Process_return p i j NILE_STATUS_OUT_OF_MEMORY))))
	(let ((b (. (indir v) cur)))
	  (set (. (indir b) head) (- (. (indir b) head) q))
	  (let ((f 0))
	    (while (< f q)
	      (set (indir (nile_Buffer_data b) (+ (. (indir b) head) f)) (indir in (+ i f)))
	      (set f (+ f 1)))))
	(set i (+ i q)))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

;; Hands the held buffers to the consumer, newest first, which is the reversed stream.
(define-function nile_Reverse_epilogue int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((v struct nile_Reverse_vars *) (nile_Process_memory p))
	((b struct nile_Buffer *) (cast 0 struct nile_Buffer *)))
    (if (!= 0 (cast (. (indir v) cur) long))
	(let ()
	  (set (. (indir (. (indir v) cur)) next) (cast (. (indir v) done) struct nile_Node *))
	  (set (. (indir v) done) (. (indir v) cur))
	  (set (. (indir v) cur) (cast 0 struct nile_Buffer *))))
    (set b (. (indir v) done))
    (set (. (indir v) done) (cast 0 struct nile_Buffer *))
    (while (!= 0 (cast b long))
      (let ((nx (cast (. (indir b) next) struct nile_Buffer *)))
	(nile_Process_enqueue_output p b)
	(set b nx)))
    (return (nile_Process_return p i j NILE_STATUS_OK))))

(define-function nile_Reverse (struct nile_Process *) ((parent struct nile_Process *) (quantum int))
  (let* (((p struct nile_Process *)
	  (nile_Process parent quantum quantum (sizeof struct nile_Reverse_vars)
			(cast 0 nile_Process_work_function_t)
			nile_Reverse_body
			nile_Reverse_epilogue)))
    (if (!= 0 (cast p long))
	(let (((v struct nile_Reverse_vars *) (nile_Process_memory p)))
	  (set (. (indir v) cur) (cast 0 struct nile_Buffer *))
	  (set (. (indir v) done) (cast 0 struct nile_Buffer *))))
    (return p)))

;; SortBy: a barrier too. The sorted buffers are kept in a list, each buffer's elements in
;; order on field index, and every buffer before the next one has keys no larger than its
;; first key. An element goes after the equal keys already there, so the sort is stable.
(define-struct nile_SortBy_vars
  ((index int)
   (head  struct nile_Buffer *)))

;; Puts the element at in[i ..] into the sorted list. 1 when no block is free.
(define-function nile_SortBy_insert int
  ((p nile_Process_t *) (v struct nile_SortBy_vars *) (in float *) (i int) (q int))
  (let (((index int) (. (indir v) index))
	((key float) (indir in (+ i (. (indir v) index))))
	((nd struct nile_Buffer *) (cast 0 struct nile_Buffer *))
	((go int) 1)
	((j int) 0))
    (if (= 0 (cast (. (indir v) head) long))
	(let ((b (nile_Buffer p)))
	  (if (= 0 (cast b long))
	      (return 1))
	  (set (. (indir v) head) b)))
    ;; the last buffer whose first key is no larger than key, or the first buffer
    (set nd (. (indir v) head))
    (while (= go 1)
      (let ((nx (cast (. (indir nd) next) struct nile_Buffer *)))
	(if (= 0 (cast nx long))
	    (set go 0)
	    (if (< key (indir (nile_Buffer_data nx) index))
		(set go 0)
		(set nd nx)))))
    ;; a full buffer splits in two: its upper half moves to a new buffer after it
    (if (< (- (nile_Process_limit p) (. (indir nd) tail)) q)
	(let ((b2 (nile_Buffer p)))
	  (if (= 0 (cast b2 long))
	      (return 1))
	  (set (. (indir b2) next) (. (indir nd) next))
	  (set (. (indir nd) next) (cast b2 struct nile_Node *))
	  (let* (((keep int) (* (/ (/ (. (indir nd) tail) q) 2) q))
		 ((src int) keep))
	    (while (< src (. (indir nd) tail))
	      (set (indir (nile_Buffer_data b2) (. (indir b2) tail)) (indir (nile_Buffer_data nd) src))
	      (set (. (indir b2) tail) (+ (. (indir b2) tail) 1))
	      (set src (+ src 1)))
	    (set (. (indir nd) tail) keep))
	  (if (= 0 (< key (indir (nile_Buffer_data b2) index)))
	      (set nd b2))))
    ;; shift the larger elements of nd up one place, then put the new one in the gap
    (set j (- (. (indir nd) tail) q))
    (set go 1)
    (while (= go 1)
      (if (< j 0)
	  (set go 0)
	  (if (< key (indir (nile_Buffer_data nd) (+ j index)))
	      (let ((f 0))
		(while (< f q)
		  (set (indir (nile_Buffer_data nd) (+ (+ j q) f)) (indir (nile_Buffer_data nd) (+ j f)))
		  (set f (+ f 1)))
		(set j (- j q)))
	      (set go 0))))
    (let ((f 0))
      (while (< f q)
	(set (indir (nile_Buffer_data nd) (+ (+ j q) f)) (indir in (+ i f)))
	(set f (+ f 1))))
    (set (. (indir nd) tail) (+ (. (indir nd) tail) q))
    (return 0)))

(define-function nile_SortBy_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((v struct nile_SortBy_vars *) (nile_Process_memory p))
	((q int) (. (indir p) in_quantum))
	((status int) 0))
    (while (= status 0)
      (while (<= (+ i q) m)
	(if (!= 0 (nile_SortBy_insert p v in i q))
	    (return (nile_Process_return p i j NILE_STATUS_OUT_OF_MEMORY)))
	(set i (+ i q)))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

;; Hands the sorted buffers on in order.
(define-function nile_SortBy_epilogue int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let* (((v struct nile_SortBy_vars *) (nile_Process_memory p))
	 ((b struct nile_Buffer *) (. (indir v) head)))
    (set (. (indir v) head) (cast 0 struct nile_Buffer *))
    (while (!= 0 (cast b long))
      (let ((nx (cast (. (indir b) next) struct nile_Buffer *)))
	(nile_Process_enqueue_output p b)
	(set b nx)))
    (return (nile_Process_return p i j NILE_STATUS_OK))))

(define-function nile_SortBy (struct nile_Process *) ((parent struct nile_Process *) (quantum int) (index int))
  (let* (((p struct nile_Process *)
	  (nile_Process parent quantum quantum (sizeof struct nile_SortBy_vars)
			(cast 0 nile_Process_work_function_t)
			nile_SortBy_body
			nile_SortBy_epilogue)))
    (if (!= 0 (cast p long))
	(let (((v struct nile_SortBy_vars *) (nile_Process_memory p)))
	  (set (. (indir v) index) index)
	  (set (. (indir v) head) (cast 0 struct nile_Buffer *))))
    (return p)))

;; Dup: copies its input to two branches, as C's nile_Dup_body does. Each branch gets its own copy of every
;; element, written in step into a buffer of its own (q1, q2), and each full pair is pushed to the branches' inputs.
;; The consumer is the branch that Dup's back-pressure follows. After each push Dup checks both branches' quotas:
;; when one is at quota, the fuller branch becomes its consumer, the other becomes its own producer (it runs on its
;; own input), and Dup stops in BLOCKED_ON_CONSUMER, so the kernel schedules the consumer. Dup's epilogue pushes the
;; last copies, releases the first branch from its producer, and makes the second its consumer, as C's does.
(define-struct nile_Dup_vars
  ((p1 struct nile_Process *)       ; the head of the first branch
   (p2 struct nile_Process *)       ; the head of the second branch
   (q1 struct nile_Buffer *)        ; the copies for the first branch being filled, 0 when none
   (q2 struct nile_Buffer *)))      ; the copies for the second branch being filled, 0 when none

;; Makes pr the producer of c, under c's lock.
(define-function nile_Process_set_producer void ((c struct nile_Process *) (pr struct nile_Process *))
  (nile_lock (. (indir c) lock))
  (set (. (indir c) producer) pr)
  (nile_unlock (. (indir c) lock)))

;; Pushes the copies b to branch c's input, under its lock; an empty buffer is freed.
(define-function nile_Dup_push void ((p nile_Process_t *) (c struct nile_Process *) (b struct nile_Buffer *))
  (if (= (. (indir b) head) (. (indir b) tail))
      (nile_Process_free_node p (cast b struct nile_Node *))
      (let ()
	(nile_lock (. (indir c) lock))
	(nile_Deque_push_tail (. (indir c) input) (cast b struct nile_Node *))
	(nile_unlock (. (indir c) lock)))))

;; Gives the branches a copy buffer each, unless they have them; 0 when no block is free.
(define-function nile_Dup_open int ((p nile_Process_t *) (v struct nile_Dup_vars *))
  (if (!= 0 (cast (. (indir v) q1) long))
      (return 1))
  (let (((b1 struct nile_Buffer *) (nile_Buffer p)))
    (if (= 0 (cast b1 long))
	(return 0))
    (let (((b2 struct nile_Buffer *) (nile_Buffer p)))
      (if (= 0 (cast b2 long))
	  (let ()
	    (nile_Process_free_node p (cast b1 struct nile_Node *))
	    (return 0)))
      (set (. (indir v) q1) b1)
      (set (. (indir v) q2) b2)
      (return 1))))

;; Pushes the full pair of copy buffers, one to each branch, and then checks the quotas. When one branch is at
;; quota the fuller becomes Dup's consumer and the other runs on its own input: its producer is itself, as C's
;; nile_Dup_body has it. Dup then stops in BLOCKED_ON_CONSUMER (1), and the kernel schedules the consumer.
(define-function nile_Dup_full int ((p nile_Process_t *) (v struct nile_Dup_vars *))
  (let (((b1 struct nile_Buffer *) (. (indir v) q1))
	((b2 struct nile_Buffer *) (. (indir v) q2))
	((p1 struct nile_Process *) (. (indir v) p1))
	((p2 struct nile_Process *) (. (indir v) p2))
	((fuller struct nile_Process *) (cast 0 struct nile_Process *))
	((other struct nile_Process *) (cast 0 struct nile_Process *)))
    (set (. (indir v) q1) (cast 0 struct nile_Buffer *))
    (set (. (indir v) q2) (cast 0 struct nile_Buffer *))
    (nile_Dup_push p p1 b1)
    (nile_Dup_push p p2 b2)
    (if (and (= 0 (nile_Process_quota_hit p1)) (= 0 (nile_Process_quota_hit p2)))
	(return 0))
    (if (> (nile_Process_input_n p1) (nile_Process_input_n p2))
	(let () (set fuller p1) (set other p2))
	(let () (set fuller p2) (set other p1)))
    (nile_Process_set_producer other other)
    (nile_Process_join p fuller)
    (nile_Process_stop_for_consumer p)
    (return 1)))

(define-function nile_Dup_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((v struct nile_Dup_vars *) (nile_Process_memory p))
	((status int) 0)
	((stop int) 0))
    (while (and (= status 0) (= stop 0))
      (while (and (< i m) (= stop 0))
	(if (= 0 (nile_Dup_open p v))
	    (return (nile_Process_return p i j NILE_STATUS_OUT_OF_MEMORY)))
	(let (((b1 struct nile_Buffer *) (. (indir v) q1))
	      ((b2 struct nile_Buffer *) (. (indir v) q2)))
	  (set (indir (nile_Buffer_data b1) (. (indir b1) tail)) (indir in i))
	  (set (indir (nile_Buffer_data b2) (. (indir b2) tail)) (indir in i))
	  (set (. (indir b1) tail) (+ (. (indir b1) tail) 1))
	  (set (. (indir b2) tail) (+ (. (indir b2) tail) 1))
	  (set i (+ i 1))
	  (if (= (. (indir b1) tail) (nile_Process_limit p))
	      (set stop (nile_Dup_full p v)))))
      (if (= stop 0)
	  (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m)))))
    (return (nile_Process_return p i j status))))

;; The last copies go to their branches. The first branch loses its producer, so it finishes when its input
;; does, and is scheduled if it is waiting; the second becomes Dup's consumer, and Dup is removed behind it.
(define-function nile_Dup_epilogue int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let* (((v struct nile_Dup_vars *) (nile_Process_memory p))
	 ((t struct nile_Thread *) (. (indir p) thread))
	 ((p1 struct nile_Process *) (. (indir v) p1))
	 ((p2 struct nile_Process *) (. (indir v) p2))
	 ((st int) 0))
    (nile_Process_flush p)
    (if (!= 0 (cast (. (indir v) q1) long))
	(let (((b1 struct nile_Buffer *) (. (indir v) q1))
	      ((b2 struct nile_Buffer *) (. (indir v) q2)))
	  (set (. (indir v) q1) (cast 0 struct nile_Buffer *))
	  (set (. (indir v) q2) (cast 0 struct nile_Buffer *))
	  (nile_Dup_push p p1 b1)
	  (nile_Dup_push p p2 b2)))
    (nile_lock (. (indir p1) lock))
    (set (. (indir p1) producer) (cast 0 struct nile_Process *))
    (set st (. (indir p1) state))
    (nile_unlock (. (indir p1) lock))
    (if (= st NILE_BLOCKED_ON_PRODUCER)
	(nile_Process_schedule_on p1 t))
    (nile_Process_join p p2)
    (return NILE_STATUS_OK)))

(define-function nile_Dup (struct nile_Process *)
  ((parent struct nile_Process *) (quantum int) (p1 struct nile_Process *) (p2 struct nile_Process *))
  (let* (((p struct nile_Process *)
	  (nile_Process parent quantum quantum (sizeof struct nile_Dup_vars)
			(cast 0 nile_Process_work_function_t)
			nile_Dup_body
			nile_Dup_epilogue)))
    (if (!= 0 (cast p long))
	(let (((v struct nile_Dup_vars *) (nile_Process_memory p)))
	  (set (. (indir v) p1) p1)
	  (set (. (indir v) p2) p2)
	  (set (. (indir v) q1) (cast 0 struct nile_Buffer *))
	  (set (. (indir v) q2) (cast 0 struct nile_Buffer *))
	  (set (. (indir p) consumer) p1)
	  (set (. (indir p1) producer) p)))
    (return p)))

;; Cat: the top branch passes its input on to the consumer of DupCat; the bottom branch
;; holds its input, in copies, until the top has finished, and then passes it on.
(define-struct nile_Cat_vars
  ((is_top   int)
   (top_done int)                   ; the bottom: the top has finished
   (other    struct nile_Process *) ; the top: the bottom, which waits for it
   (cur      struct nile_Buffer *)  ; the bottom: the buffer of copies being filled
   (first    struct nile_Buffer *)  ; the bottom: the finished buffers of copies, in order
   (last     struct nile_Buffer *)))

(define-function nile_Cat_top_body int
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

;; Adds the bottom's finished buffer of copies to the end of its list.
(define-function nile_Cat_hold void ((v struct nile_Cat_vars *) (b struct nile_Buffer *))
  (set (. (indir b) next) (cast 0 struct nile_Node *))
  (if (= 0 (cast (. (indir v) last) long))
      (set (. (indir v) first) b)
      (set (. (indir (. (indir v) last)) next) (cast b struct nile_Node *)))
  (set (. (indir v) last) b))

(define-function nile_Cat_bottom_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((v struct nile_Cat_vars *) (nile_Process_memory p))
	((limit int) (nile_Process_limit p))
	((status int) 0))
    (while (= status 0)
      (while (< i m)
	(if (or (= 0 (cast (. (indir v) cur) long)) (= limit (. (indir (. (indir v) cur)) tail)))
	    (let ()
	      (if (!= 0 (cast (. (indir v) cur) long))
		  (nile_Cat_hold v (. (indir v) cur)))
	      (let ((b (nile_Buffer p)))
		(if (= 0 (cast b long))
		    (return (nile_Process_return p i j NILE_STATUS_OUT_OF_MEMORY)))
		(set (. (indir v) cur) b))))
	(let ((b (. (indir v) cur)))
	  (set (indir (nile_Buffer_data b) (. (indir b) tail)) (indir in i))
	  (set (. (indir b) tail) (+ (. (indir b) tail) 1)))
	(set i (+ i 1)))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

;; The bottom waits for the top to finish. It gives up its thread first, then sets its state
;; under its lock; the top schedules it only when it sees that state.
(define-function nile_Cat_bottom_epilogue int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let* (((v struct nile_Cat_vars *) (nile_Process_memory p))
	 ((t struct nile_Thread *) (. (indir p) thread))
	 ((wait int) 0)
	 ((b struct nile_Buffer *) (cast 0 struct nile_Buffer *)))
    (if (!= 0 (cast (. (indir v) cur) long))
	(let ()
	  (nile_Cat_hold v (. (indir v) cur))
	  (set (. (indir v) cur) (cast 0 struct nile_Buffer *))))
    (nile_Process_deactivate p)
    (nile_lock (. (indir p) lock))
    (if (= 0 (. (indir v) top_done))
	(let ()
	  (set wait 1)
	  (set (. (indir p) state) NILE_BLOCKED_ON_GATE)))
    (nile_unlock (. (indir p) lock))
    (if (= wait 1)
	(let ()
	  (set (. (indir t) handed) 1)
	  (return NILE_STATUS_OK)))
    (nile_Process_activate p t)
    (set b (. (indir v) first))
    (set (. (indir v) first) (cast 0 struct nile_Buffer *))
    (set (. (indir v) last) (cast 0 struct nile_Buffer *))
    (while (!= 0 (cast b long))
      (let ((nx (cast (. (indir b) next) struct nile_Buffer *)))
	(nile_Process_enqueue_output p b)
	(set b nx)))
    (return NILE_STATUS_OK)))

;; The top passes its consumer on to the bottom, and wakes the bottom if it is waiting.
(define-function nile_Cat_top_epilogue int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let* (((v struct nile_Cat_vars *) (nile_Process_memory p))
	 ((t struct nile_Thread *) (. (indir p) thread))
	 ((bottom struct nile_Process *) (. (indir v) other))
	 ((bv struct nile_Cat_vars *) (cast (nile_Process_memory bottom) struct nile_Cat_vars *))
	 ((cons struct nile_Process *) (nile_Process_consumer_of p))
	 ((st int) 0))
    (if (!= 0 (cast cons long))
	(let ()
	  (nile_lock (. (indir cons) lock))
	  (set (. (indir cons) producer) bottom)
	  (nile_unlock (. (indir cons) lock))))
    (nile_lock (. (indir p) lock))
    (set (. (indir p) consumer) (cast 0 struct nile_Process *))
    (nile_unlock (. (indir p) lock))
    (nile_lock (. (indir bottom) lock))
    (set (. (indir bv) top_done) 1)
    (set st (. (indir bottom) state))
    (nile_unlock (. (indir bottom) lock))
    (if (= st NILE_BLOCKED_ON_GATE)
	(nile_Process_schedule_on bottom t))
    (return NILE_STATUS_OK)))

(define-function nile_Cat (struct nile_Process *) ((parent struct nile_Process *) (quantum int) (is_top int))
  (let* (((p struct nile_Process *) (cast 0 struct nile_Process *)))
    (if (= is_top 1)
	(set p (nile_Process parent quantum quantum (sizeof struct nile_Cat_vars)
			     (cast 0 nile_Process_work_function_t)
			     nile_Cat_top_body
			     nile_Cat_top_epilogue))
	(set p (nile_Process parent quantum quantum (sizeof struct nile_Cat_vars)
			     (cast 0 nile_Process_work_function_t)
			     nile_Cat_bottom_body
			     nile_Cat_bottom_epilogue)))
    (if (!= 0 (cast p long))
	(let (((v struct nile_Cat_vars *) (nile_Process_memory p)))
	  (set (. (indir v) is_top) is_top)
	  (set (. (indir v) top_done) 0)
	  (set (. (indir v) other) (cast 0 struct nile_Process *))
	  (set (. (indir v) cur) (cast 0 struct nile_Buffer *))
	  (set (. (indir v) first) (cast 0 struct nile_Buffer *))
	  (set (. (indir v) last) (cast 0 struct nile_Buffer *))))
    (return p)))

;; DupCat: a Dup whose second branch is a Cat's bottom and whose first is its top. The prologue
;; builds the branches and reroutes DupCat to the Dup, as C's prologue swaps it.
(define-struct nile_DupCat_vars
  ((p1 struct nile_Process *)
   (p1q int)
   (p2 struct nile_Process *)
   (p2q int)))

(define-function nile_DupCat_prologue int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let* (((v struct nile_DupCat_vars *) (nile_Process_memory p))
	 ((c1 struct nile_Process *) (nile_Cat p (. (indir v) p1q) 1))
	 ((c2 struct nile_Process *) (nile_Cat p (. (indir v) p2q) 0))
	 ((h1 struct nile_Process *) (cast 0 struct nile_Process *))
	 ((h2 struct nile_Process *) (cast 0 struct nile_Process *))
	 ((dup struct nile_Process *) (cast 0 struct nile_Process *)))
    (if (or (= 0 (cast c1 long)) (= 0 (cast c2 long)))
	(return (nile_Process_return p i j NILE_STATUS_OUT_OF_MEMORY)))
    (set h1 (if (!= 0 (cast (. (indir v) p1) long)) (nile_Process_pipe (. (indir v) p1) c1) c1))
    (set h2 (if (!= 0 (cast (. (indir v) p2) long)) (nile_Process_pipe (. (indir v) p2) c2) c2))
    (set dup (nile_Dup p (. (indir p) in_quantum) h1 h2))
    (if (= 0 (cast dup long))
	(return (nile_Process_return p i j NILE_STATUS_OUT_OF_MEMORY)))
    (set (. (indir (cast (nile_Process_memory c1) struct nile_Cat_vars *)) other) c2)
    (set (. (indir c2) consumer) (nile_Process_consumer_of p))
    (return (nile_Process_reroute p 0 0 dup))))

(define-function nile_DupCat (struct nile_Process *)
  ((parent struct nile_Process *) (quantum int)
   (p1 struct nile_Process *) (p1_out_quantum int)
   (p2 struct nile_Process *) (p2_out_quantum int))
  (let* (((p struct nile_Process *)
	  (nile_Process parent quantum quantum (sizeof struct nile_DupCat_vars)
			nile_DupCat_prologue
			(cast 0 nile_Process_work_function_t)
			(cast 0 nile_Process_work_function_t))))
    (if (!= 0 (cast p long))
	(let (((v struct nile_DupCat_vars *) (nile_Process_memory p)))
	  (set (. (indir v) p1) p1)
	  (set (. (indir v) p1q) p1_out_quantum)
	  (set (. (indir v) p2) p2)
	  (set (. (indir v) p2q) p2_out_quantum)))
    (return p)))

;; Zip: one of the two zippers of DupZip. Each writes its elements into one shared buffer at
;; its own positions, every other element's place left for its sibling, and the buffers of the
;; shared queue are handed on once both zippers have filled them. The zipper that finishes a
;; buffer second hands it on; the first to finish waits for the second, which then finishes
;; both. Positions are floats in the shared buffer: this zipper starts at j0 and stops at jn.
(define-struct nile_Zip_vars
  ((pos     int)                       ; the next float this zipper writes
   (j0      int)
   (jn      int)
   (shared  struct nile_Process *)     ; holds the queue of shared buffers
   (cur     struct nile_Buffer *)      ; the shared buffer being filled
   (sibling struct nile_Process *)
   (alive   int)))                     ; 1 until this zipper's epilogue

(define-function nile_Zip_vars_of (struct nile_Zip_vars *) ((p nile_Process_t *))
  (return (cast (nile_Process_memory p) struct nile_Zip_vars *)))

;; A zipper with no consumer of its own hands on to its sibling's, read under the shared lock as
;; nile_Zip_claim decides them.
(define-function nile_Zip_adopt void ((p nile_Process_t *) (v struct nile_Zip_vars *))
  (let (((shared struct nile_Process *) (. (indir v) shared))
	((sib struct nile_Process *) (. (indir v) sibling))
	((c struct nile_Process *) (cast 0 struct nile_Process *)))
    (nile_lock (. (indir shared) lock))
    (nile_lock (. (indir p) lock))
    (set c (. (indir p) consumer))
    (nile_unlock (. (indir p) lock))
    (if (= 0 (cast c long))
	(let ()
	  (nile_lock (. (indir sib) lock))
	  (set c (. (indir sib) consumer))
	  (nile_unlock (. (indir sib) lock))
	  (nile_lock (. (indir p) lock))
	  (set (. (indir p) consumer) c)
	  (nile_unlock (. (indir p) lock))))
    (nile_unlock (. (indir shared) lock))))

;; The shared buffer is full for this zipper. If the sibling hasn't yet queued the next one, this
;; zipper queues it and fills it next, and hands nothing on. Otherwise the sibling got there first:
;; this zipper pops the buffer both have filled and hands it to the consumer. Returns 1 when the
;; consumer is at quota, 2 when no block is free, and 0 otherwise.
(define-function nile_Zip_handoff int ((p nile_Process_t *) (v struct nile_Zip_vars *))
  (let* (((shared struct nile_Process *) (. (indir v) shared))
	 ((q struct nile_Deque *) (. (indir shared) input))
	 ((cur struct nile_Buffer *) (. (indir v) cur))
	 ((fresh struct nile_Buffer *) (nile_Buffer p))
	 ((appended int) 0)
	 ((stop int) 0))
    (if (= 0 (cast fresh long))
	(return 2))
    (set (. (indir v) pos) (. (indir v) j0))
    (nile_lock (. (indir shared) lock))
    (if (= (cast (. (indir q) tail) long) (cast cur long))
	(let ()
	  (nile_Deque_push_tail q (cast fresh struct nile_Node *))
	  (set appended 1))
	(nile_Deque_pop_head q))
    (nile_unlock (. (indir shared) lock))
    (if (= 1 appended)
	(set (. (indir v) cur) fresh)
	(let ()
	  (nile_Process_free_node p (cast fresh struct nile_Node *))
	  (set (. (indir cur) tail) (- (. (indir v) jn) (. (indir v) j0)))
	  (nile_Zip_adopt p v)
	  (nile_Process_enqueue_output p cur)
	  (nile_lock (. (indir shared) lock))
	  (set (. (indir v) cur) (cast (. (indir q) head) struct nile_Buffer *))
	  (nile_unlock (. (indir shared) lock))
	  (set stop (nile_Process_quota_hit (nile_Process_consumer_of p)))))
    (return stop)))

;; This zipper takes the consumer, its own or its sibling's, and becomes the consumer's producer; the
;; sibling's is cleared. All of it is one decision under the shared lock, which nothing takes while
;; holding a process lock: when both zippers stopped at once, each read its own consumer and cleared
;; the other's in separate sections, and both could end with none, the consumer waiting on a zipper
;; that no longer fed it (nile_Process_handle_backpressure reschedules the one left without one). A
;; sibling already blocked on the consumer this takes waits on nothing now (only its consumer would have woken
;; it, and that consumer's producer is this zipper), so it is scheduled.
(define-function nile_Zip_claim void ((p nile_Process_t *) (v struct nile_Zip_vars *))
  (let (((shared struct nile_Process *) (. (indir v) shared))
	((sib struct nile_Process *) (. (indir v) sibling))
	((orphaned int) 0)
	((c struct nile_Process *) (cast 0 struct nile_Process *)))
    (nile_lock (. (indir shared) lock))
    (nile_lock (. (indir p) lock))
    (set c (. (indir p) consumer))
    (nile_unlock (. (indir p) lock))
    (nile_lock (. (indir sib) lock))
    (if (= 0 (cast c long))
	(set c (. (indir sib) consumer)))
    (set orphaned (and (!= 0 (cast (. (indir sib) consumer) long))
		       (= NILE_BLOCKED_ON_CONSUMER (. (indir sib) state))))
    (set (. (indir sib) consumer) (cast 0 struct nile_Process *))
    (nile_unlock (. (indir sib) lock))
    (nile_lock (. (indir p) lock))
    (set (. (indir p) consumer) c)
    (nile_unlock (. (indir p) lock))
    (if (!= 0 (cast c long))
	(let ()
	  (nile_lock (. (indir c) lock))
	  (set (. (indir c) producer) p)
	  (nile_unlock (. (indir c) lock))))
    (nile_unlock (. (indir shared) lock))
    (if orphaned
	(nile_Process_schedule_on sib (. (indir p) thread)))))

(define-function nile_Zip_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let* (((v struct nile_Zip_vars *) (nile_Process_memory p))
	 ((q int) (. (indir p) in_quantum))
	 ((sq int) (. (indir (. (indir v) sibling)) in_quantum))
	 ((status int) 0)
	 ((stop int) 0))
    (if (= 0 (cast (. (indir v) cur) long))
	(let ((shared (. (indir v) shared)))
	  (nile_lock (. (indir shared) lock))
	  (set (. (indir v) cur) (cast (. (indir (. (indir shared) input)) head) struct nile_Buffer *))
	  (nile_unlock (. (indir shared) lock))))
    (while (and (= status 0) (= stop 0))
      (while (and (<= (+ i q) m) (= stop 0))
	(let ((b (. (indir v) cur))
	      (f 0))
	  (while (< f q)
	    (set (indir (nile_Buffer_data b) (+ (. (indir v) pos) f)) (indir in (+ i f)))
	    (set f (+ f 1))))
	(set i (+ i q))
	(set (. (indir v) pos) (+ (. (indir v) pos) (+ q sq)))
	(if (= (. (indir v) pos) (. (indir v) jn))
	    (let ((r (nile_Zip_handoff p v)))
	      (if (= r 2)
		  (return (nile_Process_return p i j NILE_STATUS_OUT_OF_MEMORY)))
	      (set stop r))))
      (if (= stop 0)
	  (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m)))))
    ;; the consumer is at quota, or the runtime stopped us there: it waits on this zipper, and the
    ;; sibling no longer feeds it, as C's quota handling does.
    (if (or (= stop 1) (. (indir (. (indir p) thread)) stopped))
	(let ()
	  (nile_Zip_claim p v)
	  (nile_Process_stop_for_consumer p)))
    (return (nile_Process_return p i j NILE_STATUS_OK))))

;; The second zipper to finish hands on the shared buffer that is still being filled, with its
;; length, and removes the first zipper's block and the shared one. The first to finish waits:
;; it gives up its thread and is left for the second to free.
(define-function nile_Zip_epilogue int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let* (((v struct nile_Zip_vars *) (nile_Process_memory p))
	 ((t struct nile_Thread *) (. (indir p) thread))
	 ((sib struct nile_Process *) (. (indir v) sibling))
	 ((shared struct nile_Process *) (. (indir v) shared))
	 ((sv struct nile_Zip_vars *) (nile_Zip_vars_of sib))
	 ((first int) 0)
	 ((cur struct nile_Buffer *) (cast 0 struct nile_Buffer *)))
    ;; Deactivate first, as C's nile_Zip_epilogue does: the process's heap goes to its thread whichever zipper
    ;; finished first, and the second activates for its own pair. Activating a process that still holds its heap
    ;; (the second zipper, which never deactivated) overwrote that heap and dropped its blocks. And before saying it
    ;; is no longer alive: from then the other may finish and free this block, so the first touches nothing after.
    (nile_Process_deactivate p)
    (nile_lock (. (indir shared) lock))
    (set first (. (indir sv) alive))
    (set (. (indir v) alive) 0)
    (nile_unlock (. (indir shared) lock))
    (if (= 1 first)
	(let ()
	  (set (. (indir t) handed) 1)
	  (return NILE_STATUS_OK)))
    (nile_Process_activate p t)
    (set cur (if (!= 0 (cast (. (indir v) cur) long))
		 (. (indir v) cur)
		 (cast (. (indir (. (indir shared) input)) head) struct nile_Buffer *)))
    (set (. (indir cur) tail) (- (. (indir v) pos) (. (indir v) j0)))
    (nile_lock (. (indir p) lock))
    (if (= 0 (cast (. (indir p) consumer) long))
	(set (. (indir p) consumer) (. (indir sib) consumer)))
    (nile_unlock (. (indir p) lock))
    (if (!= 0 (cast (. (indir p) consumer) long))
	(let ()
	  (nile_lock (. (indir (. (indir p) consumer)) lock))
	  (set (. (indir (. (indir p) consumer)) producer) p)
	  (nile_unlock (. (indir (. (indir p) consumer)) lock))))
    (nile_Process_free_node p (cast sib struct nile_Node *))
    (nile_Process_free_node p (cast shared struct nile_Node *))
    (nile_Process_enqueue_output p cur)
    (return NILE_STATUS_OK)))

(define-function nile_Zip (struct nile_Process *)
  ((parent struct nile_Process *) (quantum int) (j0 int) (jn int) (shared struct nile_Process *))
  (let* (((p struct nile_Process *)
	  (nile_Process parent quantum quantum (sizeof struct nile_Zip_vars)
			(cast 0 nile_Process_work_function_t)
			nile_Zip_body
			nile_Zip_epilogue)))
    (if (!= 0 (cast p long))
	(let (((v struct nile_Zip_vars *) (nile_Process_memory p)))
	  (set (. (indir v) pos) j0)
	  (set (. (indir v) j0) j0)
	  (set (. (indir v) jn) jn)
	  (set (. (indir v) shared) shared)
	  (set (. (indir v) cur) (cast 0 struct nile_Buffer *))
	  (set (. (indir v) sibling) (cast 0 struct nile_Process *))
	  (set (. (indir v) alive) 1)))
    (return p)))

;; DupZip: the input is duplicated to two branches, as Dup does, and the branches' outputs are
;; zipped: each element of the first, then the one at the same place in the second.
(define-struct nile_DupZip_vars
  ((p1 struct nile_Process *)
   (p1q int)
   (p2 struct nile_Process *)
   (p2q int)))

(define-function nile_DupZip_prologue int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let* (((v struct nile_DupZip_vars *) (nile_Process_memory p))
	 ((outq int) (+ (. (indir v) p1q) (. (indir v) p2q)))
	 ((jn int) (* (/ (nile_Buffer_capacity) outq) outq))
	 ((shared struct nile_Process *) (nile_Process p 1 1 0 (cast 0 nile_Process_work_function_t)
						      (cast 0 nile_Process_work_function_t)
						      (cast 0 nile_Process_work_function_t)))
	 ((z1 struct nile_Process *) (cast 0 struct nile_Process *))
	 ((z2 struct nile_Process *) (cast 0 struct nile_Process *))
	 ((h1 struct nile_Process *) (cast 0 struct nile_Process *))
	 ((h2 struct nile_Process *) (cast 0 struct nile_Process *))
	 ((dup struct nile_Process *) (cast 0 struct nile_Process *))
	 ((b struct nile_Buffer *) (cast 0 struct nile_Buffer *)))
    (if (= 0 (cast shared long))
	(return (nile_Process_return p i j NILE_STATUS_OUT_OF_MEMORY)))
    (set z1 (nile_Zip p (. (indir v) p1q) 0 jn shared))
    (set z2 (nile_Zip p (. (indir v) p2q) (. (indir v) p1q) (+ jn (. (indir v) p1q)) shared))
    (if (or (= 0 (cast z1 long)) (= 0 (cast z2 long)))
	(return (nile_Process_return p i j NILE_STATUS_OUT_OF_MEMORY)))
    (set h1 (if (!= 0 (cast (. (indir v) p1) long)) (nile_Process_pipe (. (indir v) p1) z1) z1))
    (set h2 (if (!= 0 (cast (. (indir v) p2) long)) (nile_Process_pipe (. (indir v) p2) z2) z2))
    (set dup (nile_Dup p (. (indir p) in_quantum) h1 h2))
    (set b (nile_Buffer p))
    (if (or (= 0 (cast dup long)) (= 0 (cast b long)))
	(return (nile_Process_return p i j NILE_STATUS_OUT_OF_MEMORY)))
    (nile_Deque_push_tail (. (indir shared) input) (cast b struct nile_Node *))
    (set (. (indir (nile_Zip_vars_of z1)) sibling) z2)
    (set (. (indir (nile_Zip_vars_of z2)) sibling) z1)
    (return (nile_Process_reroute p 0 0 dup))))

(define-function nile_DupZip (struct nile_Process *)
  ((parent struct nile_Process *) (quantum int)
   (p1 struct nile_Process *) (p1_out_quantum int)
   (p2 struct nile_Process *) (p2_out_quantum int))
  (let* (((p struct nile_Process *)
	  (nile_Process parent quantum quantum (sizeof struct nile_DupZip_vars)
			nile_DupZip_prologue
			(cast 0 nile_Process_work_function_t)
			(cast 0 nile_Process_work_function_t))))
    (if (!= 0 (cast p long))
	(let (((v struct nile_DupZip_vars *) (nile_Process_memory p)))
	  (set (. (indir v) p1) p1)
	  (set (. (indir v) p1q) p1_out_quantum)
	  (set (. (indir v) p2) p2)
	  (set (. (indir v) p2q) p2_out_quantum)))
    (return p)))

;; Every block still holding a process (a freed one's type is overwritten by the heap's link), to stderr: what a test prints
;; when a sync left blocks behind. The built-ins are named.
(define-function nile_print_live void ()
  (let (((b long) nile_blocks_from))
    (while (< (+ b BLOCK_BYTES) nile_blocks_to)
      (let (((p struct nile_Process *) (cast b struct nile_Process *)))
	(if (= NILE_PROCESS_TYPE (. (indir p) type))
	    (let (((f long) (cast (. (indir p) body) long)))
	      (dprintf 2 "# live %lx %s state %d input %d producer %lx consumer %lx epilogue %d\n" b
		       (cond ((= f (cast nile_Zip_body long)) "zip")
			     ((= f (cast nile_Dup_body long)) "dup")
			     ((= f (cast nile_Capture_body long)) "capture")
			     ((= f (cast nile_Funnel_body long)) "funnel")
			     ((= f (cast nile_Process_default_body long)) "pass")
			     ((= f 0) "none")
			     (else "kernel"))
		       (. (indir p) state) (. (indir (. (indir p) input)) n)
		       (cast (. (indir p) producer) long) (cast (. (indir p) consumer) long)
		       (!= 0 (cast (. (indir p) epilogue) long))))))
      (set b (+ b BLOCK_BYTES)))))

(export nile_print_live)
(export nile_PassThrough nile_Capture nile_Funnel nile_Funnel_pour nile_Reverse nile_SortBy nile_DupCat nile_DupZip)


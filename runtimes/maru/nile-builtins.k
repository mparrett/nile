;; The built-in processes, as nile-builtins.c, against the kernel interface: the
;; first part, PassThrough, Capture and Funnel with its pour. Reverse, SortBy,
;; DupZip and DupCat come with Task 9.

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
	((nd struct nile_Node *) (cast 0 struct nile_Node *)))
    (while (and (= NILE_BLOCKED_ON_CONSUMER (nile_Process_state_of p))
		(= NILE_STATUS_OK (nile_Thread_status_of t)))
      (nile_lock (. (indir t) lock))
      (set seen (. (indir t) ntick))
      (nile_unlock (. (indir t) lock))
      (set nd (nile_Thread_pop_own t))
      (if (= 0 (cast nd long))
	  (set nd (nile_Thread_steal_process t)))
      (if (!= 0 (cast nd long))
	  (let ()
	    (nile_Process_run (cast nd struct nile_Process *) t)
	    (nile_Thread_finished t))
	  (nile_Thread_sleep_from t seen)))))

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

(export nile_PassThrough nile_Capture nile_Funnel nile_Funnel_pour)

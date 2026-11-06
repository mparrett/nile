;; Runs recip's pipeline in a runtime with one chunk per worker and a few blocks
;; more, far too little for it. The run must set nile_error and OUT_OF_MEMORY
;; and stop without crashing (R-oom). Prints "oom ok" when it does.

(define (printf (int char * ...) *) (extern printf))
(define (malloc (void * long) *) (extern malloc))
(define (free (void void *) *) (extern free))

(define (NREALS int) 1000)
(define (NSTAGES int) 7)

;; The least startup accepts is one chunk per worker plus the records and the
;; root block, about 1.5 KB for one worker. The pipeline's working set is about
;; 20 blocks, so one chunk and a few spare blocks would not run out; this is the
;; least memory the runtime takes, and the run must exhaust it.
(define-function oom_memory int ((nthreads int))
  (return (+ 1536 (* (* 16 512) nthreads))))

(define-function test_Reciprocal_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((status int) 0)
	((r float) (cast 0.0 float)))
    (while (= status 0)
      (while (< i m)
	(set r (indir in i))
	(set i (+ i 1))
	(set r (/ (cast 1.0 float) r))
	(set r (/ (cast 1.0 float) r))
	(if (= j n)
	    (nile_Process_advance_output p (addressof out) (addressof j) (addressof n)))
	(set (indir out j) r)
	(set j (+ j 1)))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

(define-function test_Reciprocal (nile_Process_t *) ((parent nile_Process_t *))
  (return (nile_Process parent 1 1 0 0 test_Reciprocal_body 0)))

(define-function build_pipeline (nile_Process_t *)
  ((init nile_Process_t *) (out float *) (count int *))
  (let (((p nile_Process_t *) (nile_Capture init out count NREALS))
	(i 0))
    (while (< i NSTAGES)
      (set p (nile_Process_pipe (test_Reciprocal init) p))
      (set i (+ i 1)))
    (return (nile_Process_pipe (nile_Funnel init) p))))

(define-function main int ((argc int) (argv char * *))
  (let* (((nbytes int) (oom_memory 1))
	 ((mem char *) (malloc (cast nbytes long)))
	 ((init nile_Process_t *) (nile_startup mem nbytes 1))
	 ((data float *) (malloc (cast (* NREALS 4) long)))
	 ((out float *) (malloc (cast (* NREALS 4) long)))
	 ((count int) 0)
	 (i 0))
    (if (= 0 (cast init long))
	(return 1))
    (while (< i NREALS)
      (set (indir data i) (cast 2.0 float))
      (set i (+ i 1)))
    (nile_Funnel_pour (build_pipeline init out (addressof count)) data NREALS 1)
    (nile_sync init)
    (if (and (nile_error init) (= NILE_STATUS_OUT_OF_MEMORY (nile_status init)))
	(printf "oom ok\n")
      (printf "oom FAILED\n"))
    (free (nile_shutdown init))
    (free data)
    (free out)
    (return 0)))

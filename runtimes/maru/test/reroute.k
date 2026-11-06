;; Rerouting (R-reroute). A kernel passes its input through until it sees 500, then
;; reroutes to a sub-pipeline that negates. Fed 0 .. 999, the output is 0 .. 499 from
;; the kernel, then -500 .. -999 from the sub-pipeline, which takes the rest of the
;; kernel's input. Its output goes to the kernel's old consumer.
;;
;; argv[1] is the worker count, one by default. Prints "reroute ok"; check nile_reroute
;; compares the line.

(define (printf (int char * ...) *) (extern printf))
(define (malloc (void * long) *) (extern malloc))
(define (free (void void *) *) (extern free))
(define (atoi (int char *) *) (extern atoi))

(define (NREALS int) 1000)
(define (SPLIT int) 500)
(define (NBYTES_PER_THREAD int) 1000000)

;; The variables: the sub-pipeline the kernel reroutes to, built with the pipeline.
(define-struct test_Reroute_vars
  ((sub struct nile_Process *)))

(define-function test_PassUntil_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((v struct test_Reroute_vars *) (nile_Process_memory p))
	((status int) 0))
    (while (= status 0)
      (while (< i m)
	(if (= (cast (indir in i) int) SPLIT)
	    (return (nile_Process_reroute p i j (. (indir v) sub))))
	(if (= j n)
	    (nile_Process_advance_output p (addressof out) (addressof j) (addressof n)))
	(set (indir out j) (indir in i))
	(set j (+ j 1))
	(set i (+ i 1)))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

(define-function test_PassUntil (nile_Process_t *)
  ((parent nile_Process_t *) (sub nile_Process_t *))
  (let ((p (nile_Process parent 1 1 (sizeof struct test_Reroute_vars)
			 (cast 0 nile_Process_work_function_t) test_PassUntil_body
			 (cast 0 nile_Process_work_function_t))))
    (if (!= 0 (cast p long))
	(let (((v struct test_Reroute_vars *) (nile_Process_memory p)))
	  (set (. (indir v) sub) sub)))
    (return p)))

;; Negates each float it reads.
(define-function test_Negate_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let ((status 0))
    (while (= status 0)
      (while (< i m)
	(if (= j n)
	    (nile_Process_advance_output p (addressof out) (addressof j) (addressof n)))
	(set (indir out j) (- (cast 0.0 float) (indir in i)))
	(set j (+ j 1))
	(set i (+ i 1)))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

(define-function test_Negate (nile_Process_t *) ((parent nile_Process_t *))
  (return (nile_Process parent 1 1 0 (cast 0 nile_Process_work_function_t) test_Negate_body
			(cast 0 nile_Process_work_function_t))))

;; Funnel into the kernel into a Capture of NREALS floats; the sub-pipeline is built first.
(define-function build_pipeline (nile_Process_t *)
  ((init nile_Process_t *) (out float *) (count int *))
  (let* (((sub nile_Process_t *) (test_Negate init))
	 ((p nile_Process_t *) (nile_Capture init out count NREALS)))
    (set p (nile_Process_pipe (test_PassUntil init sub) p))
    (return (nile_Process_pipe (nile_Funnel init) p))))

(define-function fill_counting void ((data float *) (n int))
  (let ((k 0))
    (while (< k n)
      (set (indir data k) (cast k float))
      (set k (+ k 1)))))

;; 1 when out holds 0 .. SPLIT-1, then -SPLIT .. -(NREALS-1), and nothing else.
(define-function check_reroute int ((out float *) (count int))
  (let ((k 0)
	(ok (= count NREALS)))
    (while (and (= ok 1) (< k NREALS))
      (let ((want (if (< k SPLIT) k (- 0 k))))
	(if (!= (cast (indir out k) int) want)
	    (set ok 0)))
      (set k (+ k 1)))
    (return ok)))

(define-function main int ((argc int) (argv char * *))
  (let* (((nthreads int) (if (> argc 1) (atoi (indir argv 1)) 1))
	 ((nbytes int) (* NBYTES_PER_THREAD nthreads))
	 ((mem char *) (malloc (cast nbytes long)))
	 ((init nile_Process_t *) (nile_startup mem nbytes nthreads))
	 ((data float *) (malloc (cast (* NREALS 4) long)))
	 ((out float *) (malloc (cast (* NREALS 4) long)))
	 ((count int) 0)
	 ((failed int) 0))
    (if (= 0 (cast init long))
	(return 1))
    (fill_counting data NREALS)
    (nile_Funnel_pour (build_pipeline init out (addressof count)) data NREALS 1)
    (nile_sync init)
    (if (or (nile_error init) (= 0 (check_reroute out count)))
	(let ()
	  (set failed 1)
	  (printf "reroute FAILED: %d values\n" count))
	(printf "reroute ok\n"))
    (free (nile_shutdown init))
    (free data)
    (free out)
    (return failed)))

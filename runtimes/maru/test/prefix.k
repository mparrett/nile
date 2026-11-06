;; Pushing back at a buffer's start (R-prefix). A kernel that, for each element x
;; at a buffer's start (i == 0), pushes back x + 1000 and reads it again, so the
;; output has x + 1000 then x for those elements, and x otherwise. The kernel
;; doesn't know the buffer size: the driver finds the starts in the output, where
;; each pushed value comes out just before its x, and the starts are evenly spaced
;; from element 0.
;;
;; Then an empty stream through a kernel whose prologue emits 1 and whose epilogue
;; emits 2 (review focus 3): the output is 1 then 2, and nothing else.
;;
;; argv[1] is the worker count, one by default. Prints "prefix ok" and "empty ok";
;; check nile_prefix compares the lines.

(define (printf (int char * ...) *) (extern printf))
(define (malloc (void * long) *) (extern malloc))
(define (free (void void *) *) (extern free))
(define (atoi (int char *) *) (extern atoi))

(define (NREALS int) 1000)
(define (NBYTES_PER_THREAD int) 1000000)

;; The variables: whether the element at i == 0 has been pushed back already, so the
;; original element is read after its pushed value without being pushed again.
(define-struct test_Prefix_vars
  ((pushed int)))

(define-function test_Prefix_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((v struct test_Prefix_vars *) (nile_Process_memory p))
	((status int) 0)
	((r float) (cast 0.0 float)))
    (while (= status 0)
      (while (< i m)
	(if (and (= i 0) (= 0 (. (indir v) pushed)))
	    (let ()
	      (set r (indir in 0))
	      (nile_Process_prefix_input p (addressof in) (addressof i) (addressof m))
	      (set i (- i 1))
	      (set (indir in i) (+ r (cast 1000.0 float)))
	      (set (. (indir v) pushed) 1))
	    (let ()
	      (if (= i 0)
		  (set (. (indir v) pushed) 0))
	      (if (= j n)
		  (nile_Process_advance_output p (addressof out) (addressof j) (addressof n)))
	      (set (indir out j) (indir in i))
	      (set j (+ j 1))
	      (set i (+ i 1)))))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

(define-function test_Prefix (nile_Process_t *) ((parent nile_Process_t *))
  (let ((p (nile_Process parent 1 1 (sizeof struct test_Prefix_vars)
			 (cast 0 nile_Process_work_function_t) test_Prefix_body
			 (cast 0 nile_Process_work_function_t))))
    (if (!= 0 (cast p long))
	(let (((v struct test_Prefix_vars *) (nile_Process_memory p)))
	  (set (. (indir v) pushed) 0)))
    (return p)))

;; Copies its input, for a kernel whose prologue and epilogue are the point.
(define-function test_Copy_body int
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

;; Writes one float, value, after the output so far.
(define-function test_Emit int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int) (value float))
  (if (= j n)
      (nile_Process_advance_output p (addressof out) (addressof j) (addressof n)))
  (set (indir out j) value)
  (set j (+ j 1))
  (return (nile_Process_return p i j 0)))

(define-function test_Bracket_prologue int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (return (test_Emit p in i m out j n (cast 1.0 float))))

(define-function test_Bracket_epilogue int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (return (test_Emit p in i m out j n (cast 2.0 float))))

(define-function test_Bracket (nile_Process_t *) ((parent nile_Process_t *))
  (return (nile_Process parent 1 1 0 test_Bracket_prologue test_Copy_body test_Bracket_epilogue)))

;; Funnel into the prefix kernel into a Capture of 2 * NREALS floats.
(define-function build_prefix (nile_Process_t *)
  ((init nile_Process_t *) (out float *) (count int *))
  (let ((p (nile_Capture init out count (* 2 NREALS))))
    (set p (nile_Process_pipe (test_Prefix init) p))
    (return (nile_Process_pipe (nile_Funnel init) p))))

;; Funnel into the bracket kernel into a Capture of 8 floats.
(define-function build_bracket (nile_Process_t *)
  ((init nile_Process_t *) (out float *) (count int *))
  (let ((p (nile_Capture init out count 8)))
    (set p (nile_Process_pipe (test_Bracket init) p))
    (return (nile_Process_pipe (nile_Funnel init) p))))

(define-function fill_counting void ((data float *) (n int))
  (let ((k 0))
    (while (< k n)
      (set (indir data k) (cast k float))
      (set k (+ k 1)))))

;; 1 when out[0 .. count) is what the prefix kernel gives: each element 0 .. NREALS-1 in
;; order, with x + 1000 just before x at each start. The first start is element 0, the
;; starts are evenly spaced, there are at least three (several buffers), and nothing is
;; left over.
(define-function check_prefix int ((out float *) (count int))
  (let ((k 0)
	(x 0)
	(last -1)
	(gap -1)
	(nstarts 0)
	(ok 1))
    (while (and (= ok 1) (< k count))
      (let ((v (cast (indir out k) int)))
	(if (< v 1000)
	    (let ()
	      (if (!= v x)
		  (set ok 0))
	      (set k (+ k 1))
	      (set x (+ x 1)))
	    (let ()
	      (if (or (>= (+ k 1) count)
		      (!= (cast (indir out (+ k 1)) int) (- v 1000))
		      (!= (- v 1000) x))
		  (set ok 0)
		  (let ()
		    (if (< last 0)
			(if (!= x 0)
			    (set ok 0))
			(let ((d (- x last)))
			  (if (< gap 0)
			      (set gap d)
			      (if (!= d gap)
				  (set ok 0)))))
		    (set last x)
		    (set nstarts (+ nstarts 1))
		    (set k (+ k 2))
		    (set x (+ x 1))))))))
    (return (if (and (= ok 1) (= k count) (= x NREALS) (>= nstarts 3)) 1 0))))

(define-function main int ((argc int) (argv char * *))
  (let* (((nthreads int) (if (> argc 1) (atoi (indir argv 1)) 1))
	 ((nbytes int) (* NBYTES_PER_THREAD nthreads))
	 ((mem char *) (malloc (cast nbytes long)))
	 ((init nile_Process_t *) (nile_startup mem nbytes nthreads))
	 ((data float *) (malloc (cast (* NREALS 4) long)))
	 ((out float *) (malloc (cast (* 2 (* NREALS 4)) long)))
	 ((count int) 0)
	 ((failed int) 0))
    (if (= 0 (cast init long))
	(return 1))
    (fill_counting data NREALS)
    (nile_Funnel_pour (build_prefix init out (addressof count)) data NREALS 1)
    (nile_sync init)
    (if (or (nile_error init) (= 0 (check_prefix out count)))
	(let ()
	  (set failed 1)
	  (printf "prefix FAILED: %d values\n" count))
	(printf "prefix ok\n"))
    (set count 0)
    (nile_Funnel_pour (build_bracket init out (addressof count)) data 0 1)
    (nile_sync init)
    (if (or (nile_error init) (!= count 2)
	    (!= (cast (indir out 0) int) 1) (!= (cast (indir out 1) int) 2))
	(let ()
	  (set failed 1)
	  (printf "empty FAILED: %d values\n" count))
	(printf "empty ok\n"))
    (free (nile_shutdown init))
    (free data)
    (free out)
    (return failed)))

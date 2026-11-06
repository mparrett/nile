;; Reverse (R-reverse). Quantum 2: elements (k, -k) for k in 0 .. n-1 go through
;; Reverse into a Capture, and the output must be the elements in reverse order,
;; each still (value, -value). Two runs: n fills whole buffers exactly (ten of them,
;; which exercises the epilogue's hand-off of full buffers, review focus 5), and
;; n + 1 leaves a short last buffer.
;;
;; argv[1] is the worker count, one by default. Prints "reverse ok"; check nile_reverse
;; compares the line.

(define (printf (int char * ...) *) (extern printf))
(define (malloc (void * long) *) (extern malloc))
(define (free (void void *) *) (extern free))
(define (atoi (int char *) *) (extern atoi))

(define (QUANTUM int) 2)
(define (NBYTES_PER_THREAD int) 1000000)

(define-function build_reverse (nile_Process_t *)
  ((init nile_Process_t *) (out float *) (count int *) (size int))
  (let ((p (nile_Capture init out count size)))
    (set p (nile_Process_pipe (nile_Reverse init QUANTUM) p))
    (return (nile_Process_pipe (nile_Funnel init) p))))

;; 1 when out holds (n-1-k, -(n-1-k)) for k in 0 .. n-1, and count is 2n.
(define-function run_reverse int ((init nile_Process_t *) (n int))
  (let* (((data float *) (malloc (cast (* 2 (* n 4)) long)))
	 ((out float *) (malloc (cast (* 2 (* n 4)) long)))
	 ((count int) 0)
	 ((ok int) 1)
	 (k 0))
    (while (< k n)
      (set (indir data (* 2 k)) (cast k float))
      (set (indir data (+ (* 2 k) 1)) (- (cast 0.0 float) (cast k float)))
      (set k (+ k 1)))
    (nile_Funnel_pour (build_reverse init out (addressof count) (* 2 n)) data (* 2 n) 1)
    (nile_sync init)
    (if (or (nile_error init) (!= count (* 2 n)))
	(set ok 0))
    (set k 0)
    (while (and (= ok 1) (< k n))
      (let (((want int) (- (- n 1) k)))
	(if (or (!= (cast (indir out (* 2 k)) int) want)
		(!= (cast (indir out (+ (* 2 k) 1)) int) (- 0 want)))
	    (set ok 0)))
      (set k (+ k 1)))
    (free data)
    (free out)
    (return ok)))

(define-function main int ((argc int) (argv char * *))
  (let* (((nthreads int) (if (> argc 1) (atoi (indir argv 1)) 1))
	 ((nbytes int) (* NBYTES_PER_THREAD nthreads))
	 ((mem char *) (malloc (cast nbytes long)))
	 ((init nile_Process_t *) (nile_startup mem nbytes nthreads))
	 ((per int) (/ (nile_Buffer_capacity) QUANTUM))
	 ((failed int) 0))
    (if (= 0 (cast init long))
	(return 1))
    (if (= 0 (run_reverse init (* 10 per)))
	(set failed 1))
    (if (= 0 (run_reverse init (+ (* 10 per) 1)))
	(set failed 1))
    (if (= failed 0)
	(printf "reverse ok\n")
	(printf "reverse FAILED\n"))
    (free (nile_shutdown init))
    (return failed)))

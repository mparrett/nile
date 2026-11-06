;; DupCat and DupZip (R-dupcat), with two pass-throughs each.
;;
;; DupCat: 0 .. 999 through DupCat(PassThrough -> PassThrough, PassThrough) must come out as 0 .. 999
;; twice, in order: the top branch's output, then the bottom branch's.
;; DupZip: the same input through DupZip(PassThrough, PassThrough) must come out as
;; pairs (k, k): each element's copy from each branch, zipped element by element.
;;
;; argv[1] is the worker count, one by default. Prints "dupcat ok" and "dupzip ok";
;; check nile_dupcat compares the lines.

(define (printf (int char * ...) *) (extern printf))
(define (malloc (void * long) *) (extern malloc))
(define (free (void void *) *) (extern free))
(define (atoi (int char *) *) (extern atoi))

(define (NREALS int) 1000)
(define (NBYTES_PER_THREAD int) 1000000)

;; Funnel into the duplicator into a Capture of size floats; the sub-pipelines are built
;; first, with the parent active.
(define-function build_dup (nile_Process_t *)
  ((init nile_Process_t *) (out float *) (count int *) (size int) (zip int))
  (let* (((p1 nile_Process_t *) (nile_Process_pipe (nile_PassThrough init 1) (nile_PassThrough init 1)))
	 ((p2 nile_Process_t *) (nile_PassThrough init 1))
	 ((dup nile_Process_t *) (if zip
				     (nile_DupZip init 1 p1 1 p2 1)
				     (nile_DupCat init 1 p1 1 p2 1)))
	 ((p nile_Process_t *) (nile_Capture init out count size)))
    (set p (nile_Process_pipe dup p))
    (return (nile_Process_pipe (nile_Funnel init) p))))

(define-function fill_counting void ((data float *) (n int))
  (let ((k 0))
    (while (< k n)
      (set (indir data k) (cast k float))
      (set k (+ k 1)))))

;; Runs one pipeline over NREALS floats; returns 1 when out holds exactly want.
(define-function run_dup int ((init nile_Process_t *) (zip int))
  (let* (((data float *) (malloc (cast (* NREALS 4) long)))
	 ((out float *) (malloc (cast (* (* 2 NREALS) 4) long)))
	 ((count int) 0)
	 ((ok int) 1)
	 (k 0))
    (fill_counting data NREALS)
    (nile_Funnel_pour (build_dup init out (addressof count) (* 2 NREALS) zip) data NREALS 1)
    (nile_sync init)
    (if (or (nile_error init) (!= count (* 2 NREALS)))
	(set ok 0))
    (set k 0)
    (while (and (= ok 1) (< k (* 2 NREALS)))
      (let (((want int) (if zip (/ k 2) (% k NREALS)))
	    ((got int) 0))
	(set got (cast (indir out k) int))
	(if (!= got want)
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
	 ((failed int) 0))
    (if (= 0 (cast init long))
	(return 1))
    (if (= 0 (run_dup init 0))
	(set failed 1)
	(printf "dupcat ok\n"))
    (if (= 0 (run_dup init 1))
	(set failed 1)
	(printf "dupzip ok\n"))
    (if (!= failed 0)
	(printf "dupcat FAILED\n"))
    (free (nile_shutdown init))
    (return failed)))

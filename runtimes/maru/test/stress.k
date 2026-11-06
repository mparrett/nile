;; stress.k -- text-layout.nl's LayoutText run again and again on one runtime (R-sync-complete), as a
;; renderer runs a pipeline a frame: every round must give the points the first round gave, one per
;; glyph. A process whose producer was removed while it decided to wait on that producer read the
;; producer's freed block: it crashed, or it stayed blocked with nothing to wake it and sync returned
;; with the points missing.
;; Each round starts the runtime afresh, its workers starting while the pipeline runs, and shuts it down.
;; argv[1] is the worker count, argv[2] the rounds, argv[3] the glyphs (244 by default), from the
;; linear congruential sequence check/laws/runtime.l's rt_glyphs uses, seed 7, laid out in width 5.
;; Prints "stress ok" or how many rounds differed; built with text-layout.nl's module.

(define (printf (int char * ...) *) (extern printf))
(define (malloc (void * long) *) (extern malloc))
(define (free (void void *) *) (extern free))
(define (atoi (int char *) *) (extern atoi))

(define (NBYTES_PER_THREAD int) 1000000)

(define-function build_layout (nile_Process_t *) ((init nile_Process_t *) (out float *) (count int *) (max int))
  (let (((p nile_Process_t *) (nile_Capture init out count max)))
    (set p (nile_Process_pipe (text_layout_LayoutText init 0.0 0.0 5.0 1.0) p))
    (return (nile_Process_pipe (nile_Funnel init) p))))

;; glyph pairs (width 1..7, style 0 or 1) for seed 7, as rt_glyphs makes them
(define-function make_glyphs void ((data float *) (n int))
  (let (((x int) 7) (k 0))
    (while (< k n)
      (set x (% (+ (* 75 x) 74) 65537))
      (set (indir data (* 2 k)) (cast (+ 1 (% (/ x 10) 7)) float))
      (set (indir data (+ 1 (* 2 k))) (cast (if (< (% x 10) 3) 1 0) float))
      (set k (+ k 1)))))

;; one round: the points into out, and how many floats came out
(define-function layout_round int ((init nile_Process_t *) (data float *) (n int) (out float *))
  (let (((count int) 0))
    (nile_Funnel_pour (build_layout init out (addressof count) (* 2 n)) data (* 2 n) 1)
    (nile_sync init)
    (return count)))

(define-function main int ((argc int) (argv char * *))
  (let* (((nthreads int) (if (> argc 1) (atoi (indir argv 1)) 1))
	 ((rounds int) (if (> argc 2) (atoi (indir argv 2)) 1))
	 ((n int) (if (> argc 3) (atoi (indir argv 3)) 244))
	 ((nbytes int) (* NBYTES_PER_THREAD nthreads))
	 ((mem char *) (malloc (cast nbytes long)))
	 ((init nile_Process_t *) (cast 0 nile_Process_t *))
	 ((data float *) (malloc (cast (* (* 2 n) 4) long)))
	 ((first float *) (malloc (cast (* (* 2 n) 4) long)))
	 ((out float *) (malloc (cast (* (* 2 n) 4) long)))
	 ((count int) 0)
	 ((bad int) 0)
	 ((ok int) 0)
	 (r 0)
	 (k 0))
    (make_glyphs data n)
    (while (< r rounds)
      (set init (nile_startup mem nbytes nthreads))
      (if (= 0 (cast init long))
	  (return 1))
      (set count (layout_round init data n (if (= r 0) first out)))
      (set ok (and (= count (* 2 n)) (= 0 (nile_error init))))
      (set k 0)
      (while (and ok (> r 0) (< k (* 2 n)))
	(if (!= (indir out k) (indir first k))
	    (set ok 0))
	(set k (+ k 1)))
      (if (= ok 0)
	  (set bad (+ bad 1)))
      (nile_shutdown init)
      (set r (+ r 1)))
    (if (= bad 0)
	(printf "stress ok\n")
	(printf "stress FAILED %d of %d rounds\n" bad rounds))
    (free mem)
    (free data)
    (free first)
    (free out)
    (return (if (> bad 0) 1 0))))

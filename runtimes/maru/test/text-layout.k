;; text-layout.nl run end to end (R-text-layout): four glyphs of width 3 are laid out in a
;; width of 10 at (0, 0) with line height 1. MakeWords makes a word of three glyphs and a
;; word of one; the line breaks leave them on one line each; PlaceWords and RepeatPlacement
;; put every glyph at its word's position; PlaceGlyphs offsets each glyph along its line.
;; The points must come out as (0,0) (3,0) (6,0) (0,1), with one worker and with more.
;; argv[1] is the worker count, one by default. Prints "text-layout ok" when the points
;; match and nothing is wrong; check nile_text_layout compares the line.

(define (printf (int char * ...) *) (extern printf))
(define (malloc (void * long) *) (extern malloc))
(define (free (void void *) *) (extern free))
(define (atoi (int char *) *) (extern atoi))

(define (NGLYPHS int) 4)
(define (NBYTES_PER_THREAD int) 1000000)
(define (NOUT int) 16)

;; The pipeline: Funnel into LayoutText into a Capture of the points, built with the parent active.
(define-function build_layout (nile_Process_t *)
  ((init nile_Process_t *) (out float *) (count int *))
  (let (((p nile_Process_t *) (nile_Capture init out count (* 2 NOUT))))
    (set p (nile_Process_pipe (text_layout_LayoutText init 0.0 0.0 10.0 1.0) p))
    (return (nile_Process_pipe (nile_Funnel init) p))))

(define-function expect float ((k int) (xy int))
  (if (= k 0) (return (cast 0.0 float)))
  (if (= k 1) (return (if (= xy 0) (cast 3.0 float) (cast 0.0 float))))
  (if (= k 2) (return (if (= xy 0) (cast 6.0 float) (cast 0.0 float))))
  (return (if (= xy 0) (cast 0.0 float) (cast 1.0 float))))

(define-function main int ((argc int) (argv char * *))
  (let* (((nthreads int) (if (> argc 1) (atoi (indir argv 1)) 1))
	 ((nbytes int) (* NBYTES_PER_THREAD nthreads))
	 ((mem char *) (malloc (cast nbytes long)))
	 ((init nile_Process_t *) (nile_startup mem nbytes nthreads))
	 ((data float *) (malloc (cast (* (* 2 NGLYPHS) 4) long)))
	 ((out float *) (malloc (cast (* (* 2 NOUT) 4) long)))
	 ((count int) 0)
	 ((failed int) 0)
	 (k 0))
    (if (= 0 (cast init long))
	(return 1))
    (while (< k NGLYPHS)
      (set (indir data (* 2 k)) 3.0)
      (set (indir data (+ 1 (* 2 k))) 0.0)
      (set k (+ k 1)))
    (nile_Funnel_pour (build_layout init out (addressof count)) data (* 2 NGLYPHS) 1)
    (nile_sync init)
    (if (or (nile_error init) (!= count 8))
	(set failed 1))
    (set k 0)
    (while (and (= failed 0) (< k 4))
      (if (or (!= (indir out (* 2 k)) (expect k 0))
	      (!= (indir out (+ 1 (* 2 k))) (expect k 1)))
	  (set failed 1))
      (set k (+ k 1)))
    (if (= failed 0)
	(printf "text-layout ok\n")
	(printf "text-layout FAILED count %d\n" count))
    (free (nile_shutdown init))
    (free data)
    (free out)
    (return failed)))

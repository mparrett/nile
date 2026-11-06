;; Gezira's kernels, from gezira/nile/gezira.nl, compiled by Nile's Maru back end, render a
;; rectangle to an ARGB32 image (R-gezira-image). The rectangle is 40 by 30 at (10.5, 20), as
;; four straight quadratic Beziers written start, start, end, through TransformBeziers with a
;; translation, ClipBeziers to the image, Rasterize, ApplyTexturer over UniformColor (opaque
;; red), and WriteToImage_ARGB32 over a white background. Its left and right edges fall on half
;; pixels, so those columns are half covered; the rows are whole.
;; argv[1] is the worker count, one by default. Prints "gezira image ok" when every pixel is as
;; expected, and the same answer on any worker count; check gezira_image compares the line.

(define (printf (int char * ...) *) (extern printf))
(define (malloc (void * long) *) (extern malloc))
(define (free (void void *) *) (extern free))
(define (atoi (int char *) *) (extern atoi))

(define (WIDTH int) 64)
(define (HEIGHT int) 64)
(define (NBYTES_PER_THREAD int) 1000000)

;; The pixel as an unsigned 32-bit value, in a long.
(define-function unsigned_pixel long ((px int))
  (return (% (+ (cast px long) (* (cast 65536 long) (cast 65536 long))) (* (cast 65536 long) (cast 65536 long)))))

(define-function channel long ((u long) (shift long))
  (return (% (/ u shift) (cast 256 long))))

;; Opaque red over a point in the rectangle's interior: a = r = 255, g = b = 0.
(define-function is_full_red int ((px int))
  (let ((u (unsigned_pixel px)))
    (return (and (= 255 (channel u (cast 16777216 long)))
		 (and (= 255 (channel u (cast 65536 long)))
		      (and (= 0 (channel u (cast 256 long)))
			   (= 0 (channel u (cast 1 long)))))))))

(define-function is_white int ((px int))
  (return (= (unsigned_pixel px) (- (* (cast 65536 long) (cast 65536 long)) (cast 1 long)))))

;; A half-covered edge pixel, as gezira-image.c's blend has it for coverage 0.5 over white: coverage byte
;; 128, inverse 127, so red stays 255 and green and blue fall to 127 (ideal 127.5): 0xFFFF7F7F
;; (4294934399 = 65535 * 65536 + 32639).
(define-function is_half_edge int ((px int))
  (return (= (unsigned_pixel px) (+ (* (cast 65535 long) (cast 65536 long)) (cast 32639 long)))))

;; Funnel into the rectangle's kernels into WriteToImage_ARGB32 over the pixels; the sub-processes
;; are built with the parent active, as build_dup does.
(define-function build_image (nile_Process_t *)
  ((init nile_Process_t *) (pixels int *))
  (let* (((p nile_Process_t *) (gezira_WriteToImage_ARGB32 init pixels WIDTH HEIGHT WIDTH))
	 ((red nile_Process_t *) (gezira_UniformColor init (cast 1.0 float) (cast 0.0 float) (cast 0.0 float) (cast 1.0 float))))
    (set p (nile_Process_pipe (gezira_ApplyTexturer init red) p))
    (set p (nile_Process_pipe (gezira_Rasterize init) p))
    (set p (nile_Process_pipe (gezira_ClipBeziers init (cast 0.0 float) (cast 0.0 float) (cast 64.0 float) (cast 64.0 float)) p))
    (set p (nile_Process_pipe (gezira_TransformBeziers init (cast 1.0 float) (cast 0.0 float) (cast 0.0 float) (cast 1.0 float) (cast 10.5 float) (cast 20.0 float)) p))
    (return (nile_Process_pipe (nile_Funnel init) p))))

(define-function main int ((argc int) (argv char * *))
  (let* (((nthreads int) (if (> argc 1) (atoi (indir argv 1)) 1))
	 ((nbytes int) (* NBYTES_PER_THREAD nthreads))
	 ((mem char *) (malloc (cast nbytes long)))
	 ((init nile_Process_t *) (nile_startup mem nbytes nthreads))
	 ((pixels int *) (malloc (cast (* (* WIDTH HEIGHT) 4) long)))
	 ((data float *) (malloc (cast (* 24 4) long)))
	 ((failed int) 0)
	 ((count int) 0)
	 (k 0))
    (if (= 0 (cast init long))
	(return 1))
    (while (< k (* WIDTH HEIGHT))
      (set (indir pixels k) -1)
      (set k (+ k 1)))
    ;; Four edges, each (start, start, end), in the rectangle's own units: 6 floats each.
    (set (indir data 0) (cast 0.0 float))   (set (indir data 1) (cast 0.0 float))
    (set (indir data 2) (cast 0.0 float))   (set (indir data 3) (cast 0.0 float))
    (set (indir data 4) (cast 40.0 float))  (set (indir data 5) (cast 0.0 float))
    (set (indir data 6) (cast 40.0 float))  (set (indir data 7) (cast 0.0 float))
    (set (indir data 8) (cast 40.0 float))  (set (indir data 9) (cast 0.0 float))
    (set (indir data 10) (cast 40.0 float)) (set (indir data 11) (cast 30.0 float))
    (set (indir data 12) (cast 40.0 float)) (set (indir data 13) (cast 30.0 float))
    (set (indir data 14) (cast 40.0 float)) (set (indir data 15) (cast 30.0 float))
    (set (indir data 16) (cast 0.0 float))  (set (indir data 17) (cast 30.0 float))
    (set (indir data 18) (cast 0.0 float))  (set (indir data 19) (cast 30.0 float))
    (set (indir data 20) (cast 0.0 float))  (set (indir data 21) (cast 30.0 float))
    (set (indir data 22) (cast 0.0 float))  (set (indir data 23) (cast 0.0 float))
    (nile_Funnel_pour (build_image init pixels) data 24 1)
    (nile_sync init)
    (set k 0)
    (while (< k (* WIDTH HEIGHT))
      (if (!= (indir pixels k) -1)
	  (set count (+ count 1)))
      (set k (+ k 1)))
    (if (or (nile_error init) (!= count 1230))
	(set failed 1))
    (if (= failed 0)
	(if (or (not (is_full_red (indir pixels (+ (* 35 WIDTH) 30))))
		(or (not (is_half_edge (indir pixels (+ (* 35 WIDTH) 10))))
		    (or (not (is_half_edge (indir pixels (+ (* 35 WIDTH) 50))))
			(or (not (is_white (indir pixels (+ (* 35 WIDTH) 9))))
			    (or (not (is_white (indir pixels (+ (* 35 WIDTH) 51)))) 
				(not (is_white (indir pixels (+ (* 5 WIDTH) 5)))))))))
	    (set failed 1)))
    (if (= failed 0)
	(printf "gezira image ok\n")
	(printf "gezira image FAILED count %d\n" count))
    (free (nile_shutdown init))
    (free data)
    (free pixels)
    (return failed)))

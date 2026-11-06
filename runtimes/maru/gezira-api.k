;; The entry points an untyped caller uses to fill a Gezira image (gezira-maru.l): a runtime started and
;; stopped, an image made, cleared and read, and one path filled with a uniform paint. Built with the runtime,
;; the compiled Gezira kernels and WriteToImage_ARGB32 into build/gezira.dylib (Makefile). Handles are longs,
;; pixels are ARGB32 in C memory, a row every width pixels.

(define (malloc (void * long) *) (extern malloc))
(define (free (void void *) *) (extern free))

;; A runtime with the given number of workers, its memory from malloc; returns its init process as a long.
(define-function gezira_start long ((workers int))
  (let* (((nbytes int) (* 1000000 workers))
	 ((mem char *) (malloc (cast nbytes long))))
    (return (cast (nile_startup mem nbytes workers) long))))

;; Waits for the runtime to finish and frees its memory.
(define-function gezira_stop void ((init long))
  (free (nile_shutdown (cast init nile_Process_t *))))

;; An image of width x height pixels, cleared to 0 (transparent black); returns its address as a long.
(define-function gezira_image_new long ((width int) (height int))
  (let* (((n int) (* width height))
	 ((pixels int *) (malloc (cast (* n 4) long)))
	 (k 0))
    (while (< k n)
      (set (indir pixels k) 0)
      (set k (+ k 1)))
    (return (cast pixels long))))

(define-function gezira_image_free void ((pixels long))
  (free (cast pixels int *)))

(define-function gezira_image_clear void ((pixels long) (width int) (height int) (argb int))
  (let (((px int *) (cast pixels int *))
	(k 0))
    (while (< k (* width height))
      (set (indir px k) argb)
      (set k (+ k 1)))))

(define-function gezira_pixel int ((pixels long) (width int) (x int) (y int))
  (return (indir (cast pixels int *) (+ x (* y width)))))

;; Fills one path: params holds the matrix (a b c d e f, x' = a x + c y + e) and the uniform paint
;; (alpha red green blue); floats holds count floats, quadratic Béziers six to a path element, in the
;; image's own coordinates before the matrix. The path is clipped to width x height, which is the
;; image's size. Returns the runtime's error status (0 for none). A count that isn't a whole number of
;; elements is refused with BAD_ARG, as nile_Process_feed refuses it: the trailing floats would stay in
;; the pipeline, and the kernels' push-back past their buffer's start then writes outside it.
(define-function gezira_fill int
  ((init long) (pixels long) (width int) (height int) (params float *) (floats float *) (count int))
  (if (!= 0 (% count 6))
      (return NILE_STATUS_BAD_ARG))
  (let* (((i nile_Process_t *) (cast init nile_Process_t *))
	 ((out nile_Process_t *) (gezira_WriteToImage_ARGB32 i (cast pixels int *) width height width))
	 ((paint nile_Process_t *) (gezira_UniformColor i (indir params 7) (indir params 8) (indir params 9) (indir params 6)))
	 ((head nile_Process_t *) 0))
    (set out (nile_Process_pipe (gezira_ApplyTexturer i paint) out))
    (set out (nile_Process_pipe (gezira_Rasterize i) out))
    (set out (nile_Process_pipe (gezira_ClipBeziers i (cast 0.0 float) (cast 0.0 float) (cast width float) (cast height float)) out))
    (set out (nile_Process_pipe (gezira_TransformBeziers i (indir params 0) (indir params 1) (indir params 2)
							(indir params 3) (indir params 4) (indir params 5))
				out))
    (set head (nile_Process_pipe (nile_Funnel i) out))
    (nile_Funnel_pour head floats count 1)
    (nile_sync i)
    (return (nile_error i))))

;; the entry points, kept in the library: a unit's functions are kept only when exported
(export gezira_start)
(export gezira_stop)
(export gezira_image_new)
(export gezira_image_free)
(export gezira_image_clear)
(export gezira_pixel)
(export gezira_fill)

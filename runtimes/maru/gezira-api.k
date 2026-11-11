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
;; One path through a pipeline ending in paint, a Texturer: placed by the matrix in params[0..5], clipped by each of nstages
;; stages, clipped to width x height, rasterized, textured and written into the image.  A stage is ten floats, a b c d e
;; f then min x, min y, max x, max y: a clip rectangle in its own coordinates, and the matrix from those to the stage
;; outside it (the image's for the first), outermost first.  The path's matrix takes it into the innermost stage's
;; coordinates, so the pipeline runs as GeziraCanvas builds one for its clips interleaved with its transforms:
;; TransformBeziers (params), ClipBeziers (innermost), TransformBeziers (its matrix) ... ClipBeziers (the image).
;; Fills a path, its count whole path elements (six floats each), with paint.  The callers refuse any other count before
;; they make paint, which would otherwise hold its blocks for good.
(define-function gezira_fill_with int
  ((i nile_Process_t *) (pixels long) (width int) (height int) (params float *) (floats float *) (count int)
   (paint nile_Process_t *) (stages float *) (nstages int))
  (let* (((out nile_Process_t *) (gezira_WriteToImage_ARGB32 i (cast pixels int *) width height width))
	 ((head nile_Process_t *) 0)
	 ((k int) 0)
	 ((s float *) 0))
    (set out (nile_Process_pipe (gezira_ApplyTexturer i paint) out))
    (set out (nile_Process_pipe (gezira_Rasterize i) out))
    (set out (nile_Process_pipe (gezira_ClipBeziers i (cast 0.0 float) (cast 0.0 float) (cast width float) (cast height float)) out))
    (while (< k nstages)
      (set s (+ stages (* k 10)))
      (set out (nile_Process_pipe (gezira_TransformBeziers i (indir s 0) (indir s 1) (indir s 2) (indir s 3) (indir s 4) (indir s 5))
				  out))
      (set out (nile_Process_pipe (gezira_ClipBeziers i (indir s 6) (indir s 7) (indir s 8) (indir s 9)) out))
      (set k (+ k 1)))
    (set out (nile_Process_pipe (gezira_TransformBeziers i (indir params 0) (indir params 1) (indir params 2)
							(indir params 3) (indir params 4) (indir params 5))
				out))
    (set head (nile_Process_pipe (nile_Funnel i) out))
    (nile_Funnel_pour head floats count 1)
    (nile_sync i)
    (return (nile_error i))))

;; gezira_fill, clipped by nstages stages (gezira_fill_with).  A count that isn't whole path elements, or stages that
;; aren't there, are refused before the paint is made.
(define-function gezira_fill_clipped int
  ((init long) (pixels long) (width int) (height int) (params float *) (floats float *) (count int) (stages float *) (nstages int))
  (let (((i nile_Process_t *) (cast init nile_Process_t *)))
    (if (or (!= 0 (% count 6)) (< nstages 0) (and (< 0 nstages) (= 0 (cast stages long))))
	(return NILE_STATUS_BAD_ARG))
    (return (gezira_fill_with i pixels width height params floats count
			      (gezira_UniformColor i (indir params 7) (indir params 8) (indir params 9) (indir params 6))
			      stages nstages))))

(define-function gezira_fill int
  ((init long) (pixels long) (width int) (height int) (params float *) (floats float *) (count int))
  (return (gezira_fill_clipped init pixels width height params floats count (cast 0 float *) 0)))

;; An image's texture, as GeziraFillImage's pipelineOn:for: builds it: the points through t (a b c d e f), then read from
;; the image, through the bilinear filter unless filter is 0.
(define-function gezira_texture (nile_Process_t *)
  ((i nile_Process_t *) (t float *) (image long) (iw int) (ih int) (filter int))
  (let (((read nile_Process_t *) (gezira_ReadFromImage_ARGB32 i (cast image int *) iw ih iw)))
    (if (!= 0 filter) (set read (gezira_BilinearFilter i read)))
    (return (nile_Process_pipe (gezira_TransformPoints i (indir t 0) (indir t 1) (indir t 2) (indir t 3) (indir t 4) (indir t 5))
			       read))))

;; Fills one path with an image, clipped by nstages stages: params holds the path's matrix (0..5) and the texture's
;; (6..11), which takes a pixel of the target to a point of the image, an iw x ih image of premultiplied ARGB32.
(define-function gezira_fill_image_clipped int
  ((init long) (pixels long) (width int) (height int) (params float *) (floats float *) (count int)
   (image long) (iw int) (ih int) (filter int) (stages float *) (nstages int))
  (let (((i nile_Process_t *) (cast init nile_Process_t *)))
    (if (or (!= 0 (% count 6)) (< nstages 0) (and (< 0 nstages) (= 0 (cast stages long))))
	(return NILE_STATUS_BAD_ARG))
    (return (gezira_fill_with i pixels width height params floats count
			      (gezira_texture i (+ params 24) image iw ih filter) stages nstages))))

(define-function gezira_fill_image int
  ((init long) (pixels long) (width int) (height int) (params float *) (floats float *) (count int)
   (image long) (iw int) (ih int) (filter int))
  (return (gezira_fill_image_clipped init pixels width height params floats count image iw ih filter (cast 0 float *) 0)))

;; GeziraBindings' downsample: an iw x ih image to a new one of (iw / 2) x (ih / 2), a rectangle of that size rasterized
;; and textured from the image scaled by the sizes' ratio through the bilinear filter; answers it (0 for one too small).
(define-function gezira_downsample long ((init long) (image long) (iw int) (ih int))
  (let* (((i nile_Process_t *) (cast init nile_Process_t *))
	 ((tw int) (/ iw 2))
	 ((th int) (/ ih 2))
	 ((target long) 0)
	 ((t float *) 0)
	 ((path float *) 0)
	 ((x float) 0.0) ((y float) 0.0)
	 ((out nile_Process_t *) 0))
    (if (or (< tw 1) (< th 1)) (return 0))	; before anything is allocated
    (set t (cast (malloc (cast 24 long)) float *))
    (set path (cast (malloc (cast 96 long)) float *))
    (set target (gezira_image_new tw th))
    (set x (cast tw float))
    (set y (cast th float))
    (set (indir t 0) (/ (cast iw float) x)) (set (indir t 1) (cast 0.0 float)) (set (indir t 2) (cast 0.0 float))
    (set (indir t 3) (/ (cast ih float) y)) (set (indir t 4) (cast 0.0 float)) (set (indir t 5) (cast 0.0 float))
    ;; Rectangle>>asGeziraPath: four straight segments, each start, start, end
    (gezira_segment path 0 (cast 0.0 float) (cast 0.0 float) x (cast 0.0 float))
    (gezira_segment path 6 x (cast 0.0 float) x y)
    (gezira_segment path 12 x y (cast 0.0 float) y)
    (gezira_segment path 18 (cast 0.0 float) y (cast 0.0 float) (cast 0.0 float))
    (set out (gezira_WriteToImage_ARGB32 i (cast target int *) tw th tw))
    (set out (nile_Process_pipe (gezira_ApplyTexturer i (gezira_texture i t image iw ih 1)) out))
    (set out (nile_Process_pipe (gezira_Rasterize i) out))
    (nile_Funnel_pour (nile_Process_pipe (nile_Funnel i) out) path 24 1)
    (nile_sync i)
    (free (cast t void *))
    (free (cast path void *))
    (return target)))

(define-function gezira_segment void ((path float *) (at int) (x0 float) (y0 float) (x1 float) (y1 float))
  (set (indir path at) x0) (set (indir path (+ at 1)) y0)
  (set (indir path (+ at 2)) x0) (set (indir path (+ at 3)) y0)
  (set (indir path (+ at 4)) x1) (set (indir path (+ at 5)) y1))

;; straight alpha (a PNG's) to premultiplied (a Gezira image's), in place, each channel rounded as the writer rounds
(define-function gezira_premultiply void ((pixels long) (n int))
  (let (((px int *) (cast pixels int *)) (k 0))
    (while (< k n)
      (let* (((u long) (gezira_unsigned (indir px k)))
	     ((a int) (gezira_channel u (cast 16777216 long)))
	     ((r int) (gezira_blend (gezira_channel u (cast 65536 long)) a 0 0))
	     ((g int) (gezira_blend (gezira_channel u (cast 256 long)) a 0 0))
	     ((b int) (gezira_blend (gezira_channel u (cast 1 long)) a 0 0)))
	(set (indir px k) (cast (+ (+ (* (cast a long) (cast 16777216 long)) (* (cast r long) (cast 65536 long)))
				  (+ (* (cast g long) (cast 256 long)) (cast b long)))
				int)))
      (set k (+ k 1)))))

;; the entry points, kept in the library: a unit's functions are kept only when exported
(export gezira_start)
(export gezira_stop)
(export gezira_image_new)
(export gezira_image_free)
(export gezira_image_clear)
(export gezira_pixel)
(export gezira_fill)
(export gezira_fill_image)
(export gezira_fill_clipped)
(export gezira_fill_image_clipped)
(export gezira_downsample)
(export gezira_premultiply)

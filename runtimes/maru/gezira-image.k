;; WriteToImage_ARGB32, as gezira/c/gezira-image.c has it, in typed Maru: a consumer of
;; (Color, x, y, coverage, inverse coverage) elements, eight floats each, that composites each
;; premultiplied colour (r, g, b, alpha, as Nile's Color is laid out) over the ARGB32 pixel it lands on, in 8-bit integers. The inverse
;; coverage is read past and not used, as the C does: 255 minus the coverage byte is taken.
;; The image is caller-owned: pixels, width, height and stride in pixels.

(define-struct gezira_WriteToImage_ARGB32_vars
  ((pixels int *)
   (width  int)
   (height int)
   (stride int)))

;; A Real in [0, 1] as a byte, rounded, as Real_to_uint8_t has it.
(define-function gezira_to_byte int ((r float))
  (return (% (cast (+ (* r (cast 255.0 float)) (cast 0.5 float)) int) 256)))

;; The pixel as an unsigned 32-bit value, in a long.
(define-function gezira_unsigned long ((px int))
  (let ((two32 (* (cast 65536 long) (cast 65536 long))))
    (return (% (+ (cast px long) two32) two32))))

;; Byte shift of an unsigned pixel: 1 for blue, 256 for green, 65536 for red, 16777216 for alpha.
(define-function gezira_channel int ((u long) (shift long))
  (return (cast (% (/ u shift) (cast 256 long)) int)))

;; Source over destination, one channel: s and c are bytes of the source and its coverage,
;; d the destination byte, ic the inverse coverage; rounded as the C rounds (x + (x >> 8)) >> 8.
(define-function gezira_blend int ((s int) (c int) (d int) (ic int))
  (let ((t (+ (+ (* s c) (* d ic)) 128)))
    (return (/ (+ t (/ t 256)) 256))))

(define-function gezira_write_pixel void
  ((pixels int *) (width int) (height int) (stride int) (in float *) (i int))
  (let* (((sr int) (gezira_to_byte (indir in i)))
	 ((sg int) (gezira_to_byte (indir in (+ i 1))))
	 ((sb int) (gezira_to_byte (indir in (+ i 2))))
	 ((sa int) (gezira_to_byte (indir in (+ i 3))))
	 ((x int) (cast (indir in (+ i 4)) int))
	 ((y int) (cast (indir in (+ i 5)) int))
	 ((c int) (gezira_to_byte (indir in (+ i 6))))
	 ((ic int) (- 255 c)))
    (if (and (!= c 0) (and (<= 0 x) (and (< x width) (and (<= 0 y) (< y height)))))
	(let* (((k int) (+ x (* y stride)))
	       ((u long) (gezira_unsigned (indir pixels k)))
	       ((da int) (gezira_channel u (cast 16777216 long)))
	       ((dr int) (gezira_channel u (cast 65536 long)))
	       ((dg int) (gezira_channel u (cast 256 long)))
	       ((db int) (gezira_channel u (cast 1 long)))
	       ((a int) (gezira_blend sa c da ic))
	       ((r int) (gezira_blend sr c dr ic))
	       ((g int) (gezira_blend sg c dg ic))
	       ((b int) (gezira_blend sb c db ic))
	       ((px long) (+ (+ (* (cast a long) (cast 16777216 long)) (* (cast r long) (cast 65536 long)))
			     (+ (* (cast g long) (cast 256 long)) (cast b long)))))
	  (set (indir pixels k) (cast px int))))))

(define-function gezira_WriteToImage_ARGB32_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((v struct gezira_WriteToImage_ARGB32_vars *) (nile_Process_memory p))
	((status int) 0))
    (while (= status 0)
      (while (<= (+ i 8) m)
	(gezira_write_pixel (. (indir v) pixels) (. (indir v) width) (. (indir v) height)
			    (. (indir v) stride) in i)
	(set i (+ i 8)))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

(define-function gezira_WriteToImage_ARGB32 (struct nile_Process *)
  ((parent struct nile_Process *) (pixels int *) (width int) (height int) (stride int))
  (let* (((p struct nile_Process *)
	  (nile_Process parent 8 8 (sizeof struct gezira_WriteToImage_ARGB32_vars)
			(cast 0 nile_Process_work_function_t)
			gezira_WriteToImage_ARGB32_body
			(cast 0 nile_Process_work_function_t))))
    (if (!= 0 (cast p long))
	(let (((v struct gezira_WriteToImage_ARGB32_vars *) (nile_Process_memory p)))
	  (set (. (indir v) pixels) pixels)
	  (set (. (indir v) width) width)
	  (set (. (indir v) height) height)
	  (set (. (indir v) stride) stride)))
    (return p)))

;; ReadFromImage_ARGB32, as gezira/c/gezira-image.c has it: a Texturer, each point (x, y) to the colour of the pixel it
;; falls in, premultiplied, as Nile's Color is laid out (r, g, b, alpha), each byte over 255. A point on the right or
;; bottom edge reads the last column or row; one outside the image reads transparent black, as the C does.
(define-struct gezira_ReadFromImage_ARGB32_vars
  ((pixels int *)
   (width  int)
   (height int)
   (stride int)))

(define-function gezira_ReadFromImage_ARGB32_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let* (((v struct gezira_ReadFromImage_ARGB32_vars *) (nile_Process_memory p))
	 ((w float) (cast (. (indir v) width) float))
	 ((h float) (cast (. (indir v) height) float))
	 ((status int) 0))
    (while (= status 0)
      (while (<= (+ i 2) m)
	(let* (((x float) (indir in i))
	       ((y float) (indir in (+ i 1))))
	  (set i (+ i 2))
	  (if (= x w) (set x (- w (cast 1.0 float))))
	  (if (= y h) (set y (- h (cast 1.0 float))))
	  (let (((c long) (if (or (< x (cast 0.0 float)) (or (< y (cast 0.0 float)) (or (> x w) (> y h))))
			      (cast 0 long)
			    (gezira_unsigned (indir (. (indir v) pixels) (+ (cast x int) (* (cast y int) (. (indir v) stride))))))))
	    (if (= j n) (nile_Process_advance_output p (addressof out) (addressof j) (addressof n)))
	    (set (indir out j) (/ (cast (gezira_channel c (cast 65536 long)) float) (cast 255.0 float)))
	    (set (indir out (+ j 1)) (/ (cast (gezira_channel c (cast 256 long)) float) (cast 255.0 float)))
	    (set (indir out (+ j 2)) (/ (cast (gezira_channel c (cast 1 long)) float) (cast 255.0 float)))
	    (set (indir out (+ j 3)) (/ (cast (gezira_channel c (cast 16777216 long)) float) (cast 255.0 float)))
	    (set j (+ j 4)))))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

(define-function gezira_ReadFromImage_ARGB32 (struct nile_Process *)
  ((parent struct nile_Process *) (pixels int *) (width int) (height int) (stride int))
  (let* (((p struct nile_Process *)
	  (nile_Process parent 2 4 (sizeof struct gezira_ReadFromImage_ARGB32_vars)
			(cast 0 nile_Process_work_function_t)
			gezira_ReadFromImage_ARGB32_body
			(cast 0 nile_Process_work_function_t))))
    (if (!= 0 (cast p long))
	(let (((v struct gezira_ReadFromImage_ARGB32_vars *) (nile_Process_memory p)))
	  (set (. (indir v) pixels) pixels)
	  (set (. (indir v) width) width)
	  (set (. (indir v) height) height)
	  (set (. (indir v) stride) stride)))
    (return p)))

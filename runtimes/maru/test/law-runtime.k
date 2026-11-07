;; law-runtime.k -- one built-in or text-layout pipeline, run over integers read from stdin, for
;; check/laws/runtime.law (the laws run it through check/laws/runtime.l, one process per question).
;;
;; argv[1] is the worker count, argv[2] the mode, argv[3] the line width of the layout modes (10 by default), and
;; argv[4], when given, a schedule's seed: the threads take turns in the order it decides (nile_schedule), and a
;; last line follows the output, "contracts D R U K": buffers dropped, processes queued while running or after
;; being freed, blocks not returned after the sync, and deadlocks, each 0 for a runtime that keeps its contracts.
;;   pass, reverse, sort, dupcat, dupzip: the integers in are one stream of floats (sort takes pairs
;;       (key, payload) with field 0 the key); the built-ins run over quantum 1 (sort: 2) and print the
;;       output floats, one integer per line, as the runtime's Capture gives them.
;;   words, breaks, points: the integers in are glyph pairs (width, style) of text-layout.nl's Glyph; words
;;       prints MakeWords' Word triples (width, style, glyphs), breaks InsertLineBreaks' output of them, and
;;       points LayoutText's points (x, y), each as integers, one per line.
;; A failed run prints nothing and exits 1 (the runtime's error, no start, or no memory); a missing or unknown mode, or a
;; worker count outside 1 to 64, exits 2.

(define (printf (int char * ...) *) (extern printf))
(define (getenv (char * char *) *) (extern getenv))
(define (dprintf (int int char * ...) *) (extern dprintf))
(define (scanf (int char * ...) *) (extern scanf))
(define (malloc (void * long) *) (extern malloc))
(define (free (void void *) *) (extern free))
(define (atoi (int char *) *) (extern atoi))
(define (strcmp (int char * char *) *) (extern strcmp))
(define (exit (void int) *) (extern exit))

(define (NBYTES_PER_THREAD int) 1000000)
(define (NIN int) 65536)
(define (NOUT int) 65536)

;; text-layout.nl's processes (text_layout_MakeWords and the rest) are defined by the module
;; nile-to-maru-compiler.sh writes for it, which the check links in with this file.

;; the integers on stdin, as floats, up to max of them; returns how many
(define-function read_input int ((data float *) (max int))
  (let (((n int) 0) ((v int) 0))
    (while (and (< n max) (= 1 (scanf "%d" (addressof v))))
      (set (indir data n) (cast v float))
      (set n (+ n 1)))
    (return n)))

;; the pipeline for mode, from the Funnel's side: the Capture is its last process
(define-function build_mode (nile_Process_t *)
  ((init nile_Process_t *) (mode char *) (w int) (out float *) (count int *))
  (let ((p (nile_Capture init out count NOUT)))
    (if (= 0 (strcmp mode "pass"))
	(set p (nile_Process_pipe (nile_PassThrough init 1) p)))
    (if (= 0 (strcmp mode "reverse"))
	(set p (nile_Process_pipe (nile_Reverse init 1) p)))
    (if (= 0 (strcmp mode "sort"))
	(set p (nile_Process_pipe (nile_SortBy init 2 0) p)))
    (if (= 0 (strcmp mode "dupcat"))
	(set p (nile_Process_pipe (nile_DupCat init 1 (nile_Process_pipe (nile_PassThrough init 1) (nile_PassThrough init 1)) 1 (nile_PassThrough init 1) 1) p)))
    (if (= 0 (strcmp mode "dupzip"))
	(set p (nile_Process_pipe (nile_DupZip init 1 (nile_Process_pipe (nile_PassThrough init 1) (nile_PassThrough init 1)) 1 (nile_PassThrough init 1) 1) p)))
    (if (= 0 (strcmp mode "points"))
	(set p (nile_Process_pipe (text_layout_LayoutText init 0.0 0.0 (cast w float) 1.0) p)))
    (if (= 0 (strcmp mode "breaks"))
	(set p (nile_Process_pipe (text_layout_InsertLineBreaks init (cast w float)) p)))
    (if (or (= 0 (strcmp mode "breaks")) (= 0 (strcmp mode "words")))
	(set p (nile_Process_pipe (text_layout_MakeWords init (cast w float)) p)))
    (return (nile_Process_pipe (nile_Funnel init) p))))

(define (MAX_THREADS int) 64)	; NBYTES_PER_THREAD times it still fits nile_startup's int

(define-function known_mode int ((mode char *))
  (let (((known int) 0))
    (if (= 0 (strcmp mode "pass")) (set known 1))
    (if (= 0 (strcmp mode "reverse")) (set known 1))
    (if (= 0 (strcmp mode "sort")) (set known 1))
    (if (= 0 (strcmp mode "dupcat")) (set known 1))
    (if (= 0 (strcmp mode "dupzip")) (set known 1))
    (if (= 0 (strcmp mode "words")) (set known 1))
    (if (= 0 (strcmp mode "breaks")) (set known 1))
    (if (= 0 (strcmp mode "points")) (set known 1))
    (return known)))

;; a missing or unknown mode, or a worker count outside 1 to MAX_THREADS, exits 2; memory that can't be had, 1.  Through exit:
;; ir-gen-c.k's C main drops f_main's result and returns 0
(define-function main int ((argc int) (argv char * *))
  (if (< argc 3)
      (exit 2))
  (let* (((nthreads int) (atoi (indir argv 1)))
	 ((mode char *) (indir argv 2))
	 ((w int) (if (> argc 3) (atoi (indir argv 3)) 10))
	 ((nbytes int) 0)
	 ((mem char *) 0)
	 ((init nile_Process_t *) 0)
	 ((data float *) (malloc (cast (* NIN 4) long)))
	 ((out float *) (malloc (cast (* NOUT 4) long)))
	 ((count int) 0)
	 ((n int) 0)
	 ((failed int) 0)
	 (k 0))
    (if (or (< nthreads 1) (> nthreads MAX_THREADS) (= 0 (known_mode mode)))
	(exit 2))
    (set nbytes (* NBYTES_PER_THREAD nthreads))
    (if (> argc 4)
	(nile_schedule (cast (atoi (indir argv 4)) long)))
    (if (!= 0 (cast (getenv "NILE_TRACE") long))
	(nile_trace_on))
    (set mem (malloc (cast nbytes long)))
    (if (or (= 0 (cast mem long)) (= 0 (cast data long)) (= 0 (cast out long)))
	(exit 1))
    (set init (nile_startup mem nbytes nthreads))
    (if (= 0 (cast init long))
	(exit 1))
    (set n (read_input data NIN))
    (nile_Funnel_pour (build_mode init mode w out (addressof count)) data n 1)
    (nile_sync init)
    (if (nile_error init)
	(set failed 1))
    (if (= failed 0)
	(while (< k count)
	  (printf "%d\n" (cast (indir out k) int))
	  (set k (+ k 1))))
    (if (> argc 4)
	(let ()
	  (printf "contracts %d %d %d %d\n" (nile_dropped init) (nile_requeued init) (nile_unreturned init) (nile_deadlocks))
	  (dprintf 2 "handoffs %d\n" (nile_handoffs))
	  (if (< 0 (nile_deadlocks)) (nile_print_state init))
	  (if (< 0 (nile_unreturned init)) (nile_print_live))))
    (free (nile_shutdown init))
    (free data)
    (free out)
    (exit failed)
    (return failed)))

;; The limits a process has to respect: its variable record must fit its block
;; (R-vars-fit), an element must fit a buffer (R-element-fits), and a feed must
;; be a whole number of elements (review focus 4). Each case starts its own
;; runtime and prints one line when the limit holds; check nile_limits compares
;; the three lines.

(define (printf (int char * ...) *) (extern printf))
(define (malloc (void * long) *) (extern malloc))
(define (free (void void *) *) (extern free))

(define (NBYTES int) 100000)

(define-function vars_fit int ((init nile_Process_t *))
  (let (((q nile_Process_t *) (nile_Process init 1 1 512 0 0 0)))
    (if (and (= 0 (cast q long)) (= NILE_STATUS_BAD_ARG (nile_status init)))
	(return 1))
    (return 0)))

(define-function element_fits int ((init nile_Process_t *))
  (let (((q nile_Process_t *) (nile_Process init 200 1 0 0 0 0)))
    (if (and (= 0 (cast q long)) (= NILE_STATUS_BAD_ARG (nile_status init)))
	(return 1))
    (return 0)))

(define-function partial_element int ((init nile_Process_t *))
  (let (((data float *) (malloc (cast 20 long)))
	((p nile_Process_t *) (nile_PassThrough init 2)))
    (set (indir data 0) (cast 1.0 float))
    (set (indir data 1) (cast 2.0 float))
    (set (indir data 2) (cast 3.0 float))
    (set (indir data 3) (cast 4.0 float))
    (set (indir data 4) (cast 5.0 float))
    (nile_Process_feed p data 5)
    (return (= NILE_STATUS_BAD_ARG (nile_status init)))))

;; Runs one case in a fresh runtime and returns 1 when it holds.
(define-function case_with int ((which int))
  (let* (((mem char *) (malloc (cast NBYTES long)))
	 ((init nile_Process_t *) (nile_startup mem NBYTES 1))
	 ((ok int) 0))
    (if (= which 0) (set ok (vars_fit init)))
    (if (= which 1) (set ok (element_fits init)))
    (if (= which 2) (set ok (partial_element init)))
    (nile_sync init)
    (free (nile_shutdown init))
    (return ok)))

(define-function main int ((argc int) (argv char * *))
  (let ((failed 0))
    (if (case_with 0)
	(printf "vars-fit ok\n")
      (set failed (+ failed 1)))
    (if (case_with 1)
	(printf "element-fits ok\n")
      (set failed (+ failed 1)))
    (if (case_with 2)
	(printf "partial-element ok\n")
      (set failed (+ failed 1)))
    (return failed)))

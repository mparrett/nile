;; Starts the runtime with 1, 2 and 8 workers and with too little memory for
;; one, syncs with nothing launched, and shuts down, which must return the
;; memory it was given. Prints one line per case; check nile_startup compares
;; the lines and asks that nothing leaked (R-startup).

(define (printf (int char * ...) *) (extern printf))
(define (malloc (char * long) *) (extern malloc))

(define-function startup_case int ((nthreads int))
  (let (((memory char *) (malloc 1000000)))
    (let (((p nile_Process_t *) (nile_startup memory 1000000 nthreads)))
      (if (= 0 (cast p long))
	  (return 1)
	  (let ((ok 1))
	    (nile_sync p)
	    (if (nile_error p)
		(set ok 0))
	    (nile_print_leaks p)
	    (if (!= (cast memory long) (cast (nile_shutdown p) long))
		(set ok 0))
	    (if ok
		(printf "startup %d ok\n" nthreads)
		(printf "startup %d FAILED\n" nthreads))
	    (return (if ok 0 1)))))))

(define-function main int ((argc int) (argv char * *))
  (let ((failed 0))
    (set failed (+ failed (startup_case 1)))
    (set failed (+ failed (startup_case 2)))
    (set failed (+ failed (startup_case 8)))
    (let (((small char *) (malloc 100)))
      (if (= 0 (cast (nile_startup small 100 2) long))
	  (printf "startup small ok\n")
	  (printf "startup small FAILED\n")))
    (return failed)))

;; Prints the sizes of the pthread types as nile-sync.k's opaque structs see
;; them, on one line. Check nile_osdefs compares it with test/osdefs.c's.

(define (printf (int char * ...) *) (extern printf))

(define-function main int ((argc int) (argv char * *))
  (printf "%ld %ld %ld\n"
	  (cast (sizeof struct nile_Mutex) long)
	  (cast (sizeof struct nile_Cond) long)
	  (cast (sizeof struct nile_OSThread) long))
  (return 0))

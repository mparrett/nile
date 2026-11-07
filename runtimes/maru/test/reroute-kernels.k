;; reroute-kernels.k -- a kernel that reroutes in its body (R-reroute), for test/reroute.k and the law driver
;; (law-runtime.k's reroute mode): test_PassUntil passes its input through until it reads split, then reroutes to its
;; sub-pipeline, which takes the rest of its input; test_Negate negates each float. Nile's own → substitution
;; (Gezira's StrokeOneSide) reroutes the same way.

;; The variables: the sub-pipeline the kernel reroutes to, built with the pipeline, and the value it reroutes at.
(define-struct test_Reroute_vars
  ((sub   struct nile_Process *)
   (split int)))

(define-function test_PassUntil_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let (((v struct test_Reroute_vars *) (nile_Process_memory p))
	((status int) 0))
    (while (= status 0)
      (while (< i m)
	(if (= (cast (indir in i) int) (. (indir v) split))
	    (return (nile_Process_reroute p i j (. (indir v) sub))))
	(if (= j n)
	    (nile_Process_advance_output p (addressof out) (addressof j) (addressof n)))
	(set (indir out j) (indir in i))
	(set j (+ j 1))
	(set i (+ i 1)))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

(define-function test_PassUntil (nile_Process_t *)
  ((parent nile_Process_t *) (sub nile_Process_t *) (split int))
  (let ((p (nile_Process parent 1 1 (sizeof struct test_Reroute_vars)
			 (cast 0 nile_Process_work_function_t) test_PassUntil_body
			 (cast 0 nile_Process_work_function_t))))
    (if (!= 0 (cast p long))
	(let (((v struct test_Reroute_vars *) (nile_Process_memory p)))
	  (set (. (indir v) sub) sub)
	  (set (. (indir v) split) split)))
    (return p)))

;; Negates each float it reads.
(define-function test_Negate_body int
  ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))
  (let ((status 0))
    (while (= status 0)
      (while (< i m)
	(if (= j n)
	    (nile_Process_advance_output p (addressof out) (addressof j) (addressof n)))
	(set (indir out j) (- (cast 0.0 float) (indir in i)))
	(set j (+ j 1))
	(set i (+ i 1)))
      (set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m))))
    (return (nile_Process_return p i j status))))

(define-function test_Negate (nile_Process_t *) ((parent nile_Process_t *))
  (return (nile_Process parent 1 1 0 (cast 0 nile_Process_work_function_t) test_Negate_body
			(cast 0 nile_Process_work_function_t))))


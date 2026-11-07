;; The one definition of the runtime's interface: every call of the RFC's
;; "Public interface" and "The kernel interface" tables, with the C runtime's
;; signatures. Kernels, the runtime and its tests all compile against it.

;; struct nile_Process is defined by the runtime (nile-thread.k), where its fields are.
(define-type nile_Process_t struct nile_Process)

(define-type nile_Process_work_function_t (int
   nile_Process_t * float * int int float * int int) *)

(define (NILE_STATUS_OK int) 0)
(define (NILE_STATUS_OUT_OF_MEMORY int) 1)
(define (NILE_STATUS_BAD_ARG int) 2)
(define (NILE_STATUS_SHUTTING_DOWN int) 3)

(define (nile_startup (nile_Process_t * char *
  int int) *)
  (extern nile_startup))

(define (nile_shutdown (char * nile_Process_t *) *)
  (extern nile_shutdown))

(define (nile_sync (void nile_Process_t *) *)
  (extern nile_sync))

(define (nile_status (int nile_Process_t *) *)
  (extern nile_status))

(define (nile_error (int nile_Process_t *) *)
  (extern nile_error))

(define (nile_dropped (int nile_Process_t *) *)
  (extern nile_dropped))

(define (nile_requeued (int nile_Process_t *) *)
  (extern nile_requeued))

(define (nile_unreturned (int nile_Process_t *) *)
  (extern nile_unreturned))

;; a test's schedule: before nile_startup, the threads take turns in an order the seed decides
(define (nile_schedule (void long) *)
  (extern nile_schedule))

(define (nile_deadlocks (int void) *)
  (extern nile_deadlocks))

(define (nile_handoffs (int void) *)
  (extern nile_handoffs))

(define (nile_print_state (void nile_Process_t *) *)
  (extern nile_print_state))

(define (nile_trace_on (void void) *)
  (extern nile_trace_on))

(define (nile_print_leaks (void nile_Process_t *) *)
  (extern nile_print_leaks))

(define (nile_Process_pipe (nile_Process_t *
  nile_Process_t * nile_Process_t *) *)
  (extern nile_Process_pipe))

(define (nile_Process_feed (void
  nile_Process_t * float * int) *)
  (extern nile_Process_feed))

(define (nile_Process_gate (void
  nile_Process_t * nile_Process_t *) *)
  (extern nile_Process_gate))

(define (nile_Process_pipeline (nile_Process_t *
  nile_Process_t * * int) *)
  (extern nile_Process_pipeline))

(define (nile_Process_launch (void
  nile_Process_t * * int float * int) *)
  (extern nile_Process_launch))

(define (nile_Process (nile_Process_t *
  nile_Process_t * int int int nile_Process_work_function_t
  nile_Process_work_function_t nile_Process_work_function_t) *)
  (extern nile_Process))

(define (nile_Process_memory (void *
  nile_Process_t *) *)
  (extern nile_Process_memory))

(define (nile_Process_advance_output (void
  nile_Process_t * float * * int * int *) *)
  (extern nile_Process_advance_output))

(define (nile_Process_advance_input (int
  nile_Process_t * float * * int * int *) *)
  (extern nile_Process_advance_input))

(define (nile_Process_prefix_input (void
  nile_Process_t * float * * int * int *) *)
  (extern nile_Process_prefix_input))

(define (nile_Process_return (int
  nile_Process_t * int int int) *)
  (extern nile_Process_return))

(define (nile_Process_reroute (int
  nile_Process_t * int int nile_Process_t *) *)
  (extern nile_Process_reroute))

(define (nile_PassThrough (nile_Process_t *
  nile_Process_t * int) *)
  (extern nile_PassThrough))

(define (nile_Capture (nile_Process_t *
  nile_Process_t * float * int * int) *)
  (extern nile_Capture))

(define (nile_Reverse (nile_Process_t *
  nile_Process_t * int) *)
  (extern nile_Reverse))

(define (nile_SortBy (nile_Process_t *
  nile_Process_t * int int) *)
  (extern nile_SortBy))

(define (nile_DupZip (nile_Process_t *
  nile_Process_t * int nile_Process_t * int nile_Process_t * int) *)
  (extern nile_DupZip))

(define (nile_DupCat (nile_Process_t *
  nile_Process_t * int nile_Process_t * int nile_Process_t * int) *)
  (extern nile_DupCat))

(define (nile_Funnel (nile_Process_t *
  nile_Process_t *) *)
  (extern nile_Funnel))

(define (nile_Funnel_pour (void
  nile_Process_t * float * int int) *)
  (extern nile_Funnel_pour))

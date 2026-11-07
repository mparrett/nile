;; The runtime's synchronisation: pthreads through extern, and the opaque
;; structs that hold their objects. Sizes come from nile.osdefs.k, which
;; the build makes from nile.osdefs on the machine it runs on.

;; Opaque(ctype): a struct of longs as big as the C type, since typed Maru has
;; no arrays. Runs when the file is loaded, after nile.osdefs.k has defined
;; sizeof-ctype and alignof-ctype. The build fails rather than assume a size
;; the C type doesn't have (R-opaque-pthread).
(define-form define-opaque (name ctype)
  (let ((size  (eval (string->symbol (concat-strings "sizeof-" (symbol->string ctype)))))
	(align (eval (string->symbol (concat-strings "alignof-" (symbol->string ctype)))))
	(fields ()))
    (or (<= align 8)
	(error "opaque " ctype " needs alignment " align ", more than 8"))
    (or (= size (* 8 (/ size 8)))
	(error "opaque " ctype " is " size " bytes, not a multiple of 8"))
    (for (i 0 (/ size 8))
      (push fields (list (string->symbol (concat-strings "w" (long->string i))) 'long)))
    `(define-struct ,name ,(list-reverse! fields))))

(define-opaque nile_Mutex mutex)
(define-opaque nile_Cond cond)
(define-opaque nile_OSThread thread)

(define (pthread_mutex_init (int struct nile_Mutex * void *) *) (extern pthread_mutex_init))
(define (pthread_mutex_destroy (int struct nile_Mutex *) *) (extern pthread_mutex_destroy))
(define (pthread_mutex_lock (int struct nile_Mutex *) *) (extern pthread_mutex_lock))
(define (pthread_mutex_unlock (int struct nile_Mutex *) *) (extern pthread_mutex_unlock))

(define (pthread_cond_init (int struct nile_Cond * void *) *) (extern pthread_cond_init))
(define (pthread_cond_destroy (int struct nile_Cond *) *) (extern pthread_cond_destroy))
(define (pthread_cond_wait (int struct nile_Cond * struct nile_Mutex *) *) (extern pthread_cond_wait))
(define (pthread_cond_signal (int struct nile_Cond *) *) (extern pthread_cond_signal))
(define (pthread_cond_broadcast (int struct nile_Cond *) *) (extern pthread_cond_broadcast))

(define (pthread_create (int struct nile_OSThread * void * void * void *) *) (extern pthread_create))
;; pthread_t is a word on both hosts, so it goes by value
(define (pthread_join (int long void *) *) (extern pthread_join))

(define (pthread_mutex_trylock (int struct nile_Mutex *) *) (extern pthread_mutex_trylock))
(define (pthread_self (long void) *) (extern pthread_self))
(define (calloc (void * long long) *) (extern calloc))
(define (dprintf (int int char * ...) *) (extern dprintf))

;; ---------------------------------------------------------------- schedules, for tests
;; With nile_schedule(seed) called before nile_startup, the runtime's threads take turns: one runs at a time,
;; and at every nile_lock the turn goes to the runnable thread of highest priority. Priorities are drawn from
;; the seed when a thread starts, and at NILE_CHANGES points, also drawn from the seed, the running thread's
;; drops below every other's (PCT, Burckhardt et al., ASPLOS 2010: a bug that needs d orderings shows in a
;; run with probability at least 1/(n k^(d-1)), for n threads and k steps, where switching at random at every
;; step almost never runs one thread far ahead). A lock held elsewhere is waited for by handing the turn on,
;; never by blocking (the thread is set aside until a lock is released), and a
;; condition is waited for out of the running (nile_wait), so a schedule is a function of the seed alone:
;; a law that fails names an interleaving that runs again. No thread able to run is a deadlock: it is
;; counted (nile_deadlocks) and the schedule ends, the threads running on as the system has them. Without
;; nile_schedule, or after nile_unschedule (shutdown), the calls are pthreads' own.

(define (NILE_SLOTS int) 128)
(define (SLOT_ABSENT int) 0)
(define (SLOT_RUNNABLE int) 1)
(define (SLOT_WAITING int) 2)
(define (SLOT_DONE int) 3)
(define (SLOT_SPIN int) 4)		; failed to take a lock: set aside until one is released
(define (NILE_CHANGES int) 3)
(define (NILE_HORIZON int) 400)		; change points fall in the first this many turns

(define (nile_sched_on int))		; 1 while threads take turns
(define (nile_sched_seed long))
(define (nile_sched_turn int))		; the slot whose turn it is
(define (nile_sched_deadlocks int))
(define (nile_sched_handoffs int))	; turns passed to another thread: how much the schedule interleaved
(define (nile_sched_ids long *))		; slot -> pthread_self of its thread
(define (nile_sched_state int *))	; slot -> SLOT_*
(define (nile_sched_cond long *))	; slot -> the condition a waiting slot waits on
(define (nile_sched_prio long *))	; slot -> its priority
(define (nile_sched_change long *))	; the turns at which the running thread's priority drops
(define (nile_sched_steps long))	; turns so far
(define (nile_sched_nchanges int))	; change points in nile_sched_change
(define (nile_sched_fixed long *))	; slot -> its starting priority, when a test gives them (nile_schedule_pct)
(define (nile_sched_nfixed int))
(define (nile_sched_lock struct nile_Mutex))
(define (nile_sched_wake struct nile_Cond))

;; the seed's sequence: its next number, 0 to 32767
(define-function nile_sched_next long ()
  (set nile_sched_seed (% (+ (* nile_sched_seed 1103515245) 12345) 2147483648))
  (return (% (/ nile_sched_seed 65536) 32768)))

(define-function nile_schedule void ((seed long))
  (set nile_sched_ids (cast (calloc (cast NILE_SLOTS long) 8) long *))
  (set nile_sched_state (cast (calloc (cast NILE_SLOTS long) 4) int *))
  (set nile_sched_cond (cast (calloc (cast NILE_SLOTS long) 8) long *))
  (set nile_sched_prio (cast (calloc (cast NILE_SLOTS long) 8) long *))
  (set nile_sched_change (cast (calloc (cast NILE_CHANGES long) 8) long *))
  (pthread_mutex_init (addressof nile_sched_lock) (cast 0 void *))
  (pthread_cond_init (addressof nile_sched_wake) (cast 0 void *))
  (set nile_sched_seed seed)
  (set nile_sched_steps 0)
  (set nile_sched_handoffs 0)
  (set nile_sched_fixed (cast 0 long *))
  (set nile_sched_nchanges NILE_CHANGES)
  (let (((i int) 0))
    (while (< i NILE_CHANGES)
      (set (indir nile_sched_change i) (+ 1 (% (nile_sched_next) (cast NILE_HORIZON long))))
      (set i (+ i 1))))
  (set nile_sched_deadlocks 0)
  (set nile_sched_on 1))

;; Takes turns by a PCT schedule given in full: slot i starts at priority prios[i] (0 past np), and at each of the nc turns
;; in changes the running slot's priority drops below every other's. A law's schedule term is this, so shrinking it
;; shrinks the schedule itself, and a refutation reads as one.
(define-function nile_schedule_pct void ((prios long *) (np int) (changes long *) (nc int))
  (nile_schedule 0)
  (set nile_sched_fixed prios)
  (set nile_sched_nfixed np)
  (set nile_sched_change changes)
  (set nile_sched_nchanges nc))


;; the slot of the calling thread; -1 for one that isn't taking turns
(define-function nile_sched_me int ()
  (let (((self long) (pthread_self)) ((i int) 0) ((me int) -1))
    (while (< i NILE_SLOTS)
      (if (and (!= SLOT_ABSENT (indir nile_sched_state i)) (= self (indir nile_sched_ids i)))
	  (set me i))
      (set i (+ i 1)))
    (return me)))

;; Makes slot a runnable, before its thread starts (so when the system starts it doesn't matter), and records
;; the calling thread as the slot's when id is set.
(define-function nile_sched_slot void ((a int) (id int))
  (if nile_sched_on
      (let ()
	(pthread_mutex_lock (addressof nile_sched_lock))
	(set (indir nile_sched_state a) SLOT_RUNNABLE)
	(set (indir nile_sched_prio a) (+ 100000 (if (!= 0 (cast nile_sched_fixed long))
						      (if (< a nile_sched_nfixed) (indir nile_sched_fixed a) 0)
						      (nile_sched_next))))
	(if id
	    (let ()
	      (set (indir nile_sched_ids a) (pthread_self))
	      (set nile_sched_turn a)))
	(pthread_mutex_unlock (addressof nile_sched_lock)))))

;; Under the scheduler's lock: the runnable slot of highest priority, or, with none runnable, the set-aside
;; one of highest priority (to try its lock again); -1 for none.
(define-function nile_sched_best int ((state int))
  (let (((i int) 0) ((best int) -1))
    (while (< i NILE_SLOTS)
      (if (and (= state (indir nile_sched_state i))
	       (or (< best 0) (> (indir nile_sched_prio i) (indir nile_sched_prio best))))
	  (set best i))
      (set i (+ i 1)))
    (return best)))


;; Under the scheduler's lock: a turn passes. At a change point the running slot's priority drops below every other's;
;; then the turn goes to the slot nile_sched_best names, or, with none, a deadlock is counted and the schedule ends.
(define-function nile_sched_pass void ()
  (let (((i int) 0) ((next int) -1))
    (set nile_sched_steps (+ nile_sched_steps 1))
    (while (< i nile_sched_nchanges)
      (if (= nile_sched_steps (indir nile_sched_change i))
	  (set (indir nile_sched_prio nile_sched_turn) (- 0 nile_sched_steps)))
      (set i (+ i 1)))
    (set next (nile_sched_best SLOT_RUNNABLE))
    (if (< next 0)
	(let ()
	  (set next (nile_sched_best SLOT_SPIN))
	  (if (>= next 0) (set (indir nile_sched_state next) SLOT_RUNNABLE))))
    (if (< next 0)
	(let ()
	  (set nile_sched_deadlocks (+ nile_sched_deadlocks 1))
	  (set nile_sched_on 0))
	(let ()
	  (if (!= nile_sched_turn next) (set nile_sched_handoffs (+ nile_sched_handoffs 1)))
	  (set nile_sched_turn next)))
    (pthread_cond_broadcast (addressof nile_sched_wake))))

;; Under the scheduler's lock: every slot set aside for a lock may try again.
(define-function nile_sched_released void ()
  (let (((i int) 0))
    (while (< i NILE_SLOTS)
      (if (= SLOT_SPIN (indir nile_sched_state i)) (set (indir nile_sched_state i) SLOT_RUNNABLE))
      (set i (+ i 1)))))

;; Under the scheduler's lock: waits until it is slot me's turn and me can run, or the schedule has ended.
(define-function nile_sched_await void ((me int))
  (while (and nile_sched_on
	      (or (!= nile_sched_turn me) (!= SLOT_RUNNABLE (indir nile_sched_state me))))
    (pthread_cond_wait (addressof nile_sched_wake) (addressof nile_sched_lock))))

;; A worker's first act: it records its thread, then waits for its turn.
(define-function nile_sched_enter void ((a int))
  (if nile_sched_on
      (let ()
	(pthread_mutex_lock (addressof nile_sched_lock))
	(set (indir nile_sched_ids a) (pthread_self))
	(nile_sched_await a)
	(pthread_mutex_unlock (addressof nile_sched_lock)))))

;; A worker's last act: it leaves the turns to the others.
(define-function nile_sched_leave void ()
  (if nile_sched_on
      (let (((me int) (nile_sched_me)))
	(pthread_mutex_lock (addressof nile_sched_lock))
	(if (and (>= me 0) nile_sched_on)
	    (let ()
	      (set (indir nile_sched_state me) SLOT_DONE)
	      (if (= nile_sched_turn me) (nile_sched_pass))))
	(pthread_mutex_unlock (addressof nile_sched_lock)))))

;; The running thread hands the turn on and waits for it back.
(define-function nile_sched_yield void ()
  (let (((me int) (nile_sched_me)))
    (if (>= me 0)
	(let ()
	  (pthread_mutex_lock (addressof nile_sched_lock))
	  (if nile_sched_on
	      (let ()
		(nile_sched_pass)
		(nile_sched_await me)))
	  (pthread_mutex_unlock (addressof nile_sched_lock))))))

;; Ends the schedule (at shutdown): every thread runs on as the system has it.
(define-function nile_unschedule void ()
  (if nile_sched_on
      (let ()
	(pthread_mutex_lock (addressof nile_sched_lock))
	(set nile_sched_on 0)
	(pthread_cond_broadcast (addressof nile_sched_wake))
	(pthread_mutex_unlock (addressof nile_sched_lock)))))

(define-function nile_deadlocks int ()
  (return nile_sched_deadlocks))

(define-function nile_handoffs int ()
  (return nile_sched_handoffs))

(define-function nile_lock void ((m struct nile_Mutex *))
  (if nile_sched_on
      (let (((got int) 0) ((me int) (nile_sched_me)))
	(nile_sched_yield)
	(while (and nile_sched_on (= got 0))
	  (set got (= 0 (pthread_mutex_trylock m)))
	  (if (and (= got 0) (>= me 0))
	      (let ()		; held elsewhere: set aside until a lock is released
		(pthread_mutex_lock (addressof nile_sched_lock))
		(if nile_sched_on
		    (let ()
		      (set (indir nile_sched_state me) SLOT_SPIN)
		      (nile_sched_pass)
		      (nile_sched_await me)))
		(pthread_mutex_unlock (addressof nile_sched_lock)))))
	(if (= got 0)
	    (pthread_mutex_lock m)))
      (pthread_mutex_lock m)))

;; Takes m if it is free and answers 1, or answers 0 at once: a lock taken out of order can't wait, so it can't make a cycle.
(define-function nile_trylock int ((m struct nile_Mutex *))
  (if nile_sched_on (nile_sched_yield))
  (return (= 0 (pthread_mutex_trylock m))))

(define-function nile_unlock void ((m struct nile_Mutex *))
  (pthread_mutex_unlock m)
  (if nile_sched_on
      (let ()
	(pthread_mutex_lock (addressof nile_sched_lock))
	(nile_sched_released)
	(pthread_mutex_unlock (addressof nile_sched_lock)))))

;; Under a schedule the waiter leaves the running until signalled; a signal makes one waiter on c runnable
;; again, the first by slot, and a broadcast all of them.
(define-function nile_wait void ((c struct nile_Cond *) (m struct nile_Mutex *))
  (let (((me int) (if nile_sched_on (nile_sched_me) -1)))
    (if (< me 0)
	(pthread_cond_wait c m)
	(let ()
	  (pthread_mutex_lock (addressof nile_sched_lock))
	  (set (indir nile_sched_state me) SLOT_WAITING)
	  (set (indir nile_sched_cond me) (cast c long))
	  (pthread_mutex_unlock (addressof nile_sched_lock))
	  (pthread_mutex_unlock m)
	  (pthread_mutex_lock (addressof nile_sched_lock))
	  (if nile_sched_on
	      (let ()
		(if (= nile_sched_turn me) (nile_sched_pass))
		(nile_sched_await me)))
	  (set (indir nile_sched_state me) SLOT_RUNNABLE)
	  (pthread_mutex_unlock (addressof nile_sched_lock))
	  (nile_lock m)))))

(define-function nile_sched_signal void ((c struct nile_Cond *) (all int))
  (let (((i int) 0) ((done int) 0))
    (pthread_mutex_lock (addressof nile_sched_lock))
    (while (and (< i NILE_SLOTS) (= done 0))
      (if (and (= SLOT_WAITING (indir nile_sched_state i)) (= (cast c long) (indir nile_sched_cond i)))
	  (let ()
	    (set (indir nile_sched_state i) SLOT_RUNNABLE)
	    (if (= all 0) (set done 1))))
      (set i (+ i 1)))
    (pthread_mutex_unlock (addressof nile_sched_lock))))

(define-function nile_signal void ((c struct nile_Cond *))
  (if nile_sched_on
      (nile_sched_signal c 0)
      (pthread_cond_signal c)))

(define-function nile_broadcast void ((c struct nile_Cond *))
  (if nile_sched_on
      (nile_sched_signal c 1)
      (pthread_cond_broadcast c)))

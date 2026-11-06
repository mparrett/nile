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

(define-function nile_lock void ((m struct nile_Mutex *))
  (pthread_mutex_lock m))

(define-function nile_unlock void ((m struct nile_Mutex *))
  (pthread_mutex_unlock m))

(define-function nile_wait void ((c struct nile_Cond *) (m struct nile_Mutex *))
  (pthread_cond_wait c m))

(define-function nile_signal void ((c struct nile_Cond *))
  (pthread_cond_signal c))

(define-function nile_broadcast void ((c struct nile_Cond *))
  (pthread_cond_broadcast c))

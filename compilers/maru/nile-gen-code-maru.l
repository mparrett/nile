(nile-load "nile-gen-code-env.l")
(nile-load "nile-builtins-maru.l")

(define nile-set-cvars-from-primed)

(define-function nile-set-cvars (vars vals env)
  (list-do (var val) (zip (flatten vars) (flatten vals))
    (out env "(set "var" "val")\n")))

(define-function nile-set-cvars-if (vars vals cond-val env)
  (list-do (var val) (zip (flatten vars) (flatten vals))
    (out env "(set "var" (if "cond-val" "val" "var"))\n")))

(define-function nile-gen-code-for-processinst (full-name arg-vals result-var env)
  (out env "(set "result-var" ("full-name" p")
    (list-do val arg-vals (out-no-indent env " " val))
    (out-no-indent env "))\n"))
  
(define-method alloc-cvars <nile-primtype> (env)
  (let ((type (if (= self.name "Number") "float" "int")))
    (list (alloc-cvar env type))))

(define-method alloc-cvars <nile-processtype> (env)
  (list (alloc-cvar env "nile_Process_t *")))

(define-method gen-code <nile-numexpr> (env)
  (list (format "(cast %f float32)" (string->double self.number))))

(define-method gen-code <nile-pipeline> (env)
  (let ((process-var (gen-code self.process env)))
    (if self.downstream
      (let ((downstream-var (gen-code self.downstream env)))
        (out env "(set "process-var" (nile_Process_pipe "process-var" "downstream-var"))\n")))
    process-var))

(define-method gen-code <nile-substmt> (env)
  (let ((pipeline-var (gen-code self.pipeline env)))
    (out env "(return (nile_Process_reroute p i j "pipeline-var"))\n")))

; An if's branches each bind their own primed values. After the if, each primed
; value is copied into fresh variables: the branch's when the condition holds,
; else the other branch's. A branch that doesn't set it keeps the unprimed value.
(define-function nile-binding-named (bindings wanted)
  (if bindings
    (if (= wanted (name (car (car bindings))))
      (cdr (car bindings))
      (nile-binding-named (cdr bindings) wanted))))

(define-function nile-entry-named (bindings wanted)
  (if bindings
    (if (= wanted (name (car (car bindings))))
      (car bindings)
      (nile-entry-named (cdr bindings) wanted))))

(define-function nile-cvar-type-in (cvs cvar)
  (if cvs
    (if (= cvar (car (car cvs)))
      (cdr (car cvs))
      (nile-cvar-type-in (cdr cvs) cvar))))

; Shapes match: then, else and ref (the unprimed value) hold the same cvars' layout. A list
; merges element by element (its cdr is a list too, so the walk is the same call); a leaf is a cvar.
(define-function nile-merge-cvals (env cval then-cvals else-cvals ref-cvals)
  (if (pair? then-cvals)
    (cons (nile-merge-cvals env cval (car then-cvals) (car else-cvals) (car ref-cvals))
          (nile-merge-cvals env cval (cdr then-cvals) (cdr else-cvals) (cdr ref-cvals)))
    (if then-cvals
      (let ((merged (alloc-cvar env (nile-cvar-type-in (cvars env) ref-cvals))))
        (out env "(set "merged" (if "cval" "then-cvals" "else-cvals"))\n")
        merged))))

(define-function nile-unique-names (names)
  (let ((out ()))
    (list-do n names (or (member? n out) (push out n)))
    (list-reverse! out)))

(define-function nile-merge-primed (env cval before then-b else-b)
  (let ((entries (filter (lambda (b) (is-primed? (car b))) (concat-list then-b else-b))))
    (list-do pname (nile-unique-names (map (lambda (b) (name (car b))) entries))
      (let* ((vardecl (car (nile-entry-named entries pname)))
             (ref     (nile-binding-named before (unprimed-name vardecl)))
             (then-c  (or (nile-binding-named then-b pname) ref))
             (else-c  (or (nile-binding-named else-b pname) ref)))
        (bind env vardecl (nile-merge-cvals env cval then-c else-c ref))))))

(define-method gen-code <nile-ifstmt> (env)
  (let* ((cval   (car (gen-code self.condition env)))
         (before (bindings env)))
    (out env "(if "cval"\n")
    (push-indent env)
      (out env "(let ()\n")
      (push-indent env) (gen-code self.tblock env) (pop-indent env)
      (out env ")\n")
      (let ((then-b (bindings env)))
        (set-bindings env before)
        (out env "(let ()\n")
        (push-indent env) (gen-code self.fblock env) (pop-indent env)
        (out env ")\n")
        (let ((else-b (bindings env)))
          (pop-indent env)
          (out env ")\n")
          (set-bindings env before)
          (nile-merge-primed env cval before then-b else-b)))))

(define-method gen-code <nile-instmt> (env)
  (list-do value self.values
    (let ((vals (gen-code value env)))
      (out env "(if (= i 0) (nile_Process_prefix_input p (addressof in) (addressof i) (addressof m)))\n")
      (list-do val (list-reverse (flatten vals))
        (out env "(set i (- i 1))\n")
        (out env "(set (indir in i) "val")\n"))))) ; TODO what if val is int (bool)?

(define-method gen-code <nile-outstmt> (env)
  (list-do value (list-reverse self.values)
    (let ((vals (gen-code value env)))
      (out env "(if (= j n) (nile_Process_advance_output p (addressof out) (addressof j) (addressof n)))\n")
      (list-do val (flatten vals)
        (out env "(set (indir out j) "val")\n") ; TODO what if val is int (bool)?
        (out env "(set j (+ j 1))\n")))))

(define-method gen-code <nile-processbody> (env)
  (out env "(while (= status 0)\n")
  (push-indent env)
    (out env "(while (< i m)\n")
    (push-indent env)
      (list-do var (flatten (gen-code self.forpat env))
        (out env "(set "var" (indir in i))\n")
        (out env "(set i (+ i 1))\n"))

      (gen-code self.block env)

      (nile-set-cvars-from-primed env)
    (pop-indent env)
    (out env ")\n")
    (out env "(set status (nile_Process_advance_input p (addressof in) (addressof i) (addressof m)))\n")
  (pop-indent env)
  (out env ")\n"))

(define-method gen-code-for-vars-struct <nile-processdef> (cname env)
  (let* ((_vardecls (++ (params self.sig) (vardecls self.prologue)))
         (env_      (clone env (string-port)))
         (_         (map-with gen-code _vardecls env_))
         (all-cvars (list-reverse (cvars env_))))
    (out env "(define-struct "cname"_vars (\n")
    (push-indent env)
      (list-do (name . type) all-cvars
        (out env "("name" "type")\n"))
    (pop-indent env)
    (out env "))\n\n")))

; The size of the variable struct gen-code-for-vars-struct writes, or 0 when
; the kernel has no variables. The constructor is written before the
; variables are allocated, so this counts them the way the struct does.
(define-method vars-sizeof <nile-processdef> (cname env)
  (let* ((_vardecls (++ (params self.sig) (vardecls self.prologue)))
         (env_      (clone env (string-port)))
         (_         (map-with gen-code _vardecls env_)))
    (if (cvars env_)
      (++ (++ "(sizeof struct " cname) "_vars)")
      "0")))

(define-method gen-code-for-constructor <nile-processdef> (cname env)
  (out env "(define-function "cname" (nile_Process_t *) (\n")
  (push-indent env)
    (out env "(parent nile_Process_t *)")
    (list-do (name . type) (list-reverse (cvars env))
      (out-no-indent env "\n")
      (out env "("name" "type")"))
    (out-no-indent env ")\n\n")
    (out env "(let* (((p nile_Process_t *) (nile_Process parent "
             (csize (intype (type self)))" "(csize (outtype (type self)))" "
             (vars-sizeof self cname env)" "
             (if (empty? self.prologue) "0 " (++ cname "_prologue "))
             (if (not self.body)        "0 " (++ cname "_body "))
             (if (empty? self.epilogue) "0"  (++ cname "_epilogue"))"))\n")
    (out env "       ((vars struct "cname"_vars *) (nile_Process_memory p)))\n")
    (push-indent env)
      (out env "(if p (let ()\n")
      (push-indent env)
        (list-do (name . _) (list-reverse (cvars env))
          (out env "(set (. vars "name") "name")\n"))
      (pop-indent env)
      (out env "))\n")
    (pop-indent env)
    (out env "(return p)))\n\n")
  (pop-indent env)
  (out env "(export "cname")\n\n"))

; TODO maru format %s doesn't seem to work
(define-function nile-gen-work-function-sig (name)
  (++ (++ "(define-function "name) " (int)
    ((p nile_Process_t *) (in float *) (i int) (m int) (out float *) (j int) (n int))"))

; TODO ask Ian if define-function in hajimaru w/out no body could be a decl?
(define-function nile-gen-work-function-decl (function-name)
  (++ (nile-gen-work-function-sig function-name) "(return 0))\n\n"))

; Used to compile process prologues, bodies and epilogues
(define-function nile-gen-code-for-work-function (ast-node cname suffix _env)
  (let* ((env       (clone _env (out-port _env)))
         (env_      (clone  env (string-port)))
         (_         (gen-code ast-node env_))
         (all-cvars (list-reverse (cvars env_))))
    (println "Compiling work function: " cname suffix)
    (out env (nile-gen-work-function-sig (++ cname suffix))"\n\n")
    (push-indent env)
      (out env "(let (\n")
        (push-indent env) (push-indent env)
          (out env "((status int) 0)\n")
          (out env "((vars struct "cname"_vars *) (nile_Process_memory p))\n")
          (list-do (name . type) all-cvars
            (out env "(("name" "type"))\n"))
        (pop-indent env) (pop-indent env)
      (out env "    )\n")
      (push-indent env)
        (list-do (name . _) (list-reverse (cvars env))
          (out env "(set "name" (. vars "name"))\n"))

        (gen-code ast-node env)

        (list-do (vardecl . cvars) (list-reverse (bindings _env))
          (list-do (cvar val) (zip (flatten cvars) (flatten (lookup env (name vardecl))))
            (out env "(set (. vars "cvar") "val")\n")))
        (out env "(return (nile_Process_return p i j status))))\n\n")
      (pop-indent env)
    (pop-indent env)))

; For now, don't generate a header file for maru
(define-function nile-gen-code-for-header-file (ast module-name))

(define-function nile-gen-code-for-source-file (ast module-name)
  (let ((source-file-name (++ module-name ".k"))
        (env              (nile-gen-code-env module-name)))
    (println "Source file name: "source-file-name)
    (out env "(define (sqrtf (float float) *) (extern sqrtf))\n\n")
    (map-with gen-code ast env)
    (contents-to-file-named (port-contents (out-port env)) source-file-name)))

(nile-load "nile-gen-code.l")

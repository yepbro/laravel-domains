// Package bgjoin — a structural check of the invariant that background
// loops of the service run function are stopped (T-223 — the http-worker;
// T-232 — render-worker, analyzer, dns-worker): all background loops of
// run are stopped and joined before the defer resource closings on any
// exit — a normal ctx cancellation, an early return from the constructor,
// a panic. The scheme is derived cancellation (context.WithCancel) +
// sync.WaitGroup: Add(1)/defer bg.Done() at the launch site of each loop,
// the join defer "cancel(); bg.Wait()" is registered last among run's
// defers → executed first (LIFO), before the resource closings. Running
// run in a unit test is impossible (live Postgres/Redis/Chromium), so the
// invariant is enforced by parsing main.go via go/ast (a precedent of
// tests reading sources — test/integration/compose_test.go,
// deploy/postgres/postgresql_conf_test.go). The parser was extracted from
// the TestRunBackgroundJoin of the http-worker (T-223) and hardened per
// the T-232 review (cancel-before-Wait order, binding the derived ctx to
// the loops, an orphan Add, defers in nested blocks).
//
// Package contract (P3-2/P3-4 of the T-232 review): the package is a test
// one — import only from _test.go files; the identifier names bg (the
// WaitGroup), cancel (the derived cancellation) and ctx (the derived
// context, package context) are part of the verifiable T-223 pattern
// contract, not an implementation detail: the pattern fixes them
// identically in all services, the name check catches refactors that
// silently disable the fix. Every go launch in run is a joinable ctx
// loop: a func literal with an unconditional top-level defer bg.Done(),
// using the derived ctx; one-off goroutines in run are forbidden (launch
// from a joinable loop), launching a named function is wrapped in a
// literal with defer bg.Done().
package bgjoin

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// Check verifies the run shutdown invariant in the source file of the
// service (the path relative to the package directory: go test runs with
// cwd = the package directory, as in postgresql_conf_test.go):
//
//   - run derives the ctx via context.WithCancel and passes the derived
//     ctx to every loop and to the worker's main loop: the framework
//     cancels the ctx OnRun only after its return; an early exit/panic
//     must stop the loops themselves; creating WithCancel without binding
//     (loops on the parent ctx) would silently disable the fix
//     (P2-2 of the T-232 review);
//   - the join defer (the only defer with bg.Wait) is a func literal with
//     an unconditional top-level cancel() BEFORE bg.Wait() (Wait-before-
//     cancel and a conditional cancel mean a deadlock on early exit,
//     P2-1 of the T-232 review), registered last among run's defers →
//     executed first (LIFO), before the resource closings (including
//     defers in nested blocks, P3-3) and before the first loop launch;
//   - every go launch in run is wrapped in the pair "bg.Add(1); go
//     func(){ defer bg.Done(); … }()": an Add without a go pair is an
//     unjoined loop (a T-223/T-232 regression), an orphan Add without go
//     hangs Wait (P3-1), defer bg.Done must be an unconditional
//     top-level statement of the literal — a conditional Done (inside
//     an if) would leave Wait hanging.
func Check(t *testing.T, file string) {
	t.Helper()
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", file, err)
	}
	var run *ast.FuncDecl
	for _, d := range parsed.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Name.Name == "run" && fn.Recv == nil {
			run = fn
		}
	}
	if run == nil {
		t.Fatalf("the run function not found in %s", file)
	}

	var (
		withCancel = token.NoPos // ctx, cancel := context.WithCancel(ctx)
	ctxName    string        // identifier of the derived ctx (LHS[0])
	cancelName = "cancel"    // identifier of the cancellation (LHS[1]; the pattern contract)
		joinDefer  *ast.DeferStmt
		waits      int // bg.Wait() in run — exactly one
		launches   int
		firstGo    = token.NoPos
		allDefers  []*ast.DeferStmt // run's defers outside the loop goroutines
		broken     []string
	)

	// The join defer — by the top level of run (the contract): the defers
	// of the loop goroutines and callback closures do not execute on
	// run's exit and do not participate in the LIFO of closings.
	for _, st := range run.Body.List {
		if d, ok := st.(*ast.DeferStmt); ok {
			if callsBgWait(d) {
				if joinDefer != nil {
					broken = append(broken, "more than one defer with bg.Wait()")
				}
				joinDefer = d
			}
		}
	}

	// The closing order — over ALL of run's defers outside goroutines: a
	// defer in a nested block (if/for/…) also executes on function exit
	// and, by registration site, joins the same LIFO (P3-3 of the T-232
	// review); the bodies of func literals (the join defer, callbacks)
	// are not walked — their inner statements are not run's statements.
	walkStmts(run.Body.List, func(st ast.Stmt) {
		if d, ok := st.(*ast.DeferStmt); ok {
			allDefers = append(allDefers, d)
		}
	})

	// Binding of the derived cancellation (P2-2) — a separate walk BEFORE
	// parsing the launches: run's statement list is visited earlier than
	// the child statements; in a combined walk the go-launch checks would
	// run with an empty ctxName.
	ast.Inspect(run.Body, func(n ast.Node) bool {
		if a, ok := n.(*ast.AssignStmt); ok && len(a.Lhs) == 2 && len(a.Rhs) == 1 && withCancel == token.NoPos {
			if call, ok := a.Rhs[0].(*ast.CallExpr); ok && isPkgCall(call, "context", "WithCancel") {
				withCancel = a.Pos()
				if id, ok := a.Lhs[0].(*ast.Ident); ok && id.Name != "_" && id.Name != "" {
					ctxName = id.Name
				}
				if id, ok := a.Lhs[1].(*ast.Ident); ok && id.Name != "_" && id.Name != "" {
					cancelName = id.Name
				}
			}
		}
		return true
	})

	ast.Inspect(run.Body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.BlockStmt:
			// The pairs "bg.Add(1); go …" and "go …; bg.Add(1)" — in every
			// statement list (the loop launches sit at the top level of
			// run, but the check does not depend on the level).
			for i, st := range n.List {
				if goSt, ok := st.(*ast.GoStmt); ok {
					launches++
					if firstGo == token.NoPos {
						firstGo = goSt.Pos()
					}
					if i == 0 || !isBgAddOne(n.List[i-1]) {
						broken = append(broken, "a go launch without bg.Add(1) right before it — the loop is outside the join (T-223/T-232)")
					}
					if !defersBgDoneTopLevel(goSt) {
						broken = append(broken, "a go launch without an unconditional top-level defer bg.Done() of the literal")
					}
					if ctxName != "" && !mentionsIdent(goSt, ctxName) {
						broken = append(broken, "a go launch does not use the derived ctx "+ctxName+" — the join-defer cancel will not stop it (T-232)")
					}
					continue
				}
				// An orphan Add: Wait would hang on the "unfinished" counter
				// on any exit, including the normal one (P3-1 of the
				// T-232 review — realistic when a loop is removed).
				if isBgAddOne(st) && (i+1 >= len(n.List) || !isGoStmt(n.List[i+1])) {
					broken = append(broken, "bg.Add(1) without a go launch right after it — bg.Wait() will hang (T-232)")
				}
			}
		case *ast.CallExpr:
			if isMethodCall(n, "bg", "Wait") {
				waits++
			}
		}
		return true
	})

	if withCancel == token.NoPos {
		t.Fatalf("%s: run has no derived cancel (context.WithCancel) — an early exit/panic leaves the loops with a live ctx", file)
	}
	if joinDefer == nil {
		t.Fatalf("%s: run has no defer with bg.Wait() at the top level — the background loops are not joined before the defer resource closes", file)
	}
	// The join defer: an unconditional cancel() BEFORE bg.Wait() (P2-1 of
	// the T-232 review): Wait-before-cancel and a conditional cancel mean
	// a deadlock on early exit (the loops are not cancelled, Wait will not
	// return; on a normal stop it is masked by the already-cancelled parent).
	if err := joinDeferOrder(joinDefer, cancelName); err != "" {
		t.Errorf("%s: join-defer: %s", file, err)
	}
	if waits != 1 {
		t.Errorf("%s: bg.Wait() calls in run = %d, want 1 (the join-defer only)", file, waits)
	}
	if launches == 0 {
		t.Fatalf("%s: run has no go launches of background loops — nothing to check", file)
	}
	// The derived ctx goes to the worker's main loop: the final return with
	// a loop call must pass it (P2-2 of the T-232 review); a return without
	// a call (return err/nil) is not the final loop, not checked.
	if ctxName != "" {
		if last := len(run.Body.List) - 1; last >= 0 {
			if ret, ok := run.Body.List[last].(*ast.ReturnStmt); ok && hasCallExpr(ret) && !mentionsIdent(ret, ctxName) {
				t.Errorf("%s: the final return does not pass the derived ctx %s to the main loop — stopping run will not extinguish it (T-232)", file, ctxName)
			}
		}
	}
	// The join defer is registered last among run's defers → executed
	// first (LIFO): any defer resource closing (top level or a nested
	// block) already sees the stopped loops.
	for _, d := range allDefers {
		if d == joinDefer {
			continue
		}
		if d.Pos() > joinDefer.Pos() {
			t.Errorf("%s: the defer (%s) is registered after the join — LIFO will close its resource before the loops stop", file, fset.Position(d.Pos()))
		}
	}
	if withCancel > joinDefer.Pos() {
		t.Errorf("%s: the derived cancel comes after the join-defer — the loops may get a non-cancellable ctx", file)
	}
	if firstGo != token.NoPos && firstGo < joinDefer.Pos() {
		t.Errorf("%s: the loop's go launch comes before the join-defer registration — a path between them exits run without a join", file)
	}
	for _, b := range broken {
		t.Error(b)
	}
}

// joinDeferOrder is a structural check of the join defer body (P2-1 of the
// T-232 review): defer func(){ … }() with an unconditional "cancel()" and
// a top-level "bg.Wait()" of the literal, cancel BEFORE Wait. Returns an
// empty string when the order is correct, otherwise the essence of the
// violation.
func joinDeferOrder(d *ast.DeferStmt, cancelName string) string {
	lit, ok := d.Call.Fun.(*ast.FuncLit)
	if !ok {
		return "a func literal «cancel(); bg.Wait()» expected, not a named call"
	}
	cancelPos, waitPos := token.NoPos, token.NoPos
	for _, st := range lit.Body.List {
		expr, ok := st.(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := expr.X.(*ast.CallExpr)
		if !ok {
			continue
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == cancelName {
			if cancelPos == token.NoPos {
				cancelPos = st.Pos() // the first unconditional cancel
			}
		}
		if isMethodCall(call, "bg", "Wait") && waitPos == token.NoPos {
			waitPos = st.Pos()
		}
	}
	switch {
	case cancelPos == token.NoPos:
		return "no unconditional " + cancelName + "() at the top level — an early exit of run will not stop the still-live loops, Wait will hang"
	case waitPos == token.NoPos:
		return "no top-level bg.Wait() of the literal"
	case waitPos < cancelPos:
		return "bg.Wait() comes before " + cancelName + "() — uncancelled loops will not exit, Wait will hang (an early-exit deadlock)"
	}
	return ""
}

// walkStmts walks statements, descending into the bodies of block
// constructs (if/else, for, range, switch, select, case branches), without
// entering func literal bodies: the defers of closures and goroutines do
// not execute on run's exit and do not participate in run's LIFO of
// closings.
func walkStmts(stmts []ast.Stmt, fn func(ast.Stmt)) {
	for _, st := range stmts {
		fn(st)
		switch st := st.(type) {
		case *ast.BlockStmt:
			walkStmts(st.List, fn)
		case *ast.IfStmt:
			walkStmts(st.Body.List, fn)
			switch e := st.Else.(type) {
			case *ast.BlockStmt:
				walkStmts(e.List, fn)
			case *ast.IfStmt:
				walkStmts([]ast.Stmt{e}, fn)
			}
		case *ast.ForStmt:
			walkStmts(st.Body.List, fn)
		case *ast.RangeStmt:
			walkStmts(st.Body.List, fn)
		case *ast.SwitchStmt:
			walkStmts(st.Body.List, fn)
		case *ast.TypeSwitchStmt:
			walkStmts(st.Body.List, fn)
		case *ast.SelectStmt:
			walkStmts(st.Body.List, fn)
		case *ast.CaseClause:
			walkStmts(st.Body, fn)
		case *ast.CommClause:
			walkStmts(st.Body, fn)
		}
	}
}

// callsBgWait — is there a bg.Wait() call in the subtree?
func callsBgWait(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		if call, ok := m.(*ast.CallExpr); ok && isMethodCall(call, "bg", "Wait") {
			found = true
		}
		return true
	})
	return found
}

// mentionsIdent — is the identifier name mentioned in the subtree?
func mentionsIdent(n ast.Node, name string) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		if id, ok := m.(*ast.Ident); ok && id.Name == name {
			found = true
		}
		return true
	})
	return found
}

// hasCallExpr — is there a function call in the subtree?
func hasCallExpr(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		if _, ok := m.(*ast.CallExpr); ok {
			found = true
		}
		return true
	})
	return found
}

// isGoStmt — a goroutine launch statement.
func isGoStmt(st ast.Stmt) bool {
	_, ok := st.(*ast.GoStmt)
	return ok
}

// isBgAddOne — a "bg.Add(1)" statement.
func isBgAddOne(st ast.Stmt) bool {
	expr, ok := st.(*ast.ExprStmt)
	if !ok {
		return false
	}
	call, ok := expr.X.(*ast.CallExpr)
	if !ok || !isMethodCall(call, "bg", "Add") || len(call.Args) != 1 {
		return false
	}
	lit, ok := call.Args[0].(*ast.BasicLit)
	return ok && lit.Kind == token.INT && lit.Value == "1"
}

// defersBgDoneTopLevel — the go launch must be a func literal with an
// unconditional "defer bg.Done()" statement at the top level of the body:
// a Done inside if/for would leave Wait hanging on an "unfinished" loop.
func defersBgDoneTopLevel(goSt *ast.GoStmt) bool {
	lit, ok := goSt.Call.Fun.(*ast.FuncLit)
	if !ok {
		return false
	}
	for _, st := range lit.Body.List {
		d, ok := st.(*ast.DeferStmt)
		if !ok {
			continue
		}
		if call, ok := d.Call.Fun.(*ast.SelectorExpr); ok {
			if id, ok := call.X.(*ast.Ident); ok && id.Name == "bg" && call.Sel.Name == "Done" {
				return true
			}
		}
	}
	return false
}

// isPkgCall — a pkg.Fn(…) call by package and function name.
func isPkgCall(call *ast.CallExpr, pkg, fn string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg && sel.Sel.Name == fn
}

// isMethodCall — a recv.Method(…) call by receiver and method name.
func isMethodCall(call *ast.CallExpr, recv, method string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == recv && sel.Sel.Name == method
}

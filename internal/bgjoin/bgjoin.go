// Package bgjoin — структурная проверка инварианта остановки фоновых
// циклов функции run сервисов (T-223 — http-worker; T-232 —
// render-worker, analyzer, dns-worker): все фоновые циклы run
// останавливаются и джойнятся до defer-закрытий ресурсов на любом
// выходе — штатной отмене ctx, раннем возврате из конструктора, панике.
// Схема — производная отмена (context.WithCancel) + sync.WaitGroup:
// Add(1)/defer bg.Done() по месту запуска каждого цикла, джойн-defer
// «cancel(); bg.Wait()» зарегистрирован последним из defer run →
// исполняется первым (LIFO), до закрытий ресурсов. Прогнать run в
// юнит-тесте нельзя (живые Postgres/Redis/Chromium), инвариант
// закрепляется разбором main.go через go/ast (прецедент чтения
// исходников тестами — test/integration/compose_test.go,
// deploy/postgres/postgresql_conf_test.go). Разборщик извлечён из
// TestRunBackgroundJoin http-worker (T-223) и усилен по ревью T-232
// (порядок cancel-до-Wait, привязка производного ctx к циклам, сиротский
// Add, defer во вложенных блоках).
//
// Контракт пакета (P3-2/P3-4 ревью T-232): пакет тестовый — импортировать
// только из _test.go-файлов; имена идентификаторов bg (WaitGroup),
// cancel (производная отмена) и ctx (производный контекст, пакет
// context) — часть проверяемого контракта паттерна T-223, а не деталь
// реализации: паттерн фиксирует их одинаково во всех сервисах, проверка
// на имена ловит рефакторинги, молча отключающие фикс. Каждый go-запуск
// в run — джойнимый ctx-цикл: func-литерал с безусловным defer bg.Done()
// верхнего уровня, использующий производный ctx; одноразовые горутины в
// run запрещены (запускать из джойнимого цикла), запуск именованной
// функции оборачивается литералом с defer bg.Done().
package bgjoin

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// Check проверяет инвариант остановки run в исходнике file сервиса
// (путь относительно каталога пакета: go test запускается с cwd =
// каталог пакета, как у postgresql_conf_test.go):
//
//   - run выводит ctx производной отменой (context.WithCancel) и
//     передаёт производный ctx каждому циклу и главному циклу воркера:
//     каркас отменяет ctx OnRun только после его возврата, ранний
//     выход/паника обязаны гасить циклы сами; создание WithCancel без
//     привязки (циклы на родительском ctx) отключало бы фикс молча
//     (P2-2 ревью T-232);
//   - джойн-defer (единственный defer с bg.Wait) — func-литерал с
//     безусловным cancel() верхнего уровня ДО bg.Wait() (Wait-до-cancel
//     и условный cancel — дедлок на раннем выходе, P2-1 ревью T-232),
//     зарегистрирован последним из defer run → исполняется первым
//     (LIFO), до закрытий ресурсов (включая defer во вложенных блоках,
//     P3-3) и до первого запуска циклов;
//   - каждый go-запуск в run обёрнут парой «bg.Add(1); go func(){ defer
//     bg.Done(); … }()»: Add без пары к go — незаджойненный цикл
//     (регрессия T-223/T-232), сиротский Add без go вешает Wait (P3-1),
//     defer bg.Done обязан быть безусловным оператором верхнего уровня
//     литерала — условный Done (внутри if) оставил бы Wait виснуть.
func Check(t *testing.T, file string) {
	t.Helper()
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("разбор %s: %v", file, err)
	}
	var run *ast.FuncDecl
	for _, d := range parsed.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Name.Name == "run" && fn.Recv == nil {
			run = fn
		}
	}
	if run == nil {
		t.Fatalf("функция run в %s не найдена", file)
	}

	var (
		withCancel = token.NoPos // ctx, cancel := context.WithCancel(ctx)
		ctxName    string        // идентификатор производного ctx (LHS[0])
		cancelName = "cancel"    // идентификатор отмены (LHS[1]; контракт паттерна)
		joinDefer  *ast.DeferStmt
		waits      int // bg.Wait() в run — ровно один
		launches   int
		firstGo    = token.NoPos
		allDefers  []*ast.DeferStmt // defer-ы run вне горутин циклов
		broken     []string
	)

	// Джойн-defer — по верхнему уровню run (контракт): defer-ы горутин
	// циклов и замыканий-колбэков исполняются не на выходе run и в LIFO
	// закрытий не участвуют.
	for _, st := range run.Body.List {
		if d, ok := st.(*ast.DeferStmt); ok {
			if callsBgWait(d) {
				if joinDefer != nil {
					broken = append(broken, "больше одного defer с bg.Wait()")
				}
				joinDefer = d
			}
		}
	}

	// Порядок закрытий — по ВСЕМ defer-ам run вне горутин: defer во
	// вложенном блоке (if/for/…) тоже исполняется на выходе из функции
	// и по месту регистрации встаёт в тот же LIFO (P3-3 ревью T-232);
	// тела func-литералов (джойн-defer, колбэки) не обходим — их
	// внутренние операторы — не операторы run.
	walkStmts(run.Body.List, func(st ast.Stmt) {
		if d, ok := st.(*ast.DeferStmt); ok {
			allDefers = append(allDefers, d)
		}
	})

	// Привязка производной отмены (P2-2) — отдельным обходом ДО разбора
	// запусков: список операторов run посещается раньше дочерних
	// операторов, в общем обходе проверка go-запусков шла бы с пустым
	// ctxName.
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
			// Пары «bg.Add(1); go …» и «go …; bg.Add(1)» — в каждом
			// списке операторов (запуски циклов лежат на верхнем
			// уровне run, но проверка не зависит от уровня).
			for i, st := range n.List {
				if goSt, ok := st.(*ast.GoStmt); ok {
					launches++
					if firstGo == token.NoPos {
						firstGo = goSt.Pos()
					}
					if i == 0 || !isBgAddOne(n.List[i-1]) {
						broken = append(broken, "go-запуск без bg.Add(1) непосредственно перед ним — цикл вне джойна (T-223/T-232)")
					}
					if !defersBgDoneTopLevel(goSt) {
						broken = append(broken, "go-запуск без безусловного defer bg.Done() верхнего уровня литерала")
					}
					if ctxName != "" && !mentionsIdent(goSt, ctxName) {
						broken = append(broken, "go-запуск не использует производный ctx "+ctxName+" — отмена джойн-defer его не остановит (T-232)")
					}
					continue
				}
				// Сиротский Add: Wait повиснет на «незавершённом»
				// счётчике любого выхода, включая штатный (P3-1
				// ревью T-232 — реалистично при удалении цикла).
				if isBgAddOne(st) && (i+1 >= len(n.List) || !isGoStmt(n.List[i+1])) {
					broken = append(broken, "bg.Add(1) без go-запуска непосредственно после него — bg.Wait() повиснет (T-232)")
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
		t.Fatalf("%s: в run нет производной отмены (context.WithCancel) — ранний выход/паника оставляют циклы с живым ctx", file)
	}
	if joinDefer == nil {
		t.Fatalf("%s: в run нет defer с bg.Wait() на верхнем уровне — фоновые циклы не джойнятся до defer-закрытий ресурсов", file)
	}
	// Джойн-defer: безусловный cancel() ДО bg.Wait() (P2-1 ревью
	// T-232): Wait-до-cancel и условный cancel — дедлок на раннем
	// выходе (циклы не отменены, Wait не вернётся; на штатной остановке
	// замаскировано уже отменённым родителем).
	if err := joinDeferOrder(joinDefer, cancelName); err != "" {
		t.Errorf("%s: джойн-defer: %s", file, err)
	}
	if waits != 1 {
		t.Errorf("%s: вызовов bg.Wait() в run = %d, хочу 1 (только джойн-defer)", file, waits)
	}
	if launches == 0 {
		t.Fatalf("%s: в run нет go-запусков фоновых циклов — проверять нечего", file)
	}
	// Производный ctx — главному циклу воркера: финальный return с
	// вызовом цикла обязан передавать его (P2-2 ревью T-232); return
	// без вызова (return err/nil) — не финальный цикл, не проверяется.
	if ctxName != "" {
		if last := len(run.Body.List) - 1; last >= 0 {
			if ret, ok := run.Body.List[last].(*ast.ReturnStmt); ok && hasCallExpr(ret) && !mentionsIdent(ret, ctxName) {
				t.Errorf("%s: финальный return не передаёт производный ctx %s главному циклу — остановка run не погасит его (T-232)", file, ctxName)
			}
		}
	}
	// Джойн-defer зарегистрирован последним из defer run → исполняется
	// первым (LIFO): любой defer-закрытие ресурса (верхний уровень или
	// вложенный блок) уже видит остановленные циклы.
	for _, d := range allDefers {
		if d == joinDefer {
			continue
		}
		if d.Pos() > joinDefer.Pos() {
			t.Errorf("%s: defer (%s) зарегистрирован после джойна — LIFO закроет его ресурс раньше остановки циклов", file, fset.Position(d.Pos()))
		}
	}
	if withCancel > joinDefer.Pos() {
		t.Errorf("%s: производная отмена стоит после джойн-defer — циклы могут получить неотменяемый ctx", file)
	}
	if firstGo != token.NoPos && firstGo < joinDefer.Pos() {
		t.Errorf("%s: go-запуск цикла стоит до регистрации джойн-defer — путь между ними выходит из run без джойна", file)
	}
	for _, b := range broken {
		t.Error(b)
	}
}

// joinDeferOrder — структурная проверка тела джойн-defer (P2-1 ревью
// T-232): defer func(){ … }() с безусловным «cancel()» и «bg.Wait()»
// верхнего уровня литерала, cancel ДО Wait. Возвращает пустую строку,
// если порядок верен, иначе — суть нарушения.
func joinDeferOrder(d *ast.DeferStmt, cancelName string) string {
	lit, ok := d.Call.Fun.(*ast.FuncLit)
	if !ok {
		return "ожидался func-литерал «cancel(); bg.Wait()», не именованный вызов"
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
				cancelPos = st.Pos() // первый безусловный cancel
			}
		}
		if isMethodCall(call, "bg", "Wait") && waitPos == token.NoPos {
			waitPos = st.Pos()
		}
	}
	switch {
	case cancelPos == token.NoPos:
		return "нет безусловного " + cancelName + "() верхнего уровня — ранний выход run не остановит ещё живые циклы, Wait зависнет"
	case waitPos == token.NoPos:
		return "нет bg.Wait() верхнего уровня литерала"
	case waitPos < cancelPos:
		return "bg.Wait() стоит до " + cancelName + "() — не отмененные циклы не выйдут, Wait повиснет (дедлок раннего выхода)"
	}
	return ""
}

// walkStmts обходит операторы со спуском в тела блочных конструкций
// (if/else, for, range, switch, select, case-ветки), не входя в тела
// func-литералов: defer-ы замыканий и горутин исполняются не на выходе
// из run и в LIFO закрытий run не участвуют.
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

// callsBgWait — есть ли в поддереве вызов bg.Wait().
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

// mentionsIdent — упоминается ли идентификатор name в поддереве.
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

// hasCallExpr — есть ли в поддереве вызов функции.
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

// isGoStmt — оператор запуска горутины.
func isGoStmt(st ast.Stmt) bool {
	_, ok := st.(*ast.GoStmt)
	return ok
}

// isBgAddOne — оператор «bg.Add(1)».
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

// defersBgDoneTopLevel — go-запуск обязан быть func-литералом с
// безусловным оператором «defer bg.Done()» на верхнем уровне тела:
// Done внутри if/for оставил бы Wait виснуть на «незавершённом» цикле.
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

// isPkgCall — вызов pkg.Fn(…) по имени пакета и функции.
func isPkgCall(call *ast.CallExpr, pkg, fn string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg && sel.Sel.Name == fn
}

// isMethodCall — вызов recv.Method(…) по именам получателя и метода.
func isMethodCall(call *ast.CallExpr, recv, method string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == recv && sel.Sel.Name == method
}

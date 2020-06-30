package builders

import "go/ast"

type (
	// describes a function so that we can do minimal checks
	CallFunctionDescriber struct {
		FunctionName                ast.Expr
		MinimumNumberOfArguments    int  /* strict number of arguments, unless indicated that it can expand */
		ExtensibleNumberOfArguments bool /* number of arguments can expand */
	}
)

func makeFunc(n ast.Expr, m int, e bool) CallFunctionDescriber {
	return CallFunctionDescriber{n, m, e}
}

func (c CallFunctionDescriber) checkArgsCount(a int) {
	if c.MinimumNumberOfArguments > a {
		panic("the minimum number of arguments has not been reached")
	}
	if !c.ExtensibleNumberOfArguments && a > c.MinimumNumberOfArguments {
		panic("the maximum number of arguments exceeded")
	}
}

var (
	// make(...)
	MakeFn = makeFunc(ast.NewIdent("make"), 1, true)
	// len(...)
	LengthFn = makeFunc(ast.NewIdent("len"), 1, false)
	// append(...)
	AppendFn = makeFunc(ast.NewIdent("append"), 1, true)

	// strconv.Itoa
	ConvertItoaFn = makeFunc(SimpleSelector("strconv", "Itoa"), 1, false)
	// strings.EqualFold
	EqualFoldFn = makeFunc(SimpleSelector("strings", "EqualFold"), 2, false)
	// strings.ToLower
	ToLowerFn = makeFunc(SimpleSelector("strings", "ToLower"), 1, false)
	// strings.Join
	StringsJoinFn = makeFunc(SimpleSelector("strings", "Join"), 2, false)
	// strings.Sprintf
	SprintfFn = makeFunc(SimpleSelector("fmt", "Sprintf"), 1, true)
	// strings.Fscanf
	FscanfFn = makeFunc(SimpleSelector("fmt", "Fscanf"), 1, true)
	// json.Unmarshall
	JsonUnmarshal = makeFunc(SimpleSelector("json", "Unmarshal"), 2, false)
	// json.Marshall
	JsonMarshal = makeFunc(SimpleSelector("json", "Marshal"), 1, false)
	// time.Now
	TimeNowFn = makeFunc(SimpleSelector("time", "Now"), 0, false)

	// db.Query. Please do not forget about rows.Close
	DbQueryFn = makeFunc(SimpleSelector("db", "Query"), 1, true)
	// rows.Next
	RowsNextFn = makeFunc(SimpleSelector("rows", "Next"), 0, false)
	// rows.Err
	RowsErrFn = makeFunc(SimpleSelector("rows", "Err"), 0, false)
	// rows.Scan
	RowsScanFn = makeFunc(SimpleSelector("rows", "Scan"), 1, true)
)

func DeferCall(fn CallFunctionDescriber, args ...ast.Expr) ast.Stmt {
	fn.checkArgsCount(len(args))
	return &ast.DeferStmt{
		Call: &ast.CallExpr{
			Fun:  fn.FunctionName,
			Args: args,
		},
	}
}

func Call(fn CallFunctionDescriber, args ...ast.Expr) *ast.CallExpr {
	fn.checkArgsCount(len(args))
	return &ast.CallExpr{
		Fun:  fn.FunctionName,
		Args: args,
	}
}

func CallEllipsis(fn CallFunctionDescriber, args ...ast.Expr) *ast.CallExpr {
	fn.checkArgsCount(len(args))
	return &ast.CallExpr{
		Fun:      fn.FunctionName,
		Args:     args,
		Ellipsis: 1,
	}
}

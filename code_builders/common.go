package builders

import (
	"go/ast"
	"go/token"
	"strings"
)

type (
	CallFunctionDescriber struct {
		FunctionName                ast.Expr
		MinimumNumberOfArguments    int
		ExtensibleNumberOfArguments bool
	}
)

func makeFunc(n ast.Expr, m int, e bool) CallFunctionDescriber {
	return CallFunctionDescriber{n, m, e}
}

var (
	Nil         = ast.NewIdent("nil")
	ContextType = MakeSelectorExpression("context", "Context")

	MakeFn   = makeFunc(ast.NewIdent("make"), 1, true)
	LengthFn = makeFunc(ast.NewIdent("len"), 1, false)
	AppendFn = makeFunc(ast.NewIdent("append"), 1, true)

	ConvertItoaFn   = makeFunc(MakeSelectorExpression("strconv", "Itoa"), 1, false)
	ConvertStringFn = makeFunc(ast.NewIdent("string"), 1, false)

	EqualFoldFn   = makeFunc(MakeSelectorExpression("strings", "EqualFold"), 2, false)
	ToLowerFn     = makeFunc(MakeSelectorExpression("strings", "ToLower"), 1, false)
	StringsJoinFn = makeFunc(MakeSelectorExpression("strings", "Join"), 2, false)
	SprintfFn     = makeFunc(MakeSelectorExpression("fmt", "Sprintf"), 1, true)
	TimeNowFn     = makeFunc(MakeSelectorExpression("time", "Now"), 0, false)

	// WARNING do not forget about Close
	DbQueryFn  = makeFunc(MakeSelectorExpression("db", "Query"), 1, true)
	RowsNextFn = makeFunc(MakeSelectorExpression("rows", "Next"), 0, false)
	RowsErrFn  = makeFunc(MakeSelectorExpression("rows", "Err"), 0, false)
	RowsScanFn = makeFunc(MakeSelectorExpression("rows", "Scan"), 1, true)
)

func MakeReturn(results ...ast.Expr) *ast.ReturnStmt {
	return &ast.ReturnStmt{
		Results: results,
	}
}

func MakeEmptyReturn() ast.Stmt {
	return MakeReturn()
}

func MakeComment(comment []string) *ast.CommentGroup {
	if len(comment) == 0 {
		return nil
	} else {
		if len(comment) == 1 && strings.TrimSpace(comment[0]) == "" {
			return nil
		}
	}
	return &ast.CommentGroup{
		List: []*ast.Comment{
			{
				Text: " /* " + strings.Join(comment, "\n") + " */",
			},
		},
	}
}

func MakeDeferCallStatement(fn CallFunctionDescriber, args ...ast.Expr) ast.Stmt {
	if fn.MinimumNumberOfArguments > len(args) {
		panic("the minimum number of arguments has not been reached")
	}
	if !fn.ExtensibleNumberOfArguments && len(args) > fn.MinimumNumberOfArguments {
		panic("the maximum number of arguments exceeded")
	}
	return &ast.DeferStmt{
		Call: &ast.CallExpr{
			Fun:  fn.FunctionName,
			Args: args,
		},
	}
}

func MakeCallExpression(fn CallFunctionDescriber, args ...ast.Expr) *ast.CallExpr {
	if fn.MinimumNumberOfArguments > len(args) {
		panic("the minimum number of arguments has not been reached")
	}
	if !fn.ExtensibleNumberOfArguments && len(args) > fn.MinimumNumberOfArguments {
		panic("the maximum number of arguments exceeded")
	}
	return &ast.CallExpr{
		Fun:  fn.FunctionName,
		Args: args,
	}
}

func MakeCallExpressionEllipsis(fn CallFunctionDescriber, args ...ast.Expr) *ast.CallExpr {
	if fn.MinimumNumberOfArguments > len(args) {
		panic("the minimum number of arguments has not been reached")
	}
	if !fn.ExtensibleNumberOfArguments && len(args) > fn.MinimumNumberOfArguments {
		panic("the maximum number of arguments exceeded")
	}
	return &ast.CallExpr{
		Fun:      fn.FunctionName,
		Args:     args,
		Ellipsis: token.Pos(1),
	}
}

func MakeBlockStmt(statements ...ast.Stmt) *ast.BlockStmt {
	return &ast.BlockStmt{
		List: statements,
	}
}

func MakeImportDecl(lPos *token.Pos, imports map[string]string) ast.Decl {
	var (
		impSpec []ast.Spec
		lParen  = *lPos
	)
	*lPos++
	impSpec = makeImportSpec(lPos, imports)
	return &ast.GenDecl{
		Lparen: lParen,
		Tok:    token.IMPORT,
		Specs:  impSpec,
	}
}

func MakeField(name string, tag *ast.BasicLit, fieldType ast.Expr, comment ...string) *ast.Field {
	return &ast.Field{
		Names:   []*ast.Ident{ast.NewIdent(name)},
		Type:    fieldType,
		Tag:     tag,
		Comment: MakeComment(comment),
	}
}

func MakeVarType(name string, varType ast.Expr) *ast.ValueSpec {
	return &ast.ValueSpec{
		Names: []*ast.Ident{
			ast.NewIdent(name),
		},
		Type: varType,
	}
}

func MakeVarValue(name string, varValue ast.Expr) *ast.ValueSpec {
	return &ast.ValueSpec{
		Names: []*ast.Ident{
			ast.NewIdent(name),
		},
		Values: []ast.Expr{varValue},
	}
}

func MakeVarStatement(spec ...ast.Spec) ast.Stmt {
	return &ast.DeclStmt{
		Decl: &ast.GenDecl{
			Tok:   token.VAR,
			Specs: spec,
		},
	}
}

func MakeAssignment(lhs []string, rhs ...ast.Expr) ast.Stmt {
	lhsExpr := make([]ast.Expr, 0, len(lhs))
	for _, e := range lhs {
		lhsExpr = append(lhsExpr, ast.NewIdent(e))
	}
	return &ast.AssignStmt{
		Lhs: lhsExpr,
		Tok: token.ASSIGN,
		Rhs: rhs,
	}
}

func MakeAddAssignment(lhs []string, rhs ...ast.Expr) ast.Stmt {
	lhsExpr := make([]ast.Expr, 0, len(lhs))
	for _, e := range lhs {
		lhsExpr = append(lhsExpr, ast.NewIdent(e))
	}
	return &ast.AssignStmt{
		Lhs: lhsExpr,
		Tok: token.ADD_ASSIGN,
		Rhs: rhs,
	}
}

func MakeDefinition(lhs []string, rhs ...ast.Expr) ast.Stmt {
	lhsExpr := make([]ast.Expr, 0, len(lhs))
	for _, e := range lhs {
		lhsExpr = append(lhsExpr, ast.NewIdent(e))
	}
	return &ast.AssignStmt{
		Lhs: lhsExpr,
		Tok: token.DEFINE,
		Rhs: rhs,
	}
}

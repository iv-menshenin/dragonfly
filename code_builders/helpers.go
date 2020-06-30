package builders

import "go/ast"

// creates VarNames inline
func MakeVarNames(vars ...string) VarNames {
	return vars
}

func E(first ast.Expr, next ...ast.Expr) []ast.Expr {
	return append([]ast.Expr{first}, next...)
}

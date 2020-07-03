package builders

import "go/ast"

// creates VarNames inline
func MakeVarNames(vars ...string) VarNames {
	return vars
}

func E(first ast.Expr, next ...ast.Expr) []ast.Expr {
	var result = make([]ast.Expr, 0, len(next)+1)
	if first != nil {
		result = append(result, first)
	}
	for _, expr := range next {
		if expr != nil {
			result = append(result, expr)
		}
	}
	return result
}

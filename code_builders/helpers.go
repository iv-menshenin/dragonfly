package builders

import "go/ast"

// creates VarNames inline
func MakeVarNames(vars ...string) VarNames {
	return vars
}

// returns an []ast.Expr, any nil values will be excluded from this array
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

// returns ast.KeyValueExpr or nil if the `value` attribute is nil. useful with E helper
func IfKeyVal(key string, value ast.Expr) ast.Expr {
	if value == nil {
		return nil
	}
	return &ast.KeyValueExpr{
		Key:   ast.NewIdent(key),
		Value: value,
	}
}

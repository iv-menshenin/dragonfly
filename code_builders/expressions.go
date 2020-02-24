package builders

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

func MakeEmptyInterface() ast.Expr {
	return &ast.InterfaceType{
		Methods: &ast.FieldList{},
	}
}

func MakeBasicLiteralString(s string) ast.Expr {
	if strings.Contains(s, "\"") {
		return &ast.BasicLit{
			Kind:  token.STRING,
			Value: fmt.Sprintf("`%s`", s),
		}
	} else {
		return &ast.BasicLit{
			Kind:  token.STRING,
			Value: fmt.Sprintf("\"%s\"", s),
		}
	}
}

func MakeBasicLiteralInteger(i int) ast.Expr {
	return &ast.BasicLit{
		Kind:  token.INT,
		Value: strconv.Itoa(i),
	}
}

func MakeSelectorExpression(pack, name string) ast.Expr {
	return &ast.SelectorExpr{
		X: ast.NewIdent(pack),
		Sel: &ast.Ident{
			Name: name,
		},
	}
}

func MakeStarExpression(expr ast.Expr) ast.Expr {
	return &ast.StarExpr{
		Star: 0,
		X:    expr,
	}
}

func MakeRef(x ast.Expr) ast.Expr {
	return &ast.UnaryExpr{
		Op: token.AND,
		X:  x,
	}
}

func MakeArrayType(expr ast.Expr) ast.Expr {
	return &ast.ArrayType{
		Elt: expr,
	}
}

func MakeNotEqualExpression(left, right ast.Expr) ast.Expr {
	return &ast.BinaryExpr{
		X:  left,
		Op: token.NEQ,
		Y:  right,
	}
}

func MakeAddExpressions(exps ...ast.Expr) ast.Expr {
	newNestLevel := func(left, right ast.Expr) ast.Expr {
		if left == nil {
			return right
		}
		return &ast.BinaryExpr{
			X:  left,
			Op: token.ADD,
			Y:  right,
		}
	}
	var acc ast.Expr = nil
	for _, expr := range exps {
		acc = newNestLevel(acc, expr)
	}
	return acc
}

func MakeNotEmptyArrayExpression(arrayName string) ast.Expr {
	return &ast.BinaryExpr{
		X:  MakeCallExpression(LengthFn, ast.NewIdent(arrayName)),
		Op: token.GTR,
		Y:  MakeBasicLiteralInteger(0),
	}
}

func MakeNotNullExpression(variable ast.Expr) ast.Expr {
	return &ast.BinaryExpr{
		X:  variable,
		Op: token.NEQ,
		Y:  Nil,
	}
}

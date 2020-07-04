package builders

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
)

type (
	varValue interface {
		Expr() ast.Expr
	}
	StringConstant  string  // string constant e.g. "abc"
	IntegerConstant int64   // integer constant e.g. 123
	FloatConstant   float64 // float constant e.g. 123.45
	VariableName    string  // any variable name
)

// ast.BasicLit with token.STRING
func (c StringConstant) Expr() ast.Expr {
	if strings.Contains(string(c), "\"") || strings.Contains(string(c), "\n") {
		return &ast.BasicLit{
			Kind:  token.STRING,
			Value: fmt.Sprintf("`%s`", c),
		}
	} else {
		return &ast.BasicLit{
			Kind:  token.STRING,
			Value: fmt.Sprintf("\"%s\"", c),
		}
	}
}

// ast.BasicLit with token.INT
func (c IntegerConstant) Expr() ast.Expr {
	return &ast.BasicLit{
		ValuePos: 1,
		Kind:     token.INT,
		Value:    fmt.Sprintf("%d", c),
	}
}

// ast.BasicLit with token.FLOAT
func (c FloatConstant) Expr() ast.Expr {
	return &ast.BasicLit{
		ValuePos: 1,
		Kind:     token.FLOAT,
		Value:    fmt.Sprintf("%f", c),
	}
}

// ast.Ident with variable name
func (c VariableName) Expr() ast.Expr {
	return ast.NewIdent(string(c))
}

// somevar[1]
func Index(x ast.Expr, index varValue) ast.Expr {
	return &ast.IndexExpr{
		X:      x,
		Lbrack: 1,
		Index:  index.Expr(),
		Rbrack: 2,
	}
}

// represents a dot notation expression like "pack.object"
func SimpleSelector(pack, object string) ast.Expr {
	return Selector(ast.NewIdent(pack), object)
}

// <x>.<object>
func Selector(x ast.Expr, object string) ast.Expr {
	return &ast.SelectorExpr{
		X:   x,
		Sel: ast.NewIdent(object),
	}
}

// <tok><expr> e.g. !expr
func Unary(expr ast.Expr, tok token.Token) ast.Expr {
	if tok == token.MUL {
		return Star(expr)
	}
	return &ast.UnaryExpr{
		OpPos: 1,
		Op:    tok,
		X:     expr,
	}
}

// *<expr>
func Star(expr ast.Expr) ast.Expr {
	return &ast.StarExpr{
		Star: 1,
		X:    expr,
	}
}

// &<expr>
func Ref(expr ast.Expr) ast.Expr {
	return Unary(expr, token.AND)
}

// !<expr>
func Not(expr ast.Expr) ast.Expr {
	return Unary(expr, token.NOT)
}

// <left> <tok> <right> e.g. left == right
func Binary(left, right ast.Expr, tok token.Token) ast.Expr {
	if left == nil || right == nil {
		panic("unsupported")
	}
	return &ast.BinaryExpr{
		X:     left,
		OpPos: 1,
		Op:    tok,
		Y:     right,
	}
}

// [<l>]<expr>
func ArrayType(expr ast.Expr, l ...ast.Expr) ast.Expr {
	var lenExpr ast.Expr = nil
	if len(l) > 0 {
		lenExpr = l[0]
		if len(l) > 1 {
			panic("allowed only one value")
		}
	}
	return &ast.ArrayType{
		Lbrack: 1,
		Len:    lenExpr,
		Elt:    expr,
	}
}

// <left> != <right>
func NotEqual(left, right ast.Expr) ast.Expr {
	return Binary(left, right, token.NEQ)
}

// <left> == <right>
func Equal(left, right ast.Expr) ast.Expr {
	return Binary(left, right, token.EQL)
}

// <expr1> + <expr2> + <expr3>
func Add(exps ...ast.Expr) ast.Expr {
	var acc ast.Expr = nil
	for _, expr := range exps {
		if acc == nil {
			acc = expr
		} else {
			acc = Binary(acc, expr, token.ADD)
		}
	}
	return acc
}

// <expr1> - <expr2> - <expr3>
func Sub(exps ...ast.Expr) ast.Expr {
	var acc ast.Expr = nil
	for _, expr := range exps {
		if acc == nil {
			acc = expr
		} else {
			acc = Binary(acc, expr, token.SUB)
		}
	}
	return acc
}

// <expr> != nil
func NotNil(expr ast.Expr) ast.Expr {
	return Binary(expr, Nil, token.NEQ)
}

// <expr> == nil
func IsNil(expr ast.Expr) ast.Expr {
	return Binary(expr, Nil, token.EQL)
}

// <varName>.(<t>) e.g. varName.(string)
func VariableTypeAssert(varName string, t ast.Expr) ast.Expr {
	return &ast.TypeAssertExpr{
		X:    ast.NewIdent(varName),
		Type: t,
	}
}

// <t>(<varName>) e.g. string(varName)
func VariableTypeConvert(varName string, t ast.Expr) ast.Expr {
	return Call(
		CallFunctionDescriber{
			FunctionName:                t,
			MinimumNumberOfArguments:    1,
			ExtensibleNumberOfArguments: false,
		},
		ast.NewIdent(varName),
	)
}

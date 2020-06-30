package builders

import (
	"go/ast"
	"go/token"
)

// creates ast.DeclStmt with VAR token
func Var(spec ...ast.Spec) ast.Stmt {
	return &ast.DeclStmt{
		Decl: &ast.GenDecl{
			Tok:   token.VAR,
			Specs: spec,
		},
	}
}

// return a, b, c, ...
func Return(results ...ast.Expr) *ast.ReturnStmt {
	return &ast.ReturnStmt{
		Results: results,
	}
}

// return
func ReturnEmpty() ast.Stmt {
	return Return()
}

// { ... }
func Block(statements ...ast.Stmt) *ast.BlockStmt {
	return &ast.BlockStmt{
		List: statements,
	}
}

// if <condition> { <body> }
func If(condition ast.Expr, body ...ast.Stmt) ast.Stmt {
	return &ast.IfStmt{
		If:   1,
		Cond: condition,
		Body: Block(body...),
	}
}

// if <init>; <condition> { <body> }
func IfInit(initiation ast.Stmt, condition ast.Expr, body ...ast.Stmt) ast.Stmt {
	return &ast.IfStmt{
		If:   1,
		Init: initiation,
		Cond: condition,
		Body: Block(body...),
	}
}

// for <key>, <value> := range <x> { <body> } ":=" replaced by "=" if define is FALSE
func Range(define bool, key, value string, x ast.Expr, body ...ast.Stmt) ast.Stmt {
	var (
		tok           = token.ASSIGN
		k, v ast.Expr = nil, nil
	)
	if key != "" {
		k = ast.NewIdent(key)
	}
	if value != "" {
		v = ast.NewIdent(value)
	}
	if define {
		tok = token.DEFINE
	}
	return &ast.RangeStmt{
		For:    1,
		Key:    k,
		Value:  v,
		TokPos: 2,
		Tok:    tok,
		X:      x,
		Body:   Block(body...),
	}
}

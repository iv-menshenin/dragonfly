package builders

import (
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strings"
)

// with tags like map[tag]values, string `tag1:"values1" tag2:"values2"` is created
func MakeTagsForField(tags map[string][]string) *ast.BasicLit {
	if len(tags) == 0 {
		return nil
	}
	arrTags := make([]string, 0, len(tags))
	for key, val := range tags {
		arrTags = append(arrTags, fmt.Sprintf("%s:\"%s\"", key, strings.Join(val, ",")))
	}
	sort.Strings(arrTags)
	return &ast.BasicLit{
		ValuePos: 1,
		Kind:     token.STRING,
		Value:    "`" + strings.Join(arrTags, " ") + "`",
	}
}

// if <varName>, err = callExpr(); err != nil { <body> }
//
// varName can be omitted
func MakeCallWithErrChecking(varName string, callExpr *ast.CallExpr, body ...ast.Stmt) ast.Stmt {
	if len(body) == 0 {
		body = []ast.Stmt{ReturnEmpty()}
	}
	if varName != "" {
		return IfInit(
			Assign(MakeVarNames(varName, "err"), Assignment, callExpr),
			NotEqual(ast.NewIdent("err"), Nil),
			body...,
		)
	} else {
		return IfInit(
			Assign(MakeVarNames("err"), Assignment, callExpr),
			NotEqual(ast.NewIdent("err"), Nil),
			body...,
		)
	}
}

// if <varName>, err = callExpr(); err != nil { return err }
//
// varName can be omitted
func MakeCallReturnIfError(varName string, callExpr *ast.CallExpr) ast.Stmt {
	if varName != "" {
		return IfInit(
			Assign(MakeVarNames(varName, "err"), Assignment, callExpr),
			NotEqual(ast.NewIdent("err"), Nil),
			Return(ast.NewIdent("err")),
		)
	} else {
		return IfInit(
			Assign(MakeVarNames("err"), Assignment, callExpr),
			NotEqual(ast.NewIdent("err"), Nil),
			Return(ast.NewIdent("err")),
		)
	}
}

// len(<arrayName>) > 0
func MakeLenGreatThanZero(arrayName string) ast.Expr {
	return &ast.BinaryExpr{
		X:  Call(LengthFn, ast.NewIdent(arrayName)),
		Op: token.GTR,
		Y:  Zero,
	}
}

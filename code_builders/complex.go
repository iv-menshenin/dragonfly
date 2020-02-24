package builders

import (
	"fmt"
	"go/ast"
	"strings"
)

func MakeTagsForField(tags map[string][]string) *ast.BasicLit {
	if len(tags) == 0 {
		return nil
	}
	arrTags := make([]string, 0, len(tags))
	for key, val := range tags {
		arrTags = append(arrTags, fmt.Sprintf("%s:\"%s\"", key, strings.Join(val, ",")))
	}
	return &ast.BasicLit{
		ValuePos: 0,
		Kind:     0,
		Value:    "`" + strings.Join(arrTags, " ") + "`",
	}
}

func MakeAssignmentWithErrChecking(varName string, callExpr *ast.CallExpr, body ...ast.Stmt) ast.Stmt {
	if len(body) == 0 {
		body = []ast.Stmt{MakeEmptyReturn()}
	}
	if varName != "" {
		return &ast.IfStmt{
			Init: MakeAssignment(
				[]string{varName, "err"},
				callExpr,
			),
			Cond: MakeNotEqualExpression(ast.NewIdent("err"), ast.NewIdent("nil")),
			Body: &ast.BlockStmt{
				List: body,
			},
		}
	} else {
		return &ast.IfStmt{
			Init: MakeAssignment(
				[]string{"err"},
				callExpr,
			),
			Cond: MakeNotEqualExpression(ast.NewIdent("err"), ast.NewIdent("nil")),
			Body: &ast.BlockStmt{
				List: body,
			},
		}
	}
}

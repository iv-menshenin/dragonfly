package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

func makeEmptyInterface() ast.Expr {
	return &ast.InterfaceType{
		Methods: &ast.FieldList{},
	}
}

func makeEmptyReturn() ast.Stmt {
	return &ast.ReturnStmt{}
}

func makeBasicLiteralString(s string) ast.Expr {
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

func makeBasicLiteralInteger(i int) ast.Expr {
	return &ast.BasicLit{
		Kind:  token.INT,
		Value: strconv.Itoa(i),
	}
}

func makeTypeIdent(structType string) ast.Expr {
	return &ast.Ident{
		Name: structType,
	}
}

func makeTypeSelector(pack, name string) ast.Expr {
	return &ast.SelectorExpr{
		X: makeTypeIdent(pack),
		Sel: &ast.Ident{
			Name: name,
		},
	}
}

func makeTypeStar(expr ast.Expr) ast.Expr {
	return &ast.StarExpr{
		Star: 0,
		X:    expr,
	}
}

func makeRef(x ast.Expr) ast.Expr {
	return &ast.UnaryExpr{
		Op: token.AND,
		X:  x,
	}
}

func makeTypeArray(expr ast.Expr) ast.Expr {
	return &ast.ArrayType{
		Elt: expr,
	}
}

func makeName(name string) *ast.Ident {
	return &ast.Ident{Name: name}
}

func makeComment(comment []string) *ast.CommentGroup {
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

func makeTagsForField(tags map[string][]string) *ast.BasicLit {
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

func makeCall(name ast.Expr, args ...ast.Expr) *ast.CallExpr {
	return &ast.CallExpr{
		Fun:  name,
		Args: args,
	}
}

func makeCallEllipsis(name ast.Expr, args ...ast.Expr) *ast.CallExpr {
	return &ast.CallExpr{
		Fun:      name,
		Args:     args,
		Ellipsis: token.Pos(1),
	}
}

func makeReturn(results ...ast.Expr) *ast.ReturnStmt {
	return &ast.ReturnStmt{
		Results: results,
	}
}

func makeBlock(statements ...ast.Stmt) *ast.BlockStmt {
	return &ast.BlockStmt{
		List: statements,
	}
}

func makeImportDecl(imports ...string) ast.Decl {
	var impSpec = make([]ast.Spec, 0, len(imports))
	for _, imp := range imports {
		impSpec = append(impSpec, &ast.ImportSpec{
			Path: &ast.BasicLit{
				Kind:  token.STRING,
				Value: fmt.Sprintf("\"%s\"", imp),
			},
		})
	}
	return &ast.GenDecl{
		Tok:   token.IMPORT,
		Specs: impSpec,
	}
}

func makeField(name string, tag *ast.BasicLit, t ast.Expr, comment []string) *ast.Field {
	return &ast.Field{
		Names:   []*ast.Ident{makeName(name)},
		Type:    t,
		Tag:     tag,
		Comment: makeComment(comment),
	}
}

func makeVarType(name string, t ast.Expr) *ast.ValueSpec {
	return &ast.ValueSpec{
		Names: []*ast.Ident{
			makeName(name),
		},
		Type: t,
	}
}

func makeVarValue(name string, v ast.Expr) *ast.ValueSpec {
	return &ast.ValueSpec{
		Names: []*ast.Ident{
			makeName(name),
		},
		Values: []ast.Expr{v},
	}
}

func makeVarStatement(spec ...ast.Spec) ast.Stmt {
	return &ast.DeclStmt{
		Decl: &ast.GenDecl{
			Tok:   token.VAR,
			Specs: spec,
		},
	}
}

func makeAssignment(lhs []string, rhs ...ast.Expr) ast.Stmt {
	lhsExpr := make([]ast.Expr, 0, len(lhs))
	for _, e := range lhs {
		lhsExpr = append(lhsExpr, makeName(e))
	}
	return &ast.AssignStmt{
		Lhs: lhsExpr,
		Tok: token.ASSIGN,
		Rhs: rhs,
	}
}

func makeAddAssignment(lhs []string, rhs ...ast.Expr) ast.Stmt {
	lhsExpr := make([]ast.Expr, 0, len(lhs))
	for _, e := range lhs {
		lhsExpr = append(lhsExpr, makeName(e))
	}
	return &ast.AssignStmt{
		Lhs: lhsExpr,
		Tok: token.ADD_ASSIGN,
		Rhs: rhs,
	}
}

func makeDefinition(lhs []string, rhs ...ast.Expr) ast.Stmt {
	lhsExpr := make([]ast.Expr, 0, len(lhs))
	for _, e := range lhs {
		lhsExpr = append(lhsExpr, makeName(e))
	}
	return &ast.AssignStmt{
		Lhs: lhsExpr,
		Tok: token.DEFINE,
		Rhs: rhs,
	}
}

func makeNotEqualExpression(left, right ast.Expr) ast.Expr {
	return &ast.BinaryExpr{
		X:  left,
		Op: token.NEQ,
		Y:  right,
	}
}

func makeAddExpressions(exps ...ast.Expr) ast.Expr {
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

func makeAssignmentWithErrChecking(varName string, callExpr *ast.CallExpr, body ...ast.Stmt) ast.Stmt {
	if len(body) == 0 {
		body = []ast.Stmt{makeEmptyReturn()}
	}
	if varName != "" {
		return &ast.IfStmt{
			Init: makeAssignment(
				[]string{varName, "err"},
				callExpr,
			),
			Cond: makeNotEqualExpression(makeName("err"), makeName("nil")),
			Body: &ast.BlockStmt{
				List: body,
			},
		}
	} else {
		return &ast.IfStmt{
			Init: makeAssignment(
				[]string{"err"},
				callExpr,
			),
			Cond: makeNotEqualExpression(makeName("err"), makeName("nil")),
			Body: &ast.BlockStmt{
				List: body,
			},
		}
	}
}

func makeNotEmptyArrayExpression(arrayName string) ast.Expr {
	return &ast.BinaryExpr{
		X:  makeCall(makeName("len"), makeName(arrayName)),
		Op: token.GTR,
		Y:  makeBasicLiteralInteger(0),
	}
}

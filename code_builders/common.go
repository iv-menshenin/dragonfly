package builders

import (
	"go/ast"
	"go/token"
	"strings"
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

func MakeCallExpression(name ast.Expr, args ...ast.Expr) *ast.CallExpr {
	return &ast.CallExpr{
		Fun:  name,
		Args: args,
	}
}

func MakeCallExpressionEllipsis(name ast.Expr, args ...ast.Expr) *ast.CallExpr {
	return &ast.CallExpr{
		Fun:      name,
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

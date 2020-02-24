package main

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly"
	"go/ast"
)

func localizedFind(
	fullTableName, functionName, rowStructName string,
	optionsFields, _, resultFields []*ast.Field,
) dragonfly.AstDataChain {
	sqlQueryName := fmt.Sprintf("sql_%s", functionName)
	sqlQueryValue := fmt.Sprintf(
		`"select %%s from %s"`,
		fullTableName,
	) // language: SQL

	stmt, types, options := dragonfly.BuildIncomingArgumentsProcessor("filter", fmt.Sprintf("%sFilter", functionName), optionsFields, dragonfly.FindBuilderOptions)
	// dragonfly.BuildExecutionBlockForFunction(dragonfly.WrapperFindAll, )
	return dragonfly.AstDataChain{
		Types: types,
		Constants: map[string]*ast.ValueSpec{
			sqlQueryName: {
				Doc:   nil,
				Names: []*ast.Ident{{Name: sqlQueryName}},
				Type:  ast.NewIdent("string"),
				Values: []ast.Expr{
					&ast.BasicLit{Value: sqlQueryValue},
				},
				Comment: nil,
			},
		},
		Implementations: map[string]*ast.FuncDecl{
			functionName: {
				Recv: nil,
				Name: ast.NewIdent(functionName),
				Type: &ast.FuncType{
					Params: &ast.FieldList{
						List: append([]*ast.Field{
							{
								Names: []*ast.Ident{ast.NewIdent("ctx")},
								Type: &ast.SelectorExpr{
									X:   ast.NewIdent("context"),
									Sel: ast.NewIdent("Context"),
								},
							},
						}, options...),
					},
					Results: &ast.FieldList{
						List: []*ast.Field{
							{
								Names: []*ast.Ident{ast.NewIdent("result")},
								Type:  &ast.BasicLit{Value: rowStructName},
							},
							{
								Names: []*ast.Ident{ast.NewIdent("err")},
								Type:  ast.NewIdent("error"),
							},
						},
					},
				},
				Body: &ast.BlockStmt{
					List: stmt,
				},
			},
		},
	}
}

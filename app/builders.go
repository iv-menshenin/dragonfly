package main

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly"
	"github.com/iv-menshenin/dragonfly/code_builders"
	"go/ast"
	"strings"
)

func localizedFind(
	fullTableName, functionName, rowStructName string,
	optionsFields, _, resultFields []*ast.Field,
) dragonfly.AstDataChain {
	const (
		queryVariableName = "sqlQueryText"
	)
	sqlQueryName := fmt.Sprintf("sql%s", functionName)
	stmt, types, options := builders.BuildFindArgumentsProcessor("filter", fmt.Sprintf("%sFilter", functionName), optionsFields, builders.FindBuilderOptions)
	fieldRefs, columnList := builders.ExtractDestinationFieldRefsFromStruct(builders.ScanDestVariable.String(), resultFields)
	sqlQueryValue := fmt.Sprintf(
		`select %s from %s`,
		strings.Join(columnList, ","),
		fullTableName,
	) // language: SQL
	stmt = append(
		[]ast.Stmt{
			builders.MakeVarStatement(
				builders.MakeVarType("db", builders.MakeStarExpression(builders.MakeSelectorExpression("sql", "DB"))),
				builders.MakeVarType("rows", builders.MakeStarExpression(builders.MakeSelectorExpression("sql", "Rows"))),
				builders.MakeVarValue(queryVariableName, ast.NewIdent(sqlQueryName)),
				builders.MakeVarValue(
					builders.ArgsVariable.String(),
					builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(builders.MakeEmptyInterface()), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(optionsFields))),
				),
				builders.MakeVarValue(
					builders.FiltersVariable.String(),
					builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(optionsFields))),
				),
			),
			builders.MakeAssignmentWithErrChecking(
				"db",
				builders.MakeCallExpression(
					builders.CallFunctionDescriber{
						FunctionName:                ast.NewIdent("getDatabase"),
						MinimumNumberOfArguments:    1,
						ExtensibleNumberOfArguments: false,
					},
					ast.NewIdent("ctx"),
				),
				builders.MakeEmptyReturn(),
			),
		},
		stmt...,
	)
	stmt = append(
		stmt,
		&ast.IfStmt{
			Cond: builders.MakeNotEmptyArrayExpression(builders.FiltersVariable.String()),
			Body: builders.MakeBlockStmt(
				builders.MakeAddAssignment(
					[]string{queryVariableName},
					builders.MakeCallExpression(
						builders.SprintfFn,
						builders.MakeBasicLiteralString(" where (%s)"),
						builders.MakeCallExpression(
							builders.StringsJoinFn,
							ast.NewIdent(builders.FiltersVariable.String()),
							builders.MakeBasicLiteralString(") and ("),
						),
					),
				),
			),
		},
	)
	stmt = append(
		stmt,
		builders.BuildExecutionBlockForFunction(builders.WrapperFindAll, fieldRefs, builders.MakeExecutionOption(rowStructName, queryVariableName))...,
	)
	return dragonfly.AstDataChain{
		Types: types,
		Constants: map[string]*ast.ValueSpec{
			sqlQueryName: {
				Doc:   nil,
				Names: []*ast.Ident{{Name: sqlQueryName}},
				Type:  ast.NewIdent("string"),
				Values: []ast.Expr{
					builders.MakeBasicLiteralString(sqlQueryValue),
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
						List: append(
							[]*ast.Field{
								{
									Names: []*ast.Ident{ast.NewIdent("ctx")},
									Type: &ast.SelectorExpr{
										X:   ast.NewIdent("context"),
										Sel: ast.NewIdent("Context"),
									},
								},
							},
							append(options, builders.MakeField("locale", nil, ast.NewIdent("string")))...,
						),
					},
					Results: &ast.FieldList{
						List: []*ast.Field{
							{
								Names: []*ast.Ident{ast.NewIdent("result")},
								Type:  builders.MakeArrayType(&ast.BasicLit{Value: rowStructName}),
							},
							{
								Names: []*ast.Ident{ast.NewIdent("err")},
								Type:  ast.NewIdent("error"),
							},
						},
					},
				},
				Body: &ast.BlockStmt{
					List: append(stmt, builders.MakeEmptyReturn()),
				},
			},
		},
	}
}

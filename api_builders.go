package dragonfly

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/code_builders"
	"go/ast"
	"reflect"
	"strings"
)

const (
	sqlEmptyResultErrorName = "sqlEmptyResult"
)

func exprToString(expr ast.Expr) string {
	if i, ok := expr.(*ast.StarExpr); ok {
		return exprToString(i.X)
	}
	if i, ok := expr.(*ast.Ident); ok {
		return i.Name
	}
	if i, ok := expr.(*ast.SelectorExpr); ok {
		return exprToString(i.X) + "." + exprToString(i.Sel)
	}
	return ""
}

// TODO other place?
func funcDeclsToMap(functions []*ast.FuncDecl) map[string]*ast.FuncDecl {
	result := make(map[string]*ast.FuncDecl, len(functions))
	for i, f := range functions {
		funcName := f.Name.Name
		if f.Recv != nil && len(f.Recv.List) > 0 {
			funcName = fmt.Sprintf("%s.%s", exprToString(f.Recv.List[0].Type), funcName)
		}
		if r, ok := result[funcName]; ok {
			if reflect.DeepEqual(r, f) {
				continue
			}
			panic(fmt.Sprintf("name `%s` repeated", funcName))
		}
		result[funcName] = functions[i]
	}
	return result
}

type (
	findVariant int
)

const (
	findVariantOnce findVariant = iota
	findVariantAll
)

func addVariablesToFunctionBody(
	functionBody []ast.Stmt,
	sqlQueryVariableName string,
	sqlQuery string,
	addition ...ast.Spec,
) []ast.Stmt {
	return append(
		[]ast.Stmt{
			builders.MakeVarStatement(
				append([]ast.Spec{
					builders.MakeVarType("db", builders.MakeStarExpression(builders.MakeSelectorExpression("sql", "DB"))),
					builders.MakeVarType("rows", builders.MakeStarExpression(builders.MakeSelectorExpression("sql", "Rows"))),
					builders.MakeVarValue(sqlQueryVariableName, builders.MakeBasicLiteralString(sqlQuery)),
				}, addition...)...,
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
		functionBody...,
	)
}

func makeFindFunction(variant findVariant) ApiFuncBuilder {
	const (
		sqlTextName = "sqlText"
	)
	return func(
		fullTableName, functionName, rowStructName string,
		optionFields, _, rowFields []*ast.Field,
	) AstDataChain {
		var (
			scanBlockWrapper builders.ScanWrapper
			resultExpr       ast.Expr
			lastReturn       ast.Stmt
		)
		switch variant {
		case findVariantOnce:
			scanBlockWrapper = builders.WrapperFindOne
			resultExpr = ast.NewIdent(rowStructName)
			lastReturn = builders.MakeReturn(
				ast.NewIdent("result"),
				ast.NewIdent(sqlEmptyResultErrorName),
			)
		case findVariantAll:
			scanBlockWrapper = builders.WrapperFindAll
			resultExpr = builders.MakeArrayType(ast.NewIdent(rowStructName))
			lastReturn = builders.MakeEmptyReturn()
		default:
			panic("cannot resolve 'variant'")
		}
		var (
			fieldRefs, columnList = builders.ExtractDestinationFieldRefsFromStruct(builders.ScanDestVariable.String(), rowFields)
		)
		sqlQuery := fmt.Sprintf("select %s from %s where %%s", strings.Join(columnList, ", "), fullTableName)
		functionBody, findTypes, findAttrs := builders.BuildFindArgumentsProcessor(
			"find",
			functionName+"Option",
			optionFields,
			builders.FindBuilderOptions,
		)
		functionBody = append(
			functionBody,
			&ast.IfStmt{
				Cond: builders.MakeNotEmptyArrayExpression(builders.FiltersVariable.String()),
				Body: builders.MakeBlockStmt(
					builders.MakeAssignment(
						[]string{sqlTextName},
						builders.MakeCallExpression(
							builders.SprintfFn,
							ast.NewIdent(sqlTextName),
							builders.MakeAddExpressions(
								builders.MakeBasicLiteralString("("),
								builders.MakeCallExpression(
									builders.StringsJoinFn,
									ast.NewIdent(builders.FiltersVariable.String()),
									builders.MakeBasicLiteralString(") and ("),
								),
								builders.MakeBasicLiteralString(")"),
							),
						),
					),
				),
				Else: builders.MakeAssignment(
					[]string{sqlTextName},
					builders.MakeCallExpression(
						builders.SprintfFn,
						ast.NewIdent(sqlTextName),
						builders.MakeBasicLiteralString("1 = 1"),
					),
				),
			},
		)
		functionBody = append(
			append(
				functionBody,
				builders.BuildExecutionBlockForFunction(scanBlockWrapper, fieldRefs, builders.MakeExecutionOption(rowStructName, sqlTextName))...,
			),
			lastReturn,
		)
		functionBody = addVariablesToFunctionBody(
			functionBody,
			sqlTextName,
			sqlQuery,
			builders.MakeVarValue(
				builders.ArgsVariable.String(),
				builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(builders.MakeEmptyInterface()), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(optionFields))),
			),
			builders.MakeVarValue(
				builders.FiltersVariable.String(),
				builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(optionFields))),
			),
		)
		return AstDataChain{
			Types:     findTypes,
			Constants: nil,
			Implementations: map[string]*ast.FuncDecl{
				functionName: builders.MakeDatabaseApiFunction(functionName, resultExpr, functionBody, findAttrs...),
			},
		}
	}
}

func makeDeleteFunction(variant findVariant) ApiFuncBuilder {
	const (
		sqlTextName = "sqlText"
	)
	return func(
		fullTableName, functionName, rowStructName string,
		optionFields, _, rowFields []*ast.Field,
	) AstDataChain {
		var (
			scanBlockWrapper builders.ScanWrapper
			resultExpr       ast.Expr
			lastReturn       ast.Stmt
		)
		switch variant {
		case findVariantOnce:
			scanBlockWrapper = builders.WrapperFindOne
			resultExpr = ast.NewIdent(rowStructName)
			lastReturn = builders.MakeReturn(
				ast.NewIdent("result"),
				ast.NewIdent(sqlEmptyResultErrorName),
			)
		case findVariantAll:
			scanBlockWrapper = builders.WrapperFindAll
			resultExpr = builders.MakeArrayType(ast.NewIdent(rowStructName))
			lastReturn = builders.MakeEmptyReturn()
		default:
			panic("cannot resolve 'variant'")
		}
		var (
			fieldRefs, columnList = builders.ExtractDestinationFieldRefsFromStruct(builders.ScanDestVariable.String(), rowFields)
		)
		sqlQuery := fmt.Sprintf("delete from %s where %%s returning %s", fullTableName, strings.Join(columnList, ", "))
		functionBody, findTypes, findAttrs := builders.BuildFindArgumentsProcessor(
			"find",
			functionName+"Option",
			optionFields,
			builders.DeleteBuilderOptions,
		)
		functionBody = append(
			functionBody,
			&ast.IfStmt{
				Cond: builders.MakeNotEmptyArrayExpression(builders.FiltersVariable.String()),
				Body: builders.MakeBlockStmt(
					builders.MakeAssignment(
						[]string{sqlTextName},
						builders.MakeCallExpression(
							builders.SprintfFn,
							ast.NewIdent(sqlTextName),
							builders.MakeAddExpressions(
								builders.MakeBasicLiteralString("("),
								builders.MakeCallExpression(
									builders.StringsJoinFn,
									ast.NewIdent(builders.FiltersVariable.String()),
									builders.MakeBasicLiteralString(") and ("),
								),
								builders.MakeBasicLiteralString(")"),
							),
						),
					),
				),
				Else: builders.MakeAssignment(
					[]string{sqlTextName},
					builders.MakeCallExpression(
						builders.SprintfFn,
						ast.NewIdent(sqlTextName),
						builders.MakeBasicLiteralString("/* ERROR: CANNOT DELETE ALL */ !"),
					),
				),
			},
		)
		functionBody = append(
			append(
				functionBody,
				builders.BuildExecutionBlockForFunction(scanBlockWrapper, fieldRefs, builders.MakeExecutionOption(rowStructName, sqlTextName))...,
			),
			lastReturn,
		)
		functionBody = addVariablesToFunctionBody(
			functionBody,
			sqlTextName,
			sqlQuery,
			builders.MakeVarValue(
				builders.ArgsVariable.String(),
				builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(builders.MakeEmptyInterface()), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(optionFields))),
			),
			builders.MakeVarValue(
				builders.FiltersVariable.String(),
				builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(optionFields))),
			),
		)
		return AstDataChain{
			Types:     findTypes,
			Constants: nil,
			Implementations: map[string]*ast.FuncDecl{
				functionName: builders.MakeDatabaseApiFunction(functionName, resultExpr, functionBody, findAttrs...),
			},
		}
	}
}

func updateOneBuilder(
	fullTableName, functionName, rowStructName string,
	optionFields, mutableFields, rowFields []*ast.Field,
) AstDataChain {
	const (
		sqlTextName = "sqlText"
	)
	resultExpr := ast.NewIdent(rowStructName)
	scanBlockWrapper := builders.WrapperFindOne
	lastReturn := builders.MakeReturn(
		ast.NewIdent("result"),
		ast.NewIdent(sqlEmptyResultErrorName),
	)
	var (
		fieldRefs, outColumnList = builders.ExtractDestinationFieldRefsFromStruct(builders.ScanDestVariable.String(), rowFields)
	)
	sqlQuery := fmt.Sprintf("update %s set %%s where %%s returning %s", fullTableName, strings.Join(outColumnList, ", "))
	functionBody, inputTypes, inputAttrs := builders.BuildInputValuesProcessor(
		"values",
		makeExportedName(functionName+"Values"),
		mutableFields,
		builders.UpdateBuilderOptions,
	)
	findBlock, findTypes, findAttrs := builders.BuildFindArgumentsProcessor(
		"filter",
		makeExportedName(functionName+"Option"),
		optionFields,
		builders.IncomingArgumentsBuilderOptions,
	)
	functionBody = append(functionBody, findBlock...)
	functionBody = append(
		functionBody,
		builders.MakeAssignment(
			[]string{sqlTextName},
			builders.MakeCallExpression(
				builders.SprintfFn,
				ast.NewIdent(sqlTextName),
				builders.MakeCallExpression(
					builders.StringsJoinFn,
					ast.NewIdent(builders.FieldsVariable.String()),
					builders.MakeBasicLiteralString(", "),
				),
				builders.MakeAddExpressions(
					builders.MakeBasicLiteralString("("),
					builders.MakeCallExpression(
						builders.StringsJoinFn,
						ast.NewIdent(builders.FiltersVariable.String()),
						builders.MakeBasicLiteralString(") and ("),
					),
					builders.MakeBasicLiteralString(")"),
				),
			),
		),
	)
	functionBody = append(
		append(
			functionBody,
			builders.BuildExecutionBlockForFunction(scanBlockWrapper, fieldRefs, builders.MakeExecutionOption(rowStructName, sqlTextName))...,
		),
		lastReturn,
	)
	functionBody = addVariablesToFunctionBody(
		functionBody,
		sqlTextName,
		sqlQuery,
		builders.MakeVarValue(
			builders.ArgsVariable.String(),
			builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(builders.MakeEmptyInterface()), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
		builders.MakeVarValue(
			builders.FieldsVariable.String(),
			builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
		builders.MakeVarValue(
			builders.FiltersVariable.String(),
			builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
	)
	unionTypeDeclMaps := func(a, b map[string]*ast.TypeSpec) map[string]*ast.TypeSpec {
		for key := range b {
			a[key] = b[key]
		}
		return a
	}
	return AstDataChain{
		Types:     unionTypeDeclMaps(inputTypes, findTypes),
		Constants: nil,
		Implementations: map[string]*ast.FuncDecl{
			functionName: builders.MakeDatabaseApiFunction(functionName, resultExpr, functionBody, append(inputAttrs, findAttrs...)...),
		},
	}
}

func insertOneBuilder(
	fullTableName, functionName, rowStructName string,
	_, mutableFields, rowFields []*ast.Field,
) AstDataChain {
	const (
		sqlTextName = "sqlText"
	)
	resultExpr := ast.NewIdent(rowStructName)
	scanBlockWrapper := builders.WrapperFindOne
	lastReturn := builders.MakeReturn(
		ast.NewIdent("result"),
		ast.NewIdent(sqlEmptyResultErrorName),
	)
	var (
		fieldRefs, outColumnList = builders.ExtractDestinationFieldRefsFromStruct(builders.ScanDestVariable.String(), rowFields)
	)
	sqlQuery := fmt.Sprintf("insert into %s (%%s) values (%%s) returning %s", fullTableName, strings.Join(outColumnList, ", "))
	functionBody, functionTypes, functionAttrs := builders.BuildInputValuesProcessor(
		"record",
		makeExportedName(functionName+"Values"),
		mutableFields,
		builders.InsertBuilderOptions,
	)
	functionBody = append(
		functionBody,
		builders.MakeAssignment(
			[]string{sqlTextName},
			builders.MakeCallExpression(
				builders.SprintfFn,
				ast.NewIdent(sqlTextName),
				builders.MakeCallExpression(
					builders.StringsJoinFn,
					ast.NewIdent(builders.FieldsVariable.String()),
					builders.MakeBasicLiteralString(", "),
				),
				builders.MakeCallExpression(
					builders.StringsJoinFn,
					ast.NewIdent(builders.ValuesVariable.String()),
					builders.MakeBasicLiteralString(", "),
				),
			),
		),
	)
	functionBody = append(
		append(
			functionBody,
			builders.BuildExecutionBlockForFunction(scanBlockWrapper, fieldRefs, builders.MakeExecutionOption(rowStructName, sqlTextName))...,
		),
		lastReturn,
	)
	functionBody = addVariablesToFunctionBody(
		functionBody,
		sqlTextName,
		sqlQuery,
		builders.MakeVarValue(
			builders.ArgsVariable.String(),
			builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(builders.MakeEmptyInterface()), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
		builders.MakeVarValue(
			builders.FieldsVariable.String(),
			builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
		builders.MakeVarValue(
			builders.ValuesVariable.String(),
			builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
	)
	return AstDataChain{
		Types:     functionTypes,
		Constants: nil,
		Implementations: map[string]*ast.FuncDecl{
			functionName: builders.MakeDatabaseApiFunction(functionName, resultExpr, functionBody, functionAttrs...),
		},
	}
}

func upsertBuilder(
	fullTableName, functionName, rowStructName string,
	optionFields, mutableFields, rowFields []*ast.Field,
) AstDataChain {
	const (
		sqlTextName = "sqlText"
	)
	resultExpr := ast.NewIdent(rowStructName)
	scanBlockWrapper := builders.WrapperFindOne
	lastReturn := builders.MakeReturn(
		ast.NewIdent("result"),
		ast.NewIdent(sqlEmptyResultErrorName),
	)
	var (
		fieldRefs, outColumnList = builders.ExtractDestinationFieldRefsFromStruct(builders.ScanDestVariable.String(), rowFields)
		_, uniqueColumns         = builders.ExtractDestinationFieldRefsFromStruct("", optionFields)
	)
	sqlQuery := fmt.Sprintf("insert into %s (%%s) values (%%s) on conflict (%s) do update set %%s returning %s", fullTableName, strings.Join(uniqueColumns, ","), strings.Join(outColumnList, ", "))
	functionBody, functionTypes, functionAttrs := builders.BuildInputValuesProcessor(
		"record",
		makeExportedName(functionName+"Values"),
		mutableFields,
		builders.InsertBuilderOptions,
	)
	functionBody = append(
		functionBody,
		builders.MakeDefinition(
			[]string{"update"},
			builders.MakeCallExpression(
				builders.MakeFn,
				builders.MakeArrayType(ast.NewIdent("string")),
				builders.MakeBasicLiteralInteger(0),
				builders.MakeCallExpression(builders.LengthFn, ast.NewIdent(builders.FieldsVariable.String())),
			),
		),
		builders.MakeRangeStatement(
			"i", "", ast.NewIdent(builders.FieldsVariable.String()),
			builders.MakeBlockStmt(
				builders.MakeAssignment(
					[]string{"update"},
					builders.MakeCallExpression(
						builders.AppendFn,
						ast.NewIdent("update"),
						builders.MakeCallExpression(
							builders.SprintfFn,
							builders.MakeBasicLiteralString("%s = %s"),
							builders.MakeIndexExpression(ast.NewIdent(builders.FieldsVariable.String()), ast.NewIdent("i")),
							builders.MakeIndexExpression(ast.NewIdent(builders.ValuesVariable.String()), ast.NewIdent("i")),
						),
					),
				),
			),
		),
		builders.MakeAssignment(
			[]string{sqlTextName},
			builders.MakeCallExpression(
				builders.SprintfFn,
				ast.NewIdent(sqlTextName),
				builders.MakeCallExpression(
					builders.StringsJoinFn,
					ast.NewIdent(builders.FieldsVariable.String()),
					builders.MakeBasicLiteralString(", "),
				),
				builders.MakeCallExpression(
					builders.StringsJoinFn,
					ast.NewIdent(builders.ValuesVariable.String()),
					builders.MakeBasicLiteralString(", "),
				),
				builders.MakeCallExpression(
					builders.StringsJoinFn,
					ast.NewIdent("update"),
					builders.MakeBasicLiteralString(", "),
				),
			),
		),
	)
	functionBody = append(
		append(
			functionBody,
			builders.BuildExecutionBlockForFunction(scanBlockWrapper, fieldRefs, builders.MakeExecutionOption(rowStructName, sqlTextName))...,
		),
		lastReturn,
	)
	functionBody = addVariablesToFunctionBody(
		functionBody,
		sqlTextName,
		sqlQuery,
		builders.MakeVarValue(
			builders.ArgsVariable.String(),
			builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(builders.MakeEmptyInterface()), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
		builders.MakeVarValue(
			builders.FieldsVariable.String(),
			builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
		builders.MakeVarValue(
			builders.ValuesVariable.String(),
			builders.MakeCallExpression(builders.MakeFn, builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
	)
	return AstDataChain{
		Types:     functionTypes,
		Constants: nil,
		Implementations: map[string]*ast.FuncDecl{
			functionName: builders.MakeDatabaseApiFunction(functionName, resultExpr, functionBody, functionAttrs...),
		},
	}
}

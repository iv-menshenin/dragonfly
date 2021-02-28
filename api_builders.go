package dragonfly

import (
	"fmt"
	"github.com/iv-menshenin/go-ast"
	"go/ast"
	"go/token"
	"reflect"
	"strings"
)

const (
	sqlEmptyResultErrorName = "sqlEmptyResult"
	getQueryExecPointFnName = "getQueryExecPoint"
	queryExecInterfaceName  = "queryExecInterface"
)

func exprToString(expr ast.Expr) string {
	switch v := expr.(type) {
	case *ast.StarExpr:
		return exprToString(v.X)
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return exprToString(v.X) + "." + exprToString(v.Sel)
	default:
		panic("unimplemented")
	}
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
	findVariantPaginate
)

func simpleResultOneRecord(rowStructName string) []*ast.Field {
	return []*ast.Field{
		builders.Field("result", nil, ast.NewIdent(rowStructName)),
	}
}

func simpleResultArray(rowStructName string) []*ast.Field {
	return []*ast.Field{
		builders.Field("result", nil, builders.ArrayType(ast.NewIdent(rowStructName))),
	}
}

func resultArrayWithCounter(rowStructName string) []*ast.Field {
	return []*ast.Field{
		builders.Field("result", nil, builders.ArrayType(ast.NewIdent(rowStructName))),
		builders.Field("rowCount", nil, ast.NewIdent("int64")),
	}
}

func addVariablesToFunctionBody(
	functionBody []ast.Stmt,
	sqlQueryVariableName string,
	sqlQuery string,
	addition ...ast.Spec,
) []ast.Stmt {
	return append(
		[]ast.Stmt{
			builders.Var(
				append([]ast.Spec{
					builders.VariableType("db", ast.NewIdent(queryExecInterfaceName)),
					builders.VariableType("rows", builders.Star(builders.SimpleSelector("sql", "Rows"))),
					builders.VariableValue(sqlQueryVariableName, builders.StringConstant(sqlQuery).Expr()),
				}, addition...)...,
			),
			builders.MakeCallWithErrChecking(
				"db",
				builders.Call(
					builders.CallFunctionDescriber{
						FunctionName:                ast.NewIdent(getQueryExecPointFnName),
						MinimumNumberOfArguments:    1,
						ExtensibleNumberOfArguments: false,
					},
					ast.NewIdent("ctx"),
				),
				builders.ReturnEmpty(),
			),
		},
		functionBody...,
	)
}

// TODO simplify
func makeFindFunction(variant findVariant) ApiFuncBuilder {
	const (
		sqlTextName = "sqlText"
	)
	var (
		scanBlockWrapper      ScanWrapper
		resultExprFn          func(string) []*ast.Field
		optionsExprFn         = func(e []*ast.Field) []*ast.Field { return e }
		lastReturn            ast.Stmt
		fieldRefsWrapper      = func(e []ast.Expr) []ast.Expr { return e }
		executionBlockBuilder func(rowStructName string, fieldRefs []ast.Expr) []ast.Stmt
	)
	switch variant {
	case findVariantOnce:
		scanBlockWrapper = WrapperFindOne
		resultExprFn = simpleResultOneRecord
		lastReturn = builders.Return(
			ast.NewIdent("result"),
			ast.NewIdent(sqlEmptyResultErrorName),
		)
	case findVariantAll:
		scanBlockWrapper = WrapperFindAll
		resultExprFn = simpleResultArray
		lastReturn = builders.ReturnEmpty()
	case findVariantPaginate:
		scanBlockWrapper = WrapperFindAll
		resultExprFn = resultArrayWithCounter
		lastReturn = builders.ReturnEmpty()
		fieldRefsWrapper = func(e []ast.Expr) []ast.Expr { return append(e, builders.Ref(ast.NewIdent("rowCount"))) }
		optionsExprFn = func(e []*ast.Field) []*ast.Field {
			return append(e, builders.Field("page", nil, ast.NewIdent("Pagination")))
		}
		executionBlockBuilder = func(rowStructName string, fieldRefs []ast.Expr) []ast.Stmt {
			return BuildExecutionBlockForFunction(
				scanBlockWrapper,
				fieldRefsWrapper(fieldRefs),
				MakeExecutionOptionWithWrappers(
					rowStructName,
					sqlTextName,
					func(sql ast.Expr) ast.Expr {
						return builders.Call(
							builders.SprintfFn,
							&ast.BasicLit{
								Kind:  token.STRING,
								Value: `"with query as (%s) select query.*, (select count(*) from query) from query limit $%d offset $%d;"`,
							},
							sql,
							builders.Add(
								builders.Call(builders.LengthFn, ast.NewIdent("args")), // TODO ast.NewIdent("args")
								builders.IntegerConstant(1).Expr(),
							),
							builders.Add(
								builders.Call(builders.LengthFn, ast.NewIdent("args")), // TODO ast.NewIdent("args")
								builders.IntegerConstant(2).Expr(),
							),
						)
					},
					func(e ast.Expr) ast.Expr {
						return builders.Call(
							builders.AppendFn,
							e,
							builders.SimpleSelector("page", "Limit"),
							builders.SimpleSelector("page", "Offset"),
						)
					},
				),
			)
		}
	default:
		panic("cannot resolve 'variant'")
	}
	if executionBlockBuilder == nil {
		executionBlockBuilder = func(rowStructName string, fieldRefs []ast.Expr) []ast.Stmt {
			return BuildExecutionBlockForFunction(
				scanBlockWrapper,
				fieldRefsWrapper(fieldRefs),
				MakeExecutionOption(rowStructName, sqlTextName),
			)
		}
	}
	return func(
		fullTableName, functionName, rowStructName string,
		optionFields, _, rowFields []dataCellFactory,
	) AstDataChain {
		var (
			fieldRefs, columnList = ExtractDestinationFieldRefsFromStruct(ScanDestVariable.String(), rowFields)
		)
		sqlQuery := fmt.Sprintf("select %s from %s where %%s", strings.Join(columnList, ", "), fullTableName)
		functionBody, findTypes, findAttrs := BuildFindArgumentsProcessor(
			"find",
			functionName+"Option",
			optionFields,
			FindBuilderOptions,
		)
		functionBody = append(
			functionBody,
			&ast.IfStmt{
				Cond: builders.MakeLenGreatThanZero(FiltersVariable.String()),
				Body: builders.Block(
					builders.Assign(
						builders.MakeVarNames(sqlTextName),
						builders.Assignment,
						builders.Call(
							builders.SprintfFn,
							ast.NewIdent(sqlTextName),
							builders.Add(
								builders.StringConstant("(").Expr(),
								builders.Call(
									builders.StringsJoinFn,
									ast.NewIdent(FiltersVariable.String()),
									builders.StringConstant(") and (").Expr(),
								),
								builders.StringConstant(")").Expr(),
							),
						),
					),
				),
				Else: builders.Assign(
					builders.MakeVarNames(sqlTextName),
					builders.Assignment,
					builders.Call(
						builders.SprintfFn,
						ast.NewIdent(sqlTextName),
						builders.StringConstant("1 = 1").Expr(),
					),
				),
			},
		)
		functionBody = append(
			append(
				functionBody,
				executionBlockBuilder(rowStructName, fieldRefs)...,
			),
			lastReturn,
		)
		functionBody = addVariablesToFunctionBody(
			functionBody,
			sqlTextName,
			sqlQuery,
			builders.VariableValue(
				ArgsVariable.String(),
				builders.Call(builders.MakeFn, builders.ArrayType(builders.EmptyInterface), builders.Zero, builders.IntegerConstant(len(optionFields)).Expr()),
			),
			builders.VariableValue(
				FiltersVariable.String(),
				builders.Call(builders.MakeFn, builders.ArrayType(ast.NewIdent("string")), builders.Zero, builders.IntegerConstant(len(optionFields)).Expr()),
			),
		)
		return AstDataChain{
			Types:     findTypes,
			Constants: nil,
			Implementations: map[string]*ast.FuncDecl{
				functionName: MakeDatabaseApiFunction(functionName, resultExprFn(rowStructName), functionBody, optionsExprFn(findAttrs)...),
			},
		}
	}
}

func makeDeleteFunction(variant findVariant) ApiFuncBuilder {
	const (
		sqlTextName = "sqlText"
	)
	var (
		scanBlockWrapper ScanWrapper
		resultExprFn     func(string) []*ast.Field
		lastReturn       ast.Stmt
	)
	switch variant {
	case findVariantOnce:
		scanBlockWrapper = WrapperFindOne
		resultExprFn = simpleResultOneRecord
		lastReturn = builders.Return(
			ast.NewIdent("result"),
			ast.NewIdent(sqlEmptyResultErrorName),
		)
	case findVariantAll:
		scanBlockWrapper = WrapperFindAll
		resultExprFn = simpleResultArray
		lastReturn = builders.ReturnEmpty()
	default:
		panic("cannot resolve 'variant'")
	}
	return func(
		fullTableName, functionName, rowStructName string,
		optionFields, _, rowFields []dataCellFactory,
	) AstDataChain {
		var (
			fieldRefs, columnList = ExtractDestinationFieldRefsFromStruct(ScanDestVariable.String(), rowFields)
		)
		sqlQuery := fmt.Sprintf("delete from %s where %%s returning %s", fullTableName, strings.Join(columnList, ", "))
		functionBody, findTypes, findAttrs := BuildFindArgumentsProcessor(
			"find",
			functionName+"Option",
			optionFields,
			DeleteBuilderOptions,
		)
		functionBody = append(
			functionBody,
			&ast.IfStmt{
				Cond: builders.MakeLenGreatThanZero(FiltersVariable.String()),
				Body: builders.Block(
					builders.Assign(
						builders.MakeVarNames(sqlTextName),
						builders.Assignment,
						builders.Call(
							builders.SprintfFn,
							ast.NewIdent(sqlTextName),
							builders.Add(
								builders.StringConstant("(").Expr(),
								builders.Call(
									builders.StringsJoinFn,
									ast.NewIdent(FiltersVariable.String()),
									builders.StringConstant(") and (").Expr(),
								),
								builders.StringConstant(")").Expr(),
							),
						),
					),
				),
				Else: builders.Assign(
					builders.MakeVarNames(sqlTextName),
					builders.Assignment,
					builders.Call(
						builders.SprintfFn,
						ast.NewIdent(sqlTextName),
						builders.StringConstant("/* ERROR: CANNOT DELETE ALL */ !").Expr(),
					),
				),
			},
		)
		functionBody = append(
			append(
				functionBody,
				BuildExecutionBlockForFunction(scanBlockWrapper, fieldRefs, MakeExecutionOption(rowStructName, sqlTextName))...,
			),
			lastReturn,
		)
		functionBody = addVariablesToFunctionBody(
			functionBody,
			sqlTextName,
			sqlQuery,
			builders.VariableValue(
				ArgsVariable.String(),
				builders.Call(builders.MakeFn, builders.ArrayType(builders.EmptyInterface), builders.Zero, builders.IntegerConstant(len(optionFields)).Expr()),
			),
			builders.VariableValue(
				FiltersVariable.String(),
				builders.Call(builders.MakeFn, builders.ArrayType(ast.NewIdent("string")), builders.Zero, builders.IntegerConstant(len(optionFields)).Expr()),
			),
		)
		return AstDataChain{
			Types:     findTypes,
			Constants: nil,
			Implementations: map[string]*ast.FuncDecl{
				functionName: MakeDatabaseApiFunction(functionName, resultExprFn(rowStructName), functionBody, findAttrs...),
			},
		}
	}
}

func makeUpdateFunction(variant findVariant) ApiFuncBuilder {
	const (
		sqlTextName = "sqlText"
	)
	var (
		scanBlockWrapper ScanWrapper
		resultExprFn     func(string) []*ast.Field
		lastReturn       ast.Stmt
	)
	switch variant {
	case findVariantOnce:
		scanBlockWrapper = WrapperFindOne
		resultExprFn = simpleResultOneRecord
		lastReturn = builders.Return(
			ast.NewIdent("result"),
			ast.NewIdent(sqlEmptyResultErrorName),
		)
	case findVariantAll:
		scanBlockWrapper = WrapperFindAll
		resultExprFn = simpleResultArray
		lastReturn = builders.ReturnEmpty()
	default:
		panic("cannot resolve 'variant'")
	}
	return func(
		fullTableName, functionName, rowStructName string,
		optionFields, mutableFields, rowFields []dataCellFactory,
	) AstDataChain {
		var (
			fieldRefs, outColumnList = ExtractDestinationFieldRefsFromStruct(ScanDestVariable.String(), rowFields)
		)
		sqlQuery := fmt.Sprintf("update %s set %%s where %%s returning %s", fullTableName, strings.Join(outColumnList, ", "))
		functionBody, inputTypes, inputAttrs := BuildInputValuesProcessor(
			"values",
			makeExportedName(functionName+"Values"),
			mutableFields,
			UpdateBuilderOptions,
		)
		findBlock, findTypes, findAttrs := BuildFindArgumentsProcessor(
			"filter",
			makeExportedName(functionName+"Option"),
			optionFields,
			IncomingArgumentsBuilderOptions,
		)
		functionBody = append(functionBody, findBlock...)
		functionBody = append(
			functionBody,
			builders.Assign(
				builders.MakeVarNames(sqlTextName),
				builders.Assignment,
				builders.Call(
					builders.SprintfFn,
					ast.NewIdent(sqlTextName),
					builders.Call(
						builders.StringsJoinFn,
						ast.NewIdent(FieldsVariable.String()),
						builders.StringConstant(", ").Expr(),
					),
					builders.Add(
						builders.StringConstant("(").Expr(),
						builders.Call(
							builders.StringsJoinFn,
							ast.NewIdent(FiltersVariable.String()),
							builders.StringConstant(") and (").Expr(),
						),
						builders.StringConstant(")").Expr(),
					),
				),
			),
		)
		functionBody = append(
			append(
				functionBody,
				BuildExecutionBlockForFunction(scanBlockWrapper, fieldRefs, MakeExecutionOption(rowStructName, sqlTextName))...,
			),
			lastReturn,
		)
		functionBody = addVariablesToFunctionBody(
			functionBody,
			sqlTextName,
			sqlQuery,
			builders.VariableValue(
				ArgsVariable.String(),
				builders.Call(builders.MakeFn, builders.ArrayType(builders.EmptyInterface), builders.Zero, builders.IntegerConstant(len(mutableFields)).Expr()),
			),
			builders.VariableValue(
				FieldsVariable.String(),
				builders.Call(builders.MakeFn, builders.ArrayType(ast.NewIdent("string")), builders.Zero, builders.IntegerConstant(len(mutableFields)).Expr()),
			),
			builders.VariableValue(
				FiltersVariable.String(),
				builders.Call(builders.MakeFn, builders.ArrayType(ast.NewIdent("string")), builders.Zero, builders.IntegerConstant(len(mutableFields)).Expr()),
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
				functionName: MakeDatabaseApiFunction(functionName, resultExprFn(rowStructName), functionBody, append(inputAttrs, findAttrs...)...),
			},
		}
	}
}

func insertOneBuilder(
	fullTableName, functionName, rowStructName string,
	_, mutableFields, rowFields []dataCellFactory,
) AstDataChain {
	const (
		sqlTextName = "sqlText"
	)
	scanBlockWrapper := WrapperFindOne
	lastReturn := builders.Return(
		ast.NewIdent("result"),
		ast.NewIdent(sqlEmptyResultErrorName),
	)
	var (
		fieldRefs, outColumnList = ExtractDestinationFieldRefsFromStruct(ScanDestVariable.String(), rowFields)
	)
	sqlQuery := fmt.Sprintf("insert into %s (%%s) values (%%s) returning %s", fullTableName, strings.Join(outColumnList, ", "))
	functionBody, functionTypes, functionAttrs := BuildInputValuesProcessor(
		"record",
		makeExportedName(functionName+"Values"),
		mutableFields,
		InsertBuilderOptions,
	)
	functionBody = append(
		functionBody,
		builders.Assign(
			builders.MakeVarNames(sqlTextName),
			builders.Assignment,
			builders.Call(
				builders.SprintfFn,
				ast.NewIdent(sqlTextName),
				builders.Call(
					builders.StringsJoinFn,
					ast.NewIdent(FieldsVariable.String()),
					builders.StringConstant(", ").Expr(),
				),
				builders.Call(
					builders.StringsJoinFn,
					ast.NewIdent(ValuesVariable.String()),
					builders.StringConstant(", ").Expr(),
				),
			),
		),
	)
	functionBody = append(
		append(
			functionBody,
			BuildExecutionBlockForFunction(scanBlockWrapper, fieldRefs, MakeExecutionOption(rowStructName, sqlTextName))...,
		),
		lastReturn,
	)
	functionBody = addVariablesToFunctionBody(
		functionBody,
		sqlTextName,
		sqlQuery,
		builders.VariableValue(
			ArgsVariable.String(),
			builders.Call(builders.MakeFn, builders.ArrayType(builders.EmptyInterface), builders.Zero, builders.IntegerConstant(len(mutableFields)).Expr()),
		),
		builders.VariableValue(
			FieldsVariable.String(),
			builders.Call(builders.MakeFn, builders.ArrayType(ast.NewIdent("string")), builders.Zero, builders.IntegerConstant(len(mutableFields)).Expr()),
		),
		builders.VariableValue(
			ValuesVariable.String(),
			builders.Call(builders.MakeFn, builders.ArrayType(ast.NewIdent("string")), builders.Zero, builders.IntegerConstant(len(mutableFields)).Expr()),
		),
	)
	return AstDataChain{
		Types:     functionTypes,
		Constants: nil,
		Implementations: map[string]*ast.FuncDecl{
			functionName: MakeDatabaseApiFunction(functionName, simpleResultOneRecord(rowStructName), functionBody, functionAttrs...),
		},
	}
}

func upsertBuilder(
	fullTableName, functionName, rowStructName string,
	optionFields, mutableFields, rowFields []dataCellFactory,
) AstDataChain {
	const (
		sqlTextName = "sqlText"
	)
	scanBlockWrapper := WrapperFindOne
	lastReturn := builders.Return(
		ast.NewIdent("result"),
		ast.NewIdent(sqlEmptyResultErrorName),
	)
	var (
		fieldRefs, outColumnList = ExtractDestinationFieldRefsFromStruct(ScanDestVariable.String(), rowFields)
		_, uniqueColumns         = ExtractDestinationFieldRefsFromStruct("", optionFields)
	)
	sqlQuery := fmt.Sprintf("insert into %s (%%s) values (%%s) on conflict (%s) do update set %%s returning %s", fullTableName, strings.Join(uniqueColumns, ","), strings.Join(outColumnList, ", "))
	functionBody, functionTypes, functionAttrs := BuildInputValuesProcessor(
		"record",
		makeExportedName(functionName+"Values"),
		mutableFields,
		InsertBuilderOptions,
	)
	functionBody = append(
		functionBody,
		builders.Assign(
			builders.MakeVarNames("update"),
			builders.Definition,
			builders.Call(
				builders.MakeFn,
				builders.ArrayType(ast.NewIdent("string")),
				builders.Zero,
				builders.Call(builders.LengthFn, ast.NewIdent(FieldsVariable.String())),
			),
		),
		builders.Range(
			true, "i", "", ast.NewIdent(FieldsVariable.String()),
			builders.Assign(
				builders.MakeVarNames("update"),
				builders.Assignment,
				builders.Call(
					builders.AppendFn,
					ast.NewIdent("update"),
					builders.Call(
						builders.SprintfFn,
						builders.StringConstant("%s = %s").Expr(),
						builders.Index(ast.NewIdent(FieldsVariable.String()), builders.VariableName("i")),
						builders.Index(ast.NewIdent(ValuesVariable.String()), builders.VariableName("i")),
					),
				),
			),
		),
		builders.Assign(
			builders.MakeVarNames(sqlTextName),
			builders.Assignment,
			builders.Call(
				builders.SprintfFn,
				ast.NewIdent(sqlTextName),
				builders.Call(
					builders.StringsJoinFn,
					ast.NewIdent(FieldsVariable.String()),
					builders.StringConstant(", ").Expr(),
				),
				builders.Call(
					builders.StringsJoinFn,
					ast.NewIdent(ValuesVariable.String()),
					builders.StringConstant(", ").Expr(),
				),
				builders.Call(
					builders.StringsJoinFn,
					ast.NewIdent("update"),
					builders.StringConstant(", ").Expr(),
				),
			),
		),
	)
	functionBody = append(
		append(
			functionBody,
			BuildExecutionBlockForFunction(scanBlockWrapper, fieldRefs, MakeExecutionOption(rowStructName, sqlTextName))...,
		),
		lastReturn,
	)
	functionBody = addVariablesToFunctionBody(
		functionBody,
		sqlTextName,
		sqlQuery,
		builders.VariableValue(
			ArgsVariable.String(),
			builders.Call(builders.MakeFn, builders.ArrayType(builders.EmptyInterface), builders.Zero, builders.IntegerConstant(len(mutableFields)).Expr()),
		),
		builders.VariableValue(
			FieldsVariable.String(),
			builders.Call(builders.MakeFn, builders.ArrayType(ast.NewIdent("string")), builders.Zero, builders.IntegerConstant(len(mutableFields)).Expr()),
		),
		builders.VariableValue(
			ValuesVariable.String(),
			builders.Call(builders.MakeFn, builders.ArrayType(ast.NewIdent("string")), builders.Zero, builders.IntegerConstant(len(mutableFields)).Expr()),
		),
	)
	return AstDataChain{
		Types:     functionTypes,
		Constants: nil,
		Implementations: map[string]*ast.FuncDecl{
			functionName: MakeDatabaseApiFunction(functionName, simpleResultOneRecord(rowStructName), functionBody, functionAttrs...),
		},
	}
}

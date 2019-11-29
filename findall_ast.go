package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
)

const (
	varArgsName     = "args"
	varOptionsName  = "options"
	varSqlQueryArgs = "sqlArgs"
)

// get a list of table columns and string field descriptors for the output structure. column and field positions correspond to each other
func extractFieldRefsAndColumnsFromStruct(varName string, rowFields []*ast.Field) (fieldRefs []ast.Expr, columnNames []string) {
	var fieldNames []string
	fieldRefs = make([]ast.Expr, 0, len(rowFields))
	fieldNames, columnNames = extractFieldsAndColumnsFromStruct(rowFields)
	for _, fieldName := range fieldNames {
		fieldRefs = append(fieldRefs, makeName(fieldName))
	}
	return
}

func makeArrayQueryOption(fieldName, columnName, operator string, ci bool) []ast.Stmt {
	const (
		localVariable = "opt"
	)
	var optionExpr ast.Expr = makeName(localVariable)
	if ci {
		columnName = fmt.Sprintf("lower(%s)", columnName)
		optionExpr = makeCall(makeTypeSelector("strings", "ToLower"), optionExpr)
	}
	// for placeholders only
	var arrVariableName = fmt.Sprintf("array%s", fieldName)
	return []ast.Stmt{
		makeVarStatement(makeVarType(arrVariableName, makeTypeArray(makeName("string")))),
		&ast.RangeStmt{
			Key:   makeName("_"),
			Value: makeName(localVariable),
			X:     makeTypeSelector(varOptionsName, fieldName),
			Tok:   token.DEFINE,
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					makeAssignment([]string{varArgsName}, makeCall(makeName("append"), makeName(varArgsName), optionExpr)),
					makeAssignment(
						[]string{arrVariableName},
						makeCall(
							makeName("append"),
							makeName(arrVariableName),
							makeAddExpressions(
								makeBasicLiteralString("$"),
								makeCall(
									makeTypeSelector("strconv", "Itoa"),
									makeCall(makeName("len"), makeName(varArgsName)),
								),
							),
						),
					),
				},
			},
		},
		&ast.IfStmt{
			Cond: makeNotEmptyArrayExpression(arrVariableName),
			Body: makeBlock(
				makeAssignment(
					[]string{varSqlQueryArgs},
					makeCall(
						makeName("append"),
						makeName(varSqlQueryArgs),
						makeCall(
							makeTypeSelector("fmt", "Sprintf"),
							makeBasicLiteralString(operator),
							makeBasicLiteralString(columnName),
							makeCall(
								makeTypeSelector("strings", "Join"),
								makeName(arrVariableName),
								makeBasicLiteralString(", "),
							),
						),
					),
				),
			),
		},
	}
}

func makeUnionQueryOption(fieldName string, columnNames []string, operator string, ci bool) []ast.Stmt {
	var optionExpr = makeTypeSelector(varOptionsName, fieldName)
	if ci {
		for i, c := range columnNames {
			columnNames[i] = fmt.Sprintf("lower(%s)", c)
		}
		optionExpr = makeCall(makeTypeSelector("strings", "ToLower"), optionExpr)
	}
	operators := make([]string, 0, len(operator))
	for _, _ = range columnNames {
		operators = append(operators, operator)
	}
	callArgs := make([]ast.Expr, 0, len(columnNames)*2)
	for _, c := range columnNames {
		callArgs = append(
			callArgs,
			makeBasicLiteralString(c),
			makeAddExpressions(
				makeBasicLiteralString("$"),
				makeCall(
					makeTypeSelector("strconv", "Itoa"),
					makeCall(makeName("len"), makeName(varArgsName)),
				),
			),
		)
	}
	return []ast.Stmt{
		makeAssignment(
			[]string{varArgsName},
			makeCall(
				makeName("append"),
				makeName(varArgsName),
				optionExpr,
			),
		),
		makeAssignment(
			[]string{varSqlQueryArgs},
			makeCall(
				makeName("append"),
				makeName(varSqlQueryArgs),
				makeCall(
					makeTypeSelector("fmt", "Sprintf"),
					append([]ast.Expr{makeBasicLiteralString(strings.Join(operators, " or "))}, callArgs...)...,
				),
			),
		),
	}
}

func makeScalarQueryOption(fieldName, columnName, operator string, ci, ref bool) []ast.Stmt {
	var optionExpr = makeTypeSelector(varOptionsName, fieldName)
	if ref {
		optionExpr = makeTypeStar(optionExpr)
	}
	if ci {
		columnName = fmt.Sprintf("lower(%s)", columnName)
		optionExpr = makeCall(makeTypeSelector("strings", "ToLower"), optionExpr)
	}
	return []ast.Stmt{
		makeAssignment(
			[]string{varArgsName},
			makeCall(
				makeName("append"),
				makeName(varArgsName),
				optionExpr,
			),
		),
		makeAssignment(
			[]string{varSqlQueryArgs},
			makeCall(
				makeName("append"),
				makeName(varSqlQueryArgs),
				makeCall(
					makeTypeSelector("fmt", "Sprintf"),
					makeBasicLiteralString(operator),
					makeBasicLiteralString(columnName),
					makeAddExpressions(
						makeBasicLiteralString("$"),
						makeCall(
							makeTypeSelector("strconv", "Itoa"),
							makeCall(makeName("len"), makeName(varArgsName)),
						),
					),
				),
			),
		),
	}
}

func makeStarQueryOption(fieldName, columnName, operator string, ci bool) []ast.Stmt {
	return []ast.Stmt{
		&ast.IfStmt{
			Cond: makeNotEqualExpression(makeTypeSelector(varOptionsName, fieldName), makeName("nil")),
			Body: makeBlock(
				makeScalarQueryOption(fieldName, columnName, operator, ci, true)...,
			),
		},
	}
}

type (
	findVariant int
	scanWrapper func(...ast.Stmt) ast.Stmt
)

const (
	findVariantOnce findVariant = iota
	findVariantAll
)

func scanBlockForFindOnce(stmts ...ast.Stmt) ast.Stmt {
	return &ast.IfStmt{
		Cond: makeCall(makeTypeSelector("rows", "Next")),
		Body: makeBlock(
			append(
				append(
					[]ast.Stmt{
						makeAssignmentWithErrChecking(
							"",
							makeCall(
								makeTypeSelector("rows", "Err"),
							),
						),
					},
					stmts...,
				),
				&ast.IfStmt{
					Cond: makeCall(makeTypeSelector("rows", "Next")),
					Body: makeBlock(
						makeReturn(
							makeName("row"),
							makeName("SingletonViolation"),
						),
					),
					Else: makeReturn(
						makeName("row"),
						makeName("nil"),
					),
				},
			)...,
		),
	}
}

func scanBlockForFindAll(stmts ...ast.Stmt) ast.Stmt {
	return &ast.ForStmt{
		Cond: makeCall(makeTypeSelector("rows", "Next")),
		Body: makeBlock(
			append(
				append(
					[]ast.Stmt{
						makeAssignmentWithErrChecking(
							"",
							makeCall(
								makeTypeSelector("rows", "Err"),
							),
						),
					},
					stmts...,
				),
				makeAssignment(
					[]string{"result"},
					makeCall(
						makeName("append"),
						makeName("result"),
						makeName("row"),
					),
				),
			)...,
		),
	}
}
func addExecutionBlockToFunctionBody(functionBody []ast.Stmt, rowStructName string, scanBlock scanWrapper, fieldRefs []ast.Expr, lastReturn ast.Stmt) []ast.Stmt {
	return append(
		functionBody,
		makeAssignmentWithErrChecking(
			"rows",
			makeCallEllipsis(
				makeTypeSelector("db", "Query"),
				makeName("sqlText"),
				makeName(varArgsName),
			),
		),
		scanBlock(
			makeVarStatement(makeVarType("row", makeName(rowStructName))),
			makeAssignmentWithErrChecking(
				"",
				makeCall(
					makeTypeSelector("rows", "Scan"),
					fieldRefs...,
				),
			),
		),
		lastReturn,
	)
}

func addVariablesToFunctionBody(functionBody []ast.Stmt, sqlQuery string) []ast.Stmt {
	return append(
		functionBody,
		makeVarStatement(
			makeVarType("db", makeTypeStar(makeTypeSelector("sql", "DB"))),
			makeVarType("rows", makeTypeStar(makeTypeSelector("sql", "Rows"))),
			makeVarValue(
				varArgsName,
				makeCall(makeName("make"), makeTypeArray(makeEmptyInterface()), makeBasicLiteralInteger(0)),
			),
			makeVarValue(
				varSqlQueryArgs,
				makeCall(makeName("make"), makeTypeArray(makeName("string")), makeBasicLiteralInteger(0)),
			),
			makeVarValue(
				"sqlText",
				makeBasicLiteralString(sqlQuery),
			),
		),
		makeAssignmentWithErrChecking(
			"db",
			makeCall(makeName("getDatabase"), makeName("ctx")),
			makeEmptyReturn(),
		),
	)
}

func addInsertParametersToFunctionBody(functionBody []ast.Stmt, optionFields []*ast.Field) []ast.Stmt {
	functionBody = append(
		functionBody,
		makeDefinition(
			[]string{"colNames"},
			makeCall(
				makeName("make"),
				makeTypeArray(makeName("string")),
				makeBasicLiteralInteger(0),
			),
		),
	)
	for _, field := range optionFields {
		tags := tagToMap(field.Tag.Value)
		colName := tags[TagTypeSQL][0]
		// TODO encrypt := arrayFind(tags[TagTypeSQL], tagEncrypt) > 0
		optionExpr := []ast.Stmt{
			makeAssignment(
				[]string{"colNames"},
				makeCall(
					makeName("append"),
					makeName("colNames"),
					makeBasicLiteralString(colName),
				),
			),
			makeAssignment(
				[]string{varArgsName},
				makeCall(
					makeName("append"),
					makeName(varArgsName),
					makeTypeSelector(varOptionsName, field.Names[0].Name),
				),
			),
			makeAssignment(
				[]string{varSqlQueryArgs},
				makeCall(
					makeName("append"),
					makeName(varSqlQueryArgs),
					makeCall(
						makeTypeSelector("fmt", "Sprintf"),
						makeBasicLiteralString("$%d"),
						makeCall(
							makeName("len"),
							makeName(varArgsName),
						),
					),
				),
			),
		}
		if _, ok := field.Type.(*ast.StarExpr); ok {
			optionExpr = []ast.Stmt{
				&ast.IfStmt{
					Cond: makeNotEmptyArrayExpression(varArgsName),
					Body: makeBlock(optionExpr...),
				},
			}
		}
		functionBody = append(
			functionBody,
			optionExpr...,
		)
	}
	return append(
		functionBody,
		&ast.IfStmt{
			Cond: makeNotEmptyArrayExpression(varSqlQueryArgs),
			Body: makeBlock(
				makeAddAssignment(
					[]string{"sqlText"},
					makeCall(
						makeTypeSelector("fmt", "Sprintf"),
						makeName("sqlText"),
						makeCall(
							makeTypeSelector("strings", "Join"),
							makeName(varSqlQueryArgs),
							makeBasicLiteralString(", "),
						),
						makeCall(
							makeTypeSelector("strings", "Join"),
							makeName("colNames"),
							makeBasicLiteralString(", "),
						),
					),
				),
			),
		},
	)
}

func addDynamicParametersToFunctionBody(functionBody []ast.Stmt, optionFields []*ast.Field) []ast.Stmt {
	for _, field := range optionFields {
		tags := tagToMap(field.Tag.Value)
		colName := tags[TagTypeSQL][0]
		ci := arrayFind(tags[TagTypeSQL], tagCaseInsensitive) > 0
		opTagValue, ok := tags[TagTypeOp]
		if !ok || len(opTagValue) < 1 {
			opTagValue = []string{string(CompareEqual)}
		}
		operator := sqlCompareOperator(opTagValue[0])
		if arrayFind(tags[TagTypeSQL], TagTypeUnion) > 0 {
			columns := tags[TagTypeUnion]
			if operator.isMult() {
				panic(fmt.Sprintf("joins cannot be used in multiple expressions, for example '%s' in the expression '%s'", field.Names[0].Name, opTagValue[0]))
			}
			functionBody = append(
				functionBody,
				makeUnionQueryOption(field.Names[0].Name, columns, operator.getRawExpression(), ci)...,
			)
		} else {
			if operator.isMult() {
				functionBody = append(
					functionBody,
					makeArrayQueryOption(field.Names[0].Name, colName, operator.getRawExpression(), ci)...,
				)
			} else {
				if _, ok := field.Type.(*ast.StarExpr); ok {
					functionBody = append(
						functionBody,
						makeStarQueryOption(field.Names[0].Name, colName, operator.getRawExpression(), ci)...,
					)
				} else {
					functionBody = append(
						functionBody,
						makeScalarQueryOption(field.Names[0].Name, colName, operator.getRawExpression(), ci, false)...,
					)
				}
			}
		}
	}
	return append(
		functionBody,
		&ast.IfStmt{
			Cond: makeNotEmptyArrayExpression(varSqlQueryArgs),
			Body: makeBlock(
				makeAddAssignment(
					[]string{"sqlText"},
					makeAddExpressions(
						makeBasicLiteralString(" where ("),
						makeCall(
							makeTypeSelector("strings", "Join"),
							makeName(varSqlQueryArgs),
							makeBasicLiteralString(") and ("),
						),
						makeBasicLiteralString(")"),
					),
				),
			),
		},
	)
}

func makeFindFunction(variant findVariant) ApiFuncBuilder {
	const (
		scanVarName = "row"
	)
	return func(
		fullTableName, functionName, rowStructName string,
		optionFields, rowFields []*ast.Field,
	) *ast.File {
		var (
			scanBlockWrapper scanWrapper
			resultExpr       ast.Expr
			lastReturn       ast.Stmt
		)
		switch variant {
		case findVariantOnce:
			scanBlockWrapper = scanBlockForFindOnce
			resultExpr = makeTypeIdent(rowStructName)
			lastReturn = makeReturn(
				makeName("result"),
				makeName("EmptyResult"),
			)
		case findVariantAll:
			scanBlockWrapper = scanBlockForFindAll
			resultExpr = makeTypeArray(makeTypeIdent(rowStructName))
			lastReturn = makeEmptyReturn()
		default:
			panic("cannot resolve 'variant'")
		}
		var (
			fieldRefs, columnList = extractFieldRefsAndColumnsFromStruct(scanVarName, rowFields)
		)
		sqlQuery := fmt.Sprintf("select %s from %s ", strings.Join(columnList, ", "), fullTableName)
		functionBody := make([]ast.Stmt, 0, len(optionFields)*3+6)
		functionBody = addVariablesToFunctionBody(functionBody, sqlQuery)
		functionBody = addDynamicParametersToFunctionBody(functionBody, optionFields)
		functionBody = addExecutionBlockToFunctionBody(functionBody, rowStructName, scanBlockWrapper, fieldRefs, lastReturn)
		astFileDecls := []ast.Decl{
			// TODO generate import from function declaration automatically
			makeImportDecl(
				"database/sql",
				"fmt",
				"strconv",
				"strings",
				"context",
			),
			makeApiFunction(functionName, resultExpr, functionBody),
		}
		return &ast.File{
			Name:  makeName("generated"),
			Decls: astFileDecls,
		}
	}
}

func makeInsertFunction(variant findVariant) ApiFuncBuilder {
	const (
		scanVarName = "row"
	)
	return func(
		fullTableName, functionName, rowStructName string,
		optionFields, rowFields []*ast.Field,
	) *ast.File {
		resultExpr := makeTypeIdent(rowStructName)
		scanBlockWrapper := scanBlockForFindOnce
		lastReturn := makeReturn(
			makeName("result"),
			makeName("EmptyResult"),
		)
		var (
			fieldRefs, outColumnList = extractFieldRefsAndColumnsFromStruct(scanVarName, rowFields)
		)
		sqlQuery := fmt.Sprintf("insert into %s (%%s) values (%%s) returning %s", fullTableName, strings.Join(outColumnList, ", "))
		functionBody := make([]ast.Stmt, 0, len(optionFields)*3+6)
		functionBody = addVariablesToFunctionBody(functionBody, sqlQuery)
		functionBody = addInsertParametersToFunctionBody(functionBody, optionFields)
		functionBody = addExecutionBlockToFunctionBody(functionBody, rowStructName, scanBlockWrapper, fieldRefs, lastReturn)

		astFileDecls := []ast.Decl{
			// TODO generate import from function declaration automatically
			makeImportDecl(
				"database/sql",
				"fmt",
				"strconv",
				"strings",
				"context",
			),
			makeApiFunction(functionName, resultExpr, functionBody),
		}
		return &ast.File{
			Name:  makeName("generated"),
			Decls: astFileDecls,
		}
	}
}

func makeApiFunction(functionName string, resultExpr ast.Expr, functionBody []ast.Stmt) *ast.FuncDecl {
	return &ast.FuncDecl{
		Name: makeName(functionName),
		Type: &ast.FuncType{
			Params: &ast.FieldList{
				List: []*ast.Field{
					makeField("ctx", nil, makeTypeSelector("context", "Context"), nil),
					makeField(varOptionsName, nil, makeTypeIdent(functionName+"Option"), nil),
				},
			},
			Results: &ast.FieldList{
				List: []*ast.Field{
					makeField("result", nil, resultExpr, nil),
					makeField("err", nil, makeTypeIdent("error"), nil),
				},
			},
		},
		Body: &ast.BlockStmt{
			List: functionBody,
		},
	}
}

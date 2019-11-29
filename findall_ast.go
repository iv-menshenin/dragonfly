package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
)

const (
	varArgsName    = "args"
	varOptionsName = "options"
)

// get a list of table columns and string field descriptors for the output structure. column and field positions correspond to each other
func extractFieldRefsAndColumnsFromStruct(varName string, rowFields []*ast.Field) (fieldRefs []ast.Expr, columnNames []string) {
	var fieldNames []string
	fieldRefs = make([]ast.Expr, 0, len(rowFields))
	columnNames, fieldNames = extractFieldsAndColumnsFromStruct(rowFields)
	for _, fieldName := range fieldNames {
		fieldRefs = append(fieldRefs, makeRef(makeTypeSelector(varOptionsName, fieldName)))
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
		// TODO can we make it another way? shortest
		&ast.RangeStmt{
			Key: &ast.Ident{
				Name: "_",
				Obj: &ast.Object{
					Kind: ast.Var,
					Name: "_",
					Decl: makeDefinition(
						[]string{localVariable},
						makeRangeExpression(makeTypeSelector(varOptionsName, fieldName)),
					),
				},
			},
			Tok: token.DEFINE,
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
					[]string{"sqlWhere"},
					makeCall(
						makeName("append"),
						makeName("sqlWhere"),
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
			[]string{"sqlWhere"},
			makeCall(
				makeName("append"),
				makeName("sqlWhere"),
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
			[]string{"sqlWhere"},
			makeCall(
				makeName("append"),
				makeName("sqlWhere"),
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

func findAllAST(
	fullTableName, functionName, rowStructName string,
	optionFields, rowFields []*ast.Field,
) *ast.File {
	const (
		scanVarName = "row"
	)
	var (
		fieldRefs, columnList = extractFieldRefsAndColumnsFromStruct(scanVarName, rowFields)
	)
	sqlQuery := fmt.Sprintf("select %s from %s ", strings.Join(columnList, ", "), fullTableName)
	functionBody := make([]ast.Stmt, 0, len(optionFields)*3+6)
	functionBody = append(
		functionBody,
		makeVarStatement(
			makeVarType("db", makeTypeStar(makeTypeSelector("sql", "DB"))),
			makeVarType("rows", makeTypeStar(makeTypeSelector("sql", "Rows"))),
			makeVarValue(
				varArgsName,
				makeCall(makeName("make"), makeTypeArray(makeEmptyInterface()), makeBasicLiteralInteger(0)),
			),
			makeVarValue(
				"sqlWhere",
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
	// ARGUMENTS
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
	// /ARGUMENTS
	functionBody = append(
		functionBody,
		&ast.IfStmt{
			Cond: makeNotEmptyArrayExpression("sqlWhere"),
			Body: makeBlock(
				makeAddAssignment(
					[]string{"sqlText"},
					makeAddExpressions(
						makeBasicLiteralString(" where ("),
						makeCall(
							makeTypeSelector("strings", "Join"),
							makeName("sqlWhere"),
							makeBasicLiteralString(") and ("),
						),
						makeBasicLiteralString(")"),
					),
				),
			),
		},
		makeAssignmentWithErrChecking(
			"rows",
			makeCall(
				makeTypeSelector("db", "Query"),
				makeName("sqlText"),
				makeName(varArgsName), // TODO ellipsis?
			),
		),
		&ast.ForStmt{
			Cond: makeCall(makeTypeSelector("rows", "Next")),
			Body: makeBlock(
				makeAssignmentWithErrChecking(
					"",
					makeCall(
						makeTypeSelector("rows", "Err"),
					),
				),
				makeVarStatement(makeVarType("row", makeName("CoreShopsRow"))),
				makeAssignmentWithErrChecking(
					"",
					makeCall(
						makeTypeSelector("rows", "Scan"),
						fieldRefs...,
					),
				),
				makeAssignment(
					[]string{"result"},
					makeCall(
						makeName("append"),
						makeName("result"),
						makeName("row"),
					),
				),
			),
		},
		makeEmptyReturn(),
	)
	astFileDecls := []ast.Decl{
		makeImportDecl(
			"database/sql",
			"fmt",
			"strconv",
			"strings",
			"context",
		),
		&ast.FuncDecl{
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
						makeField("result", nil, makeTypeArray(makeTypeIdent(rowStructName)), nil),
						makeField("err", nil, makeTypeIdent("error"), nil),
					},
				},
			},
			Body: &ast.BlockStmt{
				List: functionBody,
			},
		},
	}
	return &ast.File{
		Name:  makeName("generated"),
		Decls: astFileDecls,
	}
}

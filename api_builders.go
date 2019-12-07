package dragonfly

import (
	"fmt"
	"go/ast"
	"go/token"
	"regexp"
	"strings"
)

const (
	varArgValues        = "args"
	varFindOptionsName  = "find"
	varInputOptionsName = "values"
	varArgPlaceholders  = "sqlArgs"
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

func makeArrayQueryOption(
	optionName, fieldName, columnName, operator string,
	ci bool,
) []ast.Stmt {
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
			X:     makeTypeSelector(optionName, fieldName),
			Tok:   token.DEFINE,
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					makeAssignment([]string{varArgValues}, makeCall(makeName("append"), makeName(varArgValues), optionExpr)),
					makeAssignment(
						[]string{arrVariableName},
						makeCall(
							makeName("append"),
							makeName(arrVariableName),
							makeAddExpressions(
								makeBasicLiteralString("$"),
								makeCall(
									makeTypeSelector("strconv", "Itoa"),
									makeCall(makeName("len"), makeName(varArgValues)),
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
					[]string{varArgPlaceholders},
					makeCall(
						makeName("append"),
						makeName(varArgPlaceholders),
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

func makeUnionQueryOption(
	structName, fieldName string,
	columnNames []string,
	operator string,
	ci bool,
) []ast.Stmt {
	var optionExpr = makeTypeSelector(structName, fieldName)
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
					makeCall(makeName("len"), makeName(varArgValues)),
				),
			),
		)
	}
	return []ast.Stmt{
		makeAssignment(
			[]string{varArgValues},
			makeCall(
				makeName("append"),
				makeName(varArgValues),
				optionExpr,
			),
		),
		makeAssignment(
			[]string{varArgPlaceholders},
			makeCall(
				makeName("append"),
				makeName(varArgPlaceholders),
				makeCall(
					makeTypeSelector("fmt", "Sprintf"),
					append([]ast.Expr{makeBasicLiteralString(strings.Join(operators, " or "))}, callArgs...)...,
				),
			),
		),
	}
}

func makeScalarQueryOption(optionName, fieldName, columnName, operator string, ci, ref bool) []ast.Stmt {
	var optionExpr = makeTypeSelector(optionName, fieldName)
	if ref {
		optionExpr = makeTypeStar(optionExpr)
	}
	if ci {
		columnName = fmt.Sprintf("lower(%s)", columnName)
		optionExpr = makeCall(makeTypeSelector("strings", "ToLower"), optionExpr)
	}
	return []ast.Stmt{
		makeAssignment(
			[]string{varArgValues},
			makeCall(
				makeName("append"),
				makeName(varArgValues),
				optionExpr,
			),
		),
		makeAssignment(
			[]string{varArgPlaceholders},
			makeCall(
				makeName("append"),
				makeName(varArgPlaceholders),
				makeCall(
					makeTypeSelector("fmt", "Sprintf"),
					makeBasicLiteralString(operator),
					makeBasicLiteralString(columnName),
					makeAddExpressions(
						makeBasicLiteralString("$"),
						makeCall(
							makeTypeSelector("strconv", "Itoa"),
							makeCall(makeName("len"), makeName(varArgValues)),
						),
					),
				),
			),
		),
	}
}

func makeStarQueryOption(optionName, fieldName, columnName, operator string, ci bool) []ast.Stmt {
	return []ast.Stmt{
		&ast.IfStmt{
			Cond: makeNotEqualExpression(makeTypeSelector(optionName, fieldName), makeName("nil")),
			Body: makeBlock(
				makeScalarQueryOption(optionName, fieldName, columnName, operator, ci, true)...,
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
				makeName(varArgValues),
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

func addVariablesToFunctionBody(functionBody []ast.Stmt, optionsCount int, sqlQuery string) []ast.Stmt {
	return append(
		functionBody,
		makeVarStatement(
			makeVarType("db", makeTypeStar(makeTypeSelector("sql", "DB"))),
			makeVarType("rows", makeTypeStar(makeTypeSelector("sql", "Rows"))),
			makeVarValue(
				varArgValues,
				makeCall(makeName("make"), makeTypeArray(makeEmptyInterface()), makeBasicLiteralInteger(0), makeBasicLiteralInteger(optionsCount)),
			),
			makeVarValue(
				varArgPlaceholders,
				makeCall(makeName("make"), makeTypeArray(makeName("string")), makeBasicLiteralInteger(0), makeBasicLiteralInteger(optionsCount)),
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

func processValueWrapper(
	colName string,
	field ast.Expr,
) []ast.Stmt {
	return []ast.Stmt{
		makeAssignment(
			[]string{"colNames"},
			makeCall(makeName("append"), makeName("colNames"), makeBasicLiteralString(colName)),
		),
		makeAssignment(
			[]string{varArgValues},
			makeCall(makeName("append"), makeName(varArgValues), field),
		),
		makeAssignment(
			[]string{varArgPlaceholders},
			makeCall(
				makeName("append"),
				makeName(varArgPlaceholders),
				makeCall(
					makeTypeSelector("fmt", "Sprintf"),
					makeBasicLiteralString("$%d"),
					makeCall(makeName("len"), makeName(varArgValues)),
				),
			),
		),
	}
}

var (
	fncTemplate = regexp.MustCompile(`^(\w+)\(([^)]*)\)$`)
)

func doFuncPicker(funcName string, funcArgs ...string) ast.Expr {
	switch funcName {
	case tagGenerate:
		if len(funcArgs) == 0 {
			panic("tag contains 'generate' function without any argument")
		}
		if strings.EqualFold(funcArgs[0], generateFunction) {
			return makeCall(makeTypeSelector("time", "Now"))
		}
	}
	return nil
}

func makeValuePicker(tags []string, def ast.Expr) (ast.Expr, bool) {
	for _, tag := range tags {
		sub := fncTemplate.FindAllStringSubmatch(tag, -1)
		if len(sub) > 0 {
			funcName := sub[0][1]
			funcArgs := strings.Split(sub[0][2], ";")
			if expr := doFuncPicker(funcName, funcArgs...); expr != nil {
				return expr, true
			}
		}
	}
	return def, false
}

func addInsertParametersToFunctionBody(
	functionName string,
	functionBody []ast.Stmt,
	optionFields []*ast.Field,
) (
	[]ast.Stmt,
	[]ast.Spec,
	[]*ast.Field,
) {
	funcInputOptionName := varInputOptionsName
	funcInputOptionTypeName := makeExportedName(functionName + "Values")
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
	var optionStructFields = make([]*ast.Field, 0, len(optionFields))
	for _, field := range optionFields {
		var (
			tags      = tagToMap(field.Tag.Value)
			colName   = tags[TagTypeSQL][0]
			fieldName = makeTypeSelector(funcInputOptionName, field.Names[0].Name)
		)
		valueExpr, removeField := makeValuePicker(tags[TagTypeSQL][1:], fieldName)
		if !removeField {
			optionStructFields = append(optionStructFields, field)
		}
		// TODO encrypt := arrayFind(tags[TagTypeSQL], tagEncrypt) > 0
		wrapFunc := func(stmts []ast.Stmt) []ast.Stmt { return stmts }
		if _, ok := field.Type.(*ast.StarExpr); !removeField && ok {
			wrapFunc = func(stmts []ast.Stmt) []ast.Stmt {
				return []ast.Stmt{
					&ast.IfStmt{
						Cond: makeNotNullExpression(fieldName),
						Body: makeBlock(stmts...),
					},
				}
			}
		}
		functionBody = append(
			functionBody,
			wrapFunc(processValueWrapper(
				colName, valueExpr,
			))...,
		)
	}
	return append(
			functionBody,
			&ast.IfStmt{
				Cond: makeNotEmptyArrayExpression(varArgPlaceholders),
				Body: makeBlock(
					makeAddAssignment(
						[]string{"sqlText"},
						makeCall(
							makeTypeSelector("fmt", "Sprintf"),
							makeName("sqlText"),
							makeCall(
								makeTypeSelector("strings", "Join"),
								makeName(varArgPlaceholders),
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
		),
		[]ast.Spec{
			&ast.TypeSpec{
				Name: makeName(funcInputOptionTypeName),
				Type: &ast.StructType{
					Fields:     &ast.FieldList{List: optionStructFields},
					Incomplete: false,
				},
			},
		},
		[]*ast.Field{
			{
				Names: []*ast.Ident{makeName(funcInputOptionName)},
				Type:  makeName(funcInputOptionTypeName),
			},
		}
}

func addDynamicParametersToFunctionBody(
	functionName string,
	functionBody []ast.Stmt,
	optionFields []*ast.Field,
) (
	[]ast.Stmt,
	[]ast.Spec,
	[]*ast.Field,
) {
	funcOptionName := varFindOptionsName
	funcOptionTypeName := makeExportedName(functionName + "Option")
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
				makeUnionQueryOption(funcOptionName, field.Names[0].Name, columns, operator.getRawExpression(), ci)...,
			)
		} else {
			if operator.isMult() {
				functionBody = append(
					functionBody,
					makeArrayQueryOption(funcOptionName, field.Names[0].Name, colName, operator.getRawExpression(), ci)...,
				)
			} else {
				if _, ok := field.Type.(*ast.StarExpr); ok {
					functionBody = append(
						functionBody,
						makeStarQueryOption(funcOptionName, field.Names[0].Name, colName, operator.getRawExpression(), ci)...,
					)
				} else {
					functionBody = append(
						functionBody,
						makeScalarQueryOption(funcOptionName, field.Names[0].Name, colName, operator.getRawExpression(), ci, false)...,
					)
				}
			}
		}
	}
	return append(
			functionBody,
			&ast.IfStmt{
				Cond: makeNotEmptyArrayExpression(varArgPlaceholders),
				Body: makeBlock(
					makeAddAssignment(
						[]string{"sqlText"},
						makeAddExpressions(
							makeBasicLiteralString(" where ("),
							makeCall(
								makeTypeSelector("strings", "Join"),
								makeName(varArgPlaceholders),
								makeBasicLiteralString(") and ("),
							),
							makeBasicLiteralString(")"),
						),
					),
				),
			},
		),
		[]ast.Spec{
			&ast.TypeSpec{
				Name: makeName(funcOptionTypeName),
				Type: &ast.StructType{
					Fields:     &ast.FieldList{List: optionFields},
					Incomplete: false,
				},
			},
		},
		[]*ast.Field{
			{
				Names: []*ast.Ident{makeName(funcOptionName)},
				Type:  makeName(funcOptionTypeName),
			},
		}
}

func makeFindFunction(variant findVariant) ApiFuncBuilder {
	const (
		scanVarName = "row"
	)
	return func(
		fullTableName, functionName, rowStructName string,
		optionFields, _, rowFields []*ast.Field,
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
			findTypes             []ast.Spec
			findAttrs             []*ast.Field
			fieldRefs, columnList = extractFieldRefsAndColumnsFromStruct(scanVarName, rowFields)
		)
		sqlQuery := fmt.Sprintf("select %s from %s ", strings.Join(columnList, ", "), fullTableName)
		functionBody := make([]ast.Stmt, 0, len(optionFields)*3+6)
		functionBody = addVariablesToFunctionBody(functionBody, len(optionFields), sqlQuery)
		functionBody, findTypes, findAttrs = addDynamicParametersToFunctionBody(functionName, functionBody, optionFields)
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
			&ast.GenDecl{
				Tok:   token.TYPE,
				Specs: findTypes,
			},
			makeApiFunction(functionName, resultExpr, functionBody, findAttrs...),
		}
		return &ast.File{
			Name:  makeName("generated"),
			Decls: astFileDecls,
		}
	}
}

func makeDeleteFunction(variant findVariant) ApiFuncBuilder {
	const (
		scanVarName = "row"
	)
	return func(
		fullTableName, functionName, rowStructName string,
		optionFields, _, rowFields []*ast.Field,
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
			findTypes             []ast.Spec
			findAttrs             []*ast.Field
			fieldRefs, columnList = extractFieldRefsAndColumnsFromStruct(scanVarName, rowFields)
		)
		sqlQuery := fmt.Sprintf("delete from %s ", fullTableName)
		functionBody := make([]ast.Stmt, 0, len(optionFields)*3+6)
		functionBody = addVariablesToFunctionBody(functionBody, len(optionFields), sqlQuery)
		functionBody, findTypes, findAttrs = addDynamicParametersToFunctionBody(functionName, functionBody, optionFields)
		functionBody = append(functionBody, makeAssignment(
			[]string{"sqlText"},
			makeAddExpressions(makeName("sqlText"), makeBasicLiteralString(fmt.Sprintf(" returning %s", strings.Join(columnList, ", ")))),
		))
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
			&ast.GenDecl{
				Tok:   token.TYPE,
				Specs: findTypes,
			},
			makeApiFunction(functionName, resultExpr, functionBody, findAttrs...),
		}
		return &ast.File{
			Name:  makeName("generated"),
			Decls: astFileDecls,
		}
	}
}

func updateOneBuilder(
	fullTableName, functionName, rowStructName string,
	optionFields, mutableFields, rowFields []*ast.Field,
) *ast.File {
	const (
		scanVarName = "row"
	)
	resultExpr := makeTypeIdent(rowStructName)
	scanBlockWrapper := scanBlockForFindOnce
	lastReturn := makeReturn(
		makeName("result"),
		makeName("EmptyResult"),
	)
	var (
		inputTypes, findTypes    []ast.Spec
		inputAttrs, findAttrs    []*ast.Field
		fieldRefs, outColumnList = extractFieldRefsAndColumnsFromStruct(scanVarName, rowFields)
	)
	sqlQuery := fmt.Sprintf("update %s set %%s where %%s returning %s", fullTableName, strings.Join(outColumnList, ", "))
	functionBody := make([]ast.Stmt, 0, len(optionFields)*3+6)
	functionBody = addVariablesToFunctionBody(functionBody, len(optionFields), sqlQuery)
	functionBody, inputTypes, inputAttrs = addInsertParametersToFunctionBody(functionName, functionBody, mutableFields)
	functionBody, findTypes, findAttrs = addDynamicParametersToFunctionBody(functionName, functionBody, optionFields)
	functionBody = addExecutionBlockToFunctionBody(functionBody, rowStructName, scanBlockWrapper, fieldRefs, lastReturn)

	astFileDecls := []ast.Decl{
		makeImportDecl(
			"database/sql",
			"fmt",
			"strconv",
			"strings",
			"context",
		),
		&ast.GenDecl{
			Tok:   token.TYPE,
			Specs: append(inputTypes, findTypes...),
		},
		makeApiFunction(functionName, resultExpr, functionBody, append(inputAttrs, findAttrs...)...),
	}
	return &ast.File{
		Name:  makeName("generated"),
		Decls: astFileDecls,
	}
}

func insertOneBuilder(
	fullTableName, functionName, rowStructName string,
	_, mutableFields, rowFields []*ast.Field,
) *ast.File {
	const (
		scanVarName = "row"
	)
	resultExpr := makeTypeIdent(rowStructName)
	scanBlockWrapper := scanBlockForFindOnce
	lastReturn := makeReturn(
		makeName("result"),
		makeName("EmptyResult"),
	)
	var (
		functionTypes            []ast.Spec
		functionAttrs            []*ast.Field
		fieldRefs, outColumnList = extractFieldRefsAndColumnsFromStruct(scanVarName, rowFields)
	)
	sqlQuery := fmt.Sprintf("insert into %s (%%s) values (%%s) returning %s", fullTableName, strings.Join(outColumnList, ", "))
	functionBody := make([]ast.Stmt, 0, len(mutableFields)*3+6)
	functionBody = addVariablesToFunctionBody(functionBody, len(mutableFields), sqlQuery)
	functionBody, functionTypes, functionAttrs = addInsertParametersToFunctionBody(functionName, functionBody, mutableFields)
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
		&ast.GenDecl{
			Tok:   token.TYPE,
			Specs: functionTypes,
		},
		makeApiFunction(functionName, resultExpr, functionBody, functionAttrs...),
	}
	return &ast.File{
		Name:  makeName("generated"),
		Decls: astFileDecls,
	}
}

func makeApiFunction(
	functionName string,
	resultExpr ast.Expr,
	functionBody []ast.Stmt,
	functionArgs ...*ast.Field,
) *ast.FuncDecl {
	return &ast.FuncDecl{
		Name: makeName(functionName),
		Type: &ast.FuncType{
			Params: &ast.FieldList{
				List: append(
					[]*ast.Field{
						makeField("ctx", nil, makeTypeSelector("context", "Context"), nil),
					},
					functionArgs...,
				),
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

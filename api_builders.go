package dragonfly

import (
	"fmt"
	"go/ast"
	"go/token"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type (
	builderOptions struct {
		appendValueFormat       string
		variableForColumnNames  *string
		variableForColumnValues string
		variableForColumnExpr   string
	}
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
// TODO TypeSpec type
func typeDeclsToMap(types []*ast.TypeSpec) map[string]*ast.TypeSpec {
	result := make(map[string]*ast.TypeSpec, len(types))
	for i, spec := range types {
		if r, ok := result[spec.Name.Name]; ok {
			if reflect.DeepEqual(r, spec) {
				continue
			}
			panic(fmt.Sprintf("name `%s` repeated", spec.Name.Name))
		}
		result[spec.Name.Name] = types[i]
	}
	return result
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

// get a list of table columns and string field descriptors for the output structure. column and field positions correspond to each other
func extractFieldRefsAndColumnsFromStruct(rowFields []*ast.Field) (fieldRefs []ast.Expr, columnNames []string) {
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
	options builderOptions,
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
					makeAssignment([]string{options.variableForColumnValues}, makeCall(makeName("append"), makeName(options.variableForColumnValues), optionExpr)),
					makeAssignment(
						[]string{arrVariableName},
						makeCall(
							makeName("append"),
							makeName(arrVariableName),
							makeAddExpressions(
								makeBasicLiteralString("$"),
								makeCall(
									makeTypeSelector("strconv", "Itoa"),
									makeCall(makeName("len"), makeName(options.variableForColumnValues)),
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
					[]string{options.variableForColumnExpr},
					makeCall(
						makeName("append"),
						makeName(options.variableForColumnExpr),
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
	options builderOptions,
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
					makeCall(makeName("len"), makeName(options.variableForColumnValues)),
				),
			),
		)
	}
	return []ast.Stmt{
		makeAssignment(
			[]string{options.variableForColumnValues},
			makeCall(
				makeName("append"),
				makeName(options.variableForColumnValues),
				optionExpr,
			),
		),
		makeAssignment(
			[]string{options.variableForColumnExpr},
			makeCall(
				makeName("append"),
				makeName(options.variableForColumnExpr),
				makeCall(
					makeTypeSelector("fmt", "Sprintf"),
					append([]ast.Expr{makeBasicLiteralString(strings.Join(operators, " or "))}, callArgs...)...,
				),
			),
		),
	}
}

func makeScalarQueryOption(
	optionName, fieldName, columnName, operator string,
	ci, ref bool,
	options builderOptions,
) []ast.Stmt {
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
			[]string{options.variableForColumnValues},
			makeCall(
				makeName("append"),
				makeName(options.variableForColumnValues),
				optionExpr,
			),
		),
		makeAssignment(
			[]string{options.variableForColumnExpr},
			makeCall(
				makeName("append"),
				makeName(options.variableForColumnExpr),
				makeCall(
					makeTypeSelector("fmt", "Sprintf"),
					makeBasicLiteralString(operator),
					makeBasicLiteralString(columnName),
					makeAddExpressions(
						makeBasicLiteralString("$"),
						makeCall(
							makeTypeSelector("strconv", "Itoa"),
							makeCall(makeName("len"), makeName(options.variableForColumnValues)),
						),
					),
				),
			),
		),
	}
}

func makeStarQueryOption(
	optionName, fieldName, columnName, operator string,
	ci bool,
	options builderOptions,
) []ast.Stmt {
	return []ast.Stmt{
		&ast.IfStmt{
			Cond: makeNotEqualExpression(makeTypeSelector(optionName, fieldName), makeName("nil")),
			Body: makeBlock(
				makeScalarQueryOption(optionName, fieldName, columnName, operator, ci, true, options)...,
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

type (
	executionBlockOptions struct {
		rowVariableName          string
		rowStructTypeName        string
		variableNameForSqlText   string
		variableNameForArguments string
	}
)

func addExecutionBlockToFunctionBody(
	scanBlock scanWrapper,
	fieldRefs []ast.Expr,
	lastReturn ast.Stmt,
	options executionBlockOptions,
) []ast.Stmt {
	return []ast.Stmt{
		makeAssignmentWithErrChecking(
			"rows",
			makeCallEllipsis(
				makeTypeSelector("db", "Query"),
				makeName(options.variableNameForSqlText),
				makeName(options.variableNameForArguments),
			),
		),
		scanBlock(
			makeVarStatement(makeVarType(options.rowVariableName, makeName(options.rowStructTypeName))),
			makeAssignmentWithErrChecking(
				"",
				makeCall(
					makeTypeSelector("rows", "Scan"),
					fieldRefs...,
				),
			),
		),
		lastReturn,
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
			makeVarStatement(
				append([]ast.Spec{
					makeVarType("db", makeTypeStar(makeTypeSelector("sql", "DB"))),
					makeVarType("rows", makeTypeStar(makeTypeSelector("sql", "Rows"))),
					makeVarValue(sqlQueryVariableName, makeBasicLiteralString(sqlQuery)),
				}, addition...)...,
			),
			makeAssignmentWithErrChecking(
				"db",
				makeCall(makeName("getDatabase"), makeName("ctx")),
				makeEmptyReturn(),
			),
		},
		functionBody...,
	)
}

func processValueWrapper(
	colName string,
	field ast.Expr,
	options builderOptions,
) []ast.Stmt {
	stmts := make([]ast.Stmt, 0, 3)
	if options.variableForColumnNames != nil {
		stmts = append(stmts, makeAssignment(
			[]string{*options.variableForColumnNames},
			makeCall(makeName("append"), makeName(*options.variableForColumnNames), makeBasicLiteralString(colName)),
		))
	}
	return append(
		stmts,
		makeAssignment(
			[]string{options.variableForColumnValues},
			makeCall(makeName("append"), makeName(options.variableForColumnValues), field),
		),
		makeAssignment(
			[]string{options.variableForColumnExpr},
			makeCall(
				makeName("append"),
				makeName(options.variableForColumnExpr),
				makeCall(
					makeTypeSelector("fmt", "Sprintf"),
					makeBasicLiteralString(fmt.Sprintf(options.appendValueFormat, colName)),
					makeCall(makeName("len"), makeName(options.variableForColumnValues)),
				),
			),
		),
	)
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
		if strings.EqualFold(funcArgs[0], generateFunctionNow) {
			return makeCall(makeTypeSelector("time", "Now"))
		}
		// functions with 'len' argument
		if arrayContains([]string{
			generateFunctionHex,
			generateFunctionAlpha,
			generateFunctionDigits,
		}, funcArgs[0]) {
			var l = 16
			if len(funcArgs) > 1 {
				i, err := strconv.ParseInt(funcArgs[1], 10, 64)
				if err != nil {
					panic(err)
				}
				l = int(i)
			}
			var goFncName string
			switch funcArgs[0] {
			case generateFunctionHex:
				goFncName = "randomHex"
			case generateFunctionAlpha:
				goFncName = "randomAlpha"
			case generateFunctionDigits:
				goFncName = "randomDigits"
			default:
				panic(fmt.Sprintf("cannot resolve function name `%s`", funcArgs[0]))
			}
			return makeCall(makeName(goFncName), makeBasicLiteralInteger(l))
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

func makeInputParametersProcessorBlock(
	funcInputOptionName string,
	funcInputOptionTypeName string,
	optionFields []*ast.Field,
	options builderOptions,
) (
	[]ast.Stmt,
	[]*ast.TypeSpec,
	[]*ast.Field,
) {
	var (
		optionStructFields = make([]*ast.Field, 0, len(optionFields))
		functionBody       = make([]ast.Stmt, 0, len(optionFields)*3)
	)
	for _, field := range optionFields {
		var (
			tags      = tagToMap(field.Tag.Value)
			colName   = tags[TagTypeSQL][0]
			fieldName = makeTypeSelector(funcInputOptionName, field.Names[0].Name)
		)
		valueExpr, isOmittedField := makeValuePicker(tags[TagTypeSQL][1:], fieldName)
		if !isOmittedField {
			optionStructFields = append(optionStructFields, field)
		}
		wrapFunc := func(stmts []ast.Stmt) []ast.Stmt { return stmts }
		if _, ok := field.Type.(*ast.StarExpr); !isOmittedField && ok {
			wrapFunc = func(stmts []ast.Stmt) []ast.Stmt {
				return []ast.Stmt{
					&ast.IfStmt{
						Cond: makeNotNullExpression(fieldName),
						Body: makeBlock(stmts...),
					},
				}
			}
		}
		if arrayFind(tags[TagTypeSQL], tagEncrypt) > 0 {
			if _, star := field.Type.(*ast.StarExpr); star {
				valueExpr = makeCall(
					makeName("encryptPassword"),
					makeTypeStar(valueExpr),
				)
			} else {
				valueExpr = makeCall(
					makeName("encryptPassword"),
					valueExpr,
				)
			}
		}
		functionBody = append(
			functionBody,
			wrapFunc(processValueWrapper(
				colName, valueExpr, options,
			))...,
		)
	}
	return functionBody,
		[]*ast.TypeSpec{
			{
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

func makeFilterParametersProcessorBlock(
	funcFilterOptionName string,
	funcFilterOptionTypeName string,
	optionFields []*ast.Field,
	options builderOptions,
) (
	[]ast.Stmt,
	[]*ast.TypeSpec,
	[]*ast.Field,
) {
	var (
		functionBody = make([]ast.Stmt, 0, len(optionFields)*3)
	)
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
				makeUnionQueryOption(funcFilterOptionName, field.Names[0].Name, columns, operator.getRawExpression(), ci, options)...,
			)
		} else {
			if operator.isMult() {
				functionBody = append(
					functionBody,
					makeArrayQueryOption(funcFilterOptionName, field.Names[0].Name, colName, operator.getRawExpression(), ci, options)...,
				)
			} else {
				if _, ok := field.Type.(*ast.StarExpr); ok {
					functionBody = append(
						functionBody,
						makeStarQueryOption(funcFilterOptionName, field.Names[0].Name, colName, operator.getRawExpression(), ci, options)...,
					)
				} else {
					functionBody = append(
						functionBody,
						makeScalarQueryOption(funcFilterOptionName, field.Names[0].Name, colName, operator.getRawExpression(), ci, false, options)...,
					)
				}
			}
		}
	}
	return functionBody,
		[]*ast.TypeSpec{
			{
				Name: makeName(funcFilterOptionTypeName),
				Type: &ast.StructType{
					Fields:     &ast.FieldList{List: optionFields},
					Incomplete: false,
				},
			},
		},
		[]*ast.Field{
			{
				Names: []*ast.Ident{makeName(funcFilterOptionName)},
				Type:  makeName(funcFilterOptionTypeName),
			},
		}
}

func makeFindFunction(variant findVariant) ApiFuncBuilder {
	const (
		argsVarName     = "args"
		scanVarName     = "row"
		sqlTextName     = "sqlText"
		filtersVariable = "filters"
	)
	return func(
		fullTableName, functionName, rowStructName string,
		optionFields, _, rowFields []*ast.Field,
	) AstDataChain {
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
			fieldRefs, columnList = extractFieldRefsAndColumnsFromStruct(rowFields)
		)
		sqlQuery := fmt.Sprintf("select %s from %s where %%s", strings.Join(columnList, ", "), fullTableName)
		functionBody, findTypes, findAttrs := makeFilterParametersProcessorBlock(
			"find",
			functionName+"Option",
			optionFields,
			builderOptions{
				appendValueFormat:       "%s = $%%d",
				variableForColumnNames:  nil,
				variableForColumnValues: "args",
				variableForColumnExpr:   filtersVariable,
			},
		)
		functionBody = append(
			functionBody,
			&ast.IfStmt{
				Cond: makeNotEmptyArrayExpression(filtersVariable),
				Body: makeBlock(
					makeAssignment(
						[]string{sqlTextName},
						makeCall(
							makeTypeSelector("fmt", "Sprintf"),
							makeName(sqlTextName),
							makeAddExpressions(
								makeBasicLiteralString("("),
								makeCall(
									makeTypeSelector("strings", "Join"),
									makeName(filtersVariable),
									makeBasicLiteralString(") and ("),
								),
								makeBasicLiteralString(")"),
							),
						),
					),
				),
				Else: makeAssignment(
					[]string{sqlTextName},
					makeCall(
						makeTypeSelector("fmt", "Sprintf"),
						makeName(sqlTextName),
						makeBasicLiteralString("1 = 1"),
					),
				),
			},
		)
		functionBody = append(
			functionBody,
			addExecutionBlockToFunctionBody(scanBlockWrapper, fieldRefs, lastReturn, executionBlockOptions{
				rowVariableName:          scanVarName,
				rowStructTypeName:        rowStructName,
				variableNameForSqlText:   sqlTextName,
				variableNameForArguments: argsVarName,
			})...,
		)
		functionBody = addVariablesToFunctionBody(
			functionBody,
			sqlTextName,
			sqlQuery,
			makeVarValue(
				argsVarName,
				makeCall(makeName("make"), makeTypeArray(makeEmptyInterface()), makeBasicLiteralInteger(0), makeBasicLiteralInteger(len(optionFields))),
			),
			makeVarValue(
				filtersVariable,
				makeCall(makeName("make"), makeTypeArray(makeName("string")), makeBasicLiteralInteger(0), makeBasicLiteralInteger(len(optionFields))),
			),
		)
		return AstDataChain{
			Types:     typeDeclsToMap(findTypes),
			Constants: nil,
			Implementations: map[string]*ast.FuncDecl{
				functionName: makeApiFunction(functionName, resultExpr, functionBody, findAttrs...),
			},
		}
	}
}

func makeDeleteFunction(variant findVariant) ApiFuncBuilder {
	const (
		argsVarName     = "args"
		scanVarName     = "row"
		sqlTextName     = "sqlText"
		filtersVariable = "filters"
	)
	return func(
		fullTableName, functionName, rowStructName string,
		optionFields, _, rowFields []*ast.Field,
	) AstDataChain {
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
			fieldRefs, columnList = extractFieldRefsAndColumnsFromStruct(rowFields)
		)
		sqlQuery := fmt.Sprintf("delete from %s where %%s returning %s", fullTableName, strings.Join(columnList, ", "))
		functionBody, findTypes, findAttrs := makeFilterParametersProcessorBlock(
			"find",
			functionName+"Option",
			optionFields,
			builderOptions{
				appendValueFormat:       "%s = $%%d",
				variableForColumnNames:  nil,
				variableForColumnValues: argsVarName,
				variableForColumnExpr:   filtersVariable,
			},
		)
		functionBody = append(
			functionBody,
			&ast.IfStmt{
				Cond: makeNotEmptyArrayExpression(filtersVariable),
				Body: makeBlock(
					makeAssignment(
						[]string{sqlTextName},
						makeCall(
							makeTypeSelector("fmt", "Sprintf"),
							makeName(sqlTextName),
							makeAddExpressions(
								makeBasicLiteralString("("),
								makeCall(
									makeTypeSelector("strings", "Join"),
									makeName(filtersVariable),
									makeBasicLiteralString(") and ("),
								),
								makeBasicLiteralString(")"),
							),
						),
					),
				),
				Else: makeAssignment(
					[]string{sqlTextName},
					makeCall(
						makeTypeSelector("fmt", "Sprintf"),
						makeName(sqlTextName),
						makeBasicLiteralString("/* ERROR: CANNOT DELETE ALL */ !"),
					),
				),
			},
		)
		functionBody = append(
			functionBody,
			addExecutionBlockToFunctionBody(scanBlockWrapper, fieldRefs, lastReturn, executionBlockOptions{
				rowVariableName:          scanVarName,
				rowStructTypeName:        rowStructName,
				variableNameForSqlText:   sqlTextName,
				variableNameForArguments: argsVarName,
			})...,
		)
		functionBody = addVariablesToFunctionBody(
			functionBody,
			sqlTextName,
			sqlQuery,
			makeVarValue(
				argsVarName,
				makeCall(makeName("make"), makeTypeArray(makeEmptyInterface()), makeBasicLiteralInteger(0), makeBasicLiteralInteger(len(optionFields))),
			),
			makeVarValue(
				filtersVariable,
				makeCall(makeName("make"), makeTypeArray(makeName("string")), makeBasicLiteralInteger(0), makeBasicLiteralInteger(len(optionFields))),
			),
		)
		return AstDataChain{
			Types:     typeDeclsToMap(findTypes),
			Constants: nil,
			Implementations: map[string]*ast.FuncDecl{
				functionName: makeApiFunction(functionName, resultExpr, functionBody, findAttrs...),
			},
		}
	}
}

func updateOneBuilder(
	fullTableName, functionName, rowStructName string,
	optionFields, mutableFields, rowFields []*ast.Field,
) AstDataChain {
	const (
		argsVarName        = "args"
		scanVarName        = "row"
		sqlTextName        = "sqlText"
		filtersVariable    = "filters"
		fieldsNameVariable = "fields"
	)
	resultExpr := makeTypeIdent(rowStructName)
	scanBlockWrapper := scanBlockForFindOnce
	lastReturn := makeReturn(
		makeName("result"),
		makeName("EmptyResult"),
	)
	var (
		fieldRefs, outColumnList = extractFieldRefsAndColumnsFromStruct(rowFields)
	)
	sqlQuery := fmt.Sprintf("update %s set %%s where %%s returning %s", fullTableName, strings.Join(outColumnList, ", "))
	functionBody, inputTypes, inputAttrs := makeInputParametersProcessorBlock(
		"values",
		makeExportedName(functionName+"Values"),
		mutableFields,
		builderOptions{
			appendValueFormat:       "%s = $%%d",
			variableForColumnNames:  nil,
			variableForColumnValues: argsVarName,
			variableForColumnExpr:   fieldsNameVariable,
		},
	)
	findBlock, findTypes, findAttrs := makeFilterParametersProcessorBlock(
		"filter",
		makeExportedName(functionName+"Option"),
		optionFields,
		builderOptions{
			appendValueFormat:       "",
			variableForColumnNames:  nil,
			variableForColumnValues: argsVarName,
			variableForColumnExpr:   filtersVariable,
		},
	)
	functionBody = append(functionBody, findBlock...)
	functionBody = append(
		functionBody,
		makeAssignment(
			[]string{sqlTextName},
			makeCall(
				makeTypeSelector("fmt", "Sprintf"),
				makeName(sqlTextName),
				makeCall(
					makeTypeSelector("strings", "Join"),
					makeName(fieldsNameVariable),
					makeBasicLiteralString(", "),
				),
				makeAddExpressions(
					makeBasicLiteralString("("),
					makeCall(
						makeTypeSelector("strings", "Join"),
						makeName(filtersVariable),
						makeBasicLiteralString(") and ("),
					),
					makeBasicLiteralString(")"),
				),
			),
		),
	)
	functionBody = append(
		functionBody,
		addExecutionBlockToFunctionBody(scanBlockWrapper, fieldRefs, lastReturn, executionBlockOptions{
			rowVariableName:          scanVarName,
			rowStructTypeName:        rowStructName,
			variableNameForSqlText:   sqlTextName,
			variableNameForArguments: argsVarName,
		})...,
	)
	functionBody = addVariablesToFunctionBody(
		functionBody,
		sqlTextName,
		sqlQuery,
		makeVarValue(
			argsVarName,
			makeCall(makeName("make"), makeTypeArray(makeEmptyInterface()), makeBasicLiteralInteger(0), makeBasicLiteralInteger(len(mutableFields))),
		),
		makeVarValue(
			fieldsNameVariable,
			makeCall(makeName("make"), makeTypeArray(makeName("string")), makeBasicLiteralInteger(0), makeBasicLiteralInteger(len(mutableFields))),
		),
		makeVarValue(
			filtersVariable,
			makeCall(makeName("make"), makeTypeArray(makeName("string")), makeBasicLiteralInteger(0), makeBasicLiteralInteger(len(mutableFields))),
		),
	)
	return AstDataChain{
		Types:     typeDeclsToMap(append(inputTypes, findTypes...)),
		Constants: nil,
		Implementations: map[string]*ast.FuncDecl{
			functionName: makeApiFunction(functionName, resultExpr, functionBody, append(inputAttrs, findAttrs...)...),
		},
	}
}

func insertOneBuilder(
	fullTableName, functionName, rowStructName string,
	_, mutableFields, rowFields []*ast.Field,
) AstDataChain {
	const (
		argsVarName        = "args"
		scanVarName        = "row"
		sqlTextName        = "sqlText"
		fieldsNameVariable = "fields"
		valuesNameVariable = "values"
	)
	resultExpr := makeTypeIdent(rowStructName)
	scanBlockWrapper := scanBlockForFindOnce
	lastReturn := makeReturn(
		makeName("result"),
		makeName("EmptyResult"),
	)
	var (
		fieldRefs, outColumnList = extractFieldRefsAndColumnsFromStruct(rowFields)
	)
	sqlQuery := fmt.Sprintf("insert into %s (%%s) values (%%s) returning %s", fullTableName, strings.Join(outColumnList, ", "))
	functionBody, functionTypes, functionAttrs := makeInputParametersProcessorBlock(
		"record",
		makeExportedName(functionName+"Values"),
		mutableFields,
		builderOptions{
			appendValueFormat:       "/* %s */ $%%d",
			variableForColumnNames:  refString(fieldsNameVariable),
			variableForColumnValues: argsVarName,
			variableForColumnExpr:   valuesNameVariable,
		},
	)
	functionBody = append(
		functionBody,
		makeAssignment(
			[]string{sqlTextName},
			makeCall(
				makeTypeSelector("fmt", "Sprintf"),
				makeName(sqlTextName),
				makeCall(
					makeTypeSelector("strings", "Join"),
					makeName(fieldsNameVariable),
					makeBasicLiteralString(", "),
				),
				makeCall(
					makeTypeSelector("strings", "Join"),
					makeName(valuesNameVariable),
					makeBasicLiteralString(", "),
				),
			),
		),
	)
	functionBody = append(
		functionBody,
		addExecutionBlockToFunctionBody(scanBlockWrapper, fieldRefs, lastReturn, executionBlockOptions{
			rowVariableName:          scanVarName,
			rowStructTypeName:        rowStructName,
			variableNameForSqlText:   sqlTextName,
			variableNameForArguments: argsVarName,
		})...,
	)
	functionBody = addVariablesToFunctionBody(
		functionBody,
		sqlTextName,
		sqlQuery,
		makeVarValue(
			argsVarName,
			makeCall(makeName("make"), makeTypeArray(makeEmptyInterface()), makeBasicLiteralInteger(0), makeBasicLiteralInteger(len(mutableFields))),
		),
		makeVarValue(
			fieldsNameVariable,
			makeCall(makeName("make"), makeTypeArray(makeName("string")), makeBasicLiteralInteger(0), makeBasicLiteralInteger(len(mutableFields))),
		),
		makeVarValue(
			valuesNameVariable,
			makeCall(makeName("make"), makeTypeArray(makeName("string")), makeBasicLiteralInteger(0), makeBasicLiteralInteger(len(mutableFields))),
		),
	)
	return AstDataChain{
		Types:     typeDeclsToMap(functionTypes),
		Constants: nil,
		Implementations: map[string]*ast.FuncDecl{
			functionName: makeApiFunction(functionName, resultExpr, functionBody, functionAttrs...),
		},
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

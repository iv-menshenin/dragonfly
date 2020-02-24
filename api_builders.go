package dragonfly

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/code_builders"
	"go/ast"
	"go/token"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type (
	variableName   string
	builderOptions struct {
		appendValueFormat       string
		variableForColumnNames  *variableName
		variableForColumnValues variableName
		variableForColumnExpr   variableName
	}
	executionBlockOptions struct {
		rowVariableName          variableName
		rowStructTypeName        variableName
		variableNameForSqlText   variableName
		variableNameForArguments variableName
	}
)

const (
	argsVariable    variableName = "args"
	filtersVariable variableName = "filters"
	fieldsVariable  variableName = "fields"
	valuesVariable  variableName = "values"

	scanDestVariable variableName = "row"
)

func (v variableName) String() string {
	return string(v)
}

var (
	fieldsVariableRef  = fieldsVariable
	FindBuilderOptions = builderOptions{
		appendValueFormat:       "%s = $%%d",
		variableForColumnNames:  nil,
		variableForColumnValues: "args",
		variableForColumnExpr:   filtersVariable,
	}
	InsertBuilderOptions = builderOptions{
		appendValueFormat:       "/* %s */ $%%d",
		variableForColumnNames:  &fieldsVariableRef,
		variableForColumnValues: argsVariable,
		variableForColumnExpr:   valuesVariable,
	}
	UpdateBuilderOptions = builderOptions{
		appendValueFormat:       "%s = $%%d",
		variableForColumnNames:  nil,
		variableForColumnValues: argsVariable,
		variableForColumnExpr:   fieldsVariable,
	}
	DeleteBuilderOptions = builderOptions{
		appendValueFormat:       "%s = $%%d",
		variableForColumnNames:  nil,
		variableForColumnValues: argsVariable,
		variableForColumnExpr:   filtersVariable,
	}
	IncomingArgumentsBuilderOptions = builderOptions{
		appendValueFormat:       "",
		variableForColumnNames:  nil,
		variableForColumnValues: argsVariable,
		variableForColumnExpr:   filtersVariable,
	}
)

func MakeExecutionOption(rowStructName, sqlVariableName string) executionBlockOptions {
	return executionBlockOptions{
		rowVariableName:          scanDestVariable,
		rowStructTypeName:        variableName(rowStructName),
		variableNameForSqlText:   variableName(sqlVariableName),
		variableNameForArguments: argsVariable,
	}
}

var (
	WrapperFindOne = scanBlockForFindOnce
	WrapperFindAll = scanBlockForFindAll
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
		fieldRefs = append(fieldRefs, ast.NewIdent(fieldName))
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
	var optionExpr ast.Expr = ast.NewIdent(localVariable)
	if ci {
		columnName = fmt.Sprintf("lower(%s)", columnName)
		optionExpr = builders.MakeCallExpression(builders.MakeSelectorExpression("strings", "ToLower"), optionExpr)
	}
	// for placeholders only
	var arrVariableName = fmt.Sprintf("array%s", fieldName)
	return []ast.Stmt{
		builders.MakeVarStatement(builders.MakeVarType(arrVariableName, builders.MakeArrayType(ast.NewIdent("string")))),
		&ast.RangeStmt{
			Key:   ast.NewIdent("_"),
			Value: ast.NewIdent(localVariable),
			X:     builders.MakeSelectorExpression(optionName, fieldName),
			Tok:   token.DEFINE,
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					builders.MakeAssignment([]string{options.variableForColumnValues.String()}, builders.MakeCallExpression(ast.NewIdent("append"), ast.NewIdent(options.variableForColumnValues.String()), optionExpr)),
					builders.MakeAssignment(
						[]string{arrVariableName},
						builders.MakeCallExpression(
							ast.NewIdent("append"),
							ast.NewIdent(arrVariableName),
							builders.MakeAddExpressions(
								builders.MakeBasicLiteralString("$"),
								builders.MakeCallExpression(
									builders.MakeSelectorExpression("strconv", "Itoa"),
									builders.MakeCallExpression(ast.NewIdent("len"), ast.NewIdent(options.variableForColumnValues.String())),
								),
							),
						),
					),
				},
			},
		},
		&ast.IfStmt{
			Cond: builders.MakeNotEmptyArrayExpression(arrVariableName),
			Body: builders.MakeBlockStmt(
				builders.MakeAssignment(
					[]string{options.variableForColumnExpr.String()},
					builders.MakeCallExpression(
						ast.NewIdent("append"),
						ast.NewIdent(options.variableForColumnExpr.String()),
						builders.MakeCallExpression(
							builders.MakeSelectorExpression("fmt", "Sprintf"),
							builders.MakeBasicLiteralString(operator),
							builders.MakeBasicLiteralString(columnName),
							builders.MakeCallExpression(
								builders.MakeSelectorExpression("strings", "Join"),
								ast.NewIdent(arrVariableName),
								builders.MakeBasicLiteralString(", "),
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
	var optionExpr = builders.MakeSelectorExpression(structName, fieldName)
	if ci {
		for i, c := range columnNames {
			columnNames[i] = fmt.Sprintf("lower(%s)", c)
		}
		optionExpr = builders.MakeCallExpression(builders.MakeSelectorExpression("strings", "ToLower"), optionExpr)
	}
	operators := make([]string, 0, len(operator))
	for _, _ = range columnNames {
		operators = append(operators, operator)
	}
	callArgs := make([]ast.Expr, 0, len(columnNames)*2)
	for _, c := range columnNames {
		callArgs = append(
			callArgs,
			builders.MakeBasicLiteralString(c),
			builders.MakeAddExpressions(
				builders.MakeBasicLiteralString("$"),
				builders.MakeCallExpression(
					builders.MakeSelectorExpression("strconv", "Itoa"),
					builders.MakeCallExpression(ast.NewIdent("len"), ast.NewIdent(options.variableForColumnValues.String())),
				),
			),
		)
	}
	return []ast.Stmt{
		builders.MakeAssignment(
			[]string{options.variableForColumnValues.String()},
			builders.MakeCallExpression(
				ast.NewIdent("append"),
				ast.NewIdent(options.variableForColumnValues.String()),
				optionExpr,
			),
		),
		builders.MakeAssignment(
			[]string{options.variableForColumnExpr.String()},
			builders.MakeCallExpression(
				ast.NewIdent("append"),
				ast.NewIdent(options.variableForColumnExpr.String()),
				builders.MakeCallExpression(
					builders.MakeSelectorExpression("fmt", "Sprintf"),
					append([]ast.Expr{builders.MakeBasicLiteralString(strings.Join(operators, " or "))}, callArgs...)...,
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
	var optionExpr = builders.MakeSelectorExpression(optionName, fieldName)
	if ref {
		optionExpr = builders.MakeStarExpression(optionExpr)
	}
	if ci {
		columnName = fmt.Sprintf("lower(%s)", columnName)
		optionExpr = builders.MakeCallExpression(builders.MakeSelectorExpression("strings", "ToLower"), optionExpr)
	}
	return []ast.Stmt{
		builders.MakeAssignment(
			[]string{options.variableForColumnValues.String()},
			builders.MakeCallExpression(
				ast.NewIdent("append"),
				ast.NewIdent(options.variableForColumnValues.String()),
				optionExpr,
			),
		),
		builders.MakeAssignment(
			[]string{options.variableForColumnExpr.String()},
			builders.MakeCallExpression(
				ast.NewIdent("append"),
				ast.NewIdent(options.variableForColumnExpr.String()),
				builders.MakeCallExpression(
					builders.MakeSelectorExpression("fmt", "Sprintf"),
					builders.MakeBasicLiteralString(operator),
					builders.MakeBasicLiteralString(columnName),
					builders.MakeAddExpressions(
						builders.MakeBasicLiteralString("$"),
						builders.MakeCallExpression(
							builders.MakeSelectorExpression("strconv", "Itoa"),
							builders.MakeCallExpression(ast.NewIdent("len"), ast.NewIdent(options.variableForColumnValues.String())),
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
			Cond: builders.MakeNotEqualExpression(builders.MakeSelectorExpression(optionName, fieldName), ast.NewIdent("nil")),
			Body: builders.MakeBlockStmt(
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
		Cond: builders.MakeCallExpression(builders.MakeSelectorExpression("rows", "Next")),
		Body: builders.MakeBlockStmt(
			append(
				append(
					[]ast.Stmt{
						builders.MakeAssignmentWithErrChecking(
							"",
							builders.MakeCallExpression(
								builders.MakeSelectorExpression("rows", "Err"),
							),
						),
					},
					stmts...,
				),
				&ast.IfStmt{
					Cond: builders.MakeCallExpression(builders.MakeSelectorExpression("rows", "Next")),
					Body: builders.MakeBlockStmt(
						builders.MakeReturn(
							ast.NewIdent("row"),
							ast.NewIdent("SingletonViolation"),
						),
					),
					Else: builders.MakeReturn(
						ast.NewIdent("row"),
						ast.NewIdent("nil"),
					),
				},
			)...,
		),
	}
}

func scanBlockForFindAll(stmts ...ast.Stmt) ast.Stmt {
	return &ast.ForStmt{
		Cond: builders.MakeCallExpression(builders.MakeSelectorExpression("rows", "Next")),
		Body: builders.MakeBlockStmt(
			append(
				append(
					[]ast.Stmt{
						builders.MakeAssignmentWithErrChecking(
							"",
							builders.MakeCallExpression(
								builders.MakeSelectorExpression("rows", "Err"),
							),
						),
					},
					stmts...,
				),
				builders.MakeAssignment(
					[]string{"result"},
					builders.MakeCallExpression(
						ast.NewIdent("append"),
						ast.NewIdent("result"),
						ast.NewIdent("row"),
					),
				),
			)...,
		),
	}
}

func BuildExecutionBlockForFunction(
	scanBlock scanWrapper,
	fieldRefs []ast.Expr,
	options executionBlockOptions,
) []ast.Stmt {
	return []ast.Stmt{
		builders.MakeAssignmentWithErrChecking(
			"rows",
			builders.MakeCallExpressionEllipsis(
				builders.MakeSelectorExpression("db", "Query"),
				ast.NewIdent(options.variableNameForSqlText.String()),
				ast.NewIdent(options.variableNameForArguments.String()),
			),
		),
		scanBlock(
			builders.MakeVarStatement(builders.MakeVarType(options.rowVariableName.String(), ast.NewIdent(options.rowStructTypeName.String()))),
			builders.MakeAssignmentWithErrChecking(
				"",
				builders.MakeCallExpression(
					builders.MakeSelectorExpression("rows", "Scan"),
					fieldRefs...,
				),
			),
		),
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
			builders.MakeVarStatement(
				append([]ast.Spec{
					builders.MakeVarType("db", builders.MakeStarExpression(builders.MakeSelectorExpression("sql", "DB"))),
					builders.MakeVarType("rows", builders.MakeStarExpression(builders.MakeSelectorExpression("sql", "Rows"))),
					builders.MakeVarValue(sqlQueryVariableName, builders.MakeBasicLiteralString(sqlQuery)),
				}, addition...)...,
			),
			builders.MakeAssignmentWithErrChecking(
				"db",
				builders.MakeCallExpression(ast.NewIdent("getDatabase"), ast.NewIdent("ctx")),
				builders.MakeEmptyReturn(),
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
		stmts = append(stmts, builders.MakeAssignment(
			[]string{options.variableForColumnNames.String()},
			builders.MakeCallExpression(ast.NewIdent("append"), ast.NewIdent(options.variableForColumnNames.String()), builders.MakeBasicLiteralString(colName)),
		))
	}
	return append(
		stmts,
		builders.MakeAssignment(
			[]string{options.variableForColumnValues.String()},
			builders.MakeCallExpression(ast.NewIdent("append"), ast.NewIdent(options.variableForColumnValues.String()), field),
		),
		builders.MakeAssignment(
			[]string{options.variableForColumnExpr.String()},
			builders.MakeCallExpression(
				ast.NewIdent("append"),
				ast.NewIdent(options.variableForColumnExpr.String()),
				builders.MakeCallExpression(
					builders.MakeSelectorExpression("fmt", "Sprintf"),
					builders.MakeBasicLiteralString(fmt.Sprintf(options.appendValueFormat, colName)),
					builders.MakeCallExpression(ast.NewIdent("len"), ast.NewIdent(options.variableForColumnValues.String())),
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
			return builders.MakeCallExpression(builders.MakeSelectorExpression("time", "Now"))
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
			return builders.MakeCallExpression(ast.NewIdent(goFncName), builders.MakeBasicLiteralInteger(l))
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
	map[string]*ast.TypeSpec,
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
			fieldName = builders.MakeSelectorExpression(funcInputOptionName, field.Names[0].Name)
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
						Cond: builders.MakeNotNullExpression(fieldName),
						Body: builders.MakeBlockStmt(stmts...),
					},
				}
			}
		}
		if arrayFind(tags[TagTypeSQL], tagEncrypt) > 0 {
			if _, star := field.Type.(*ast.StarExpr); star {
				valueExpr = builders.MakeCallExpression(
					ast.NewIdent("encryptPassword"),
					builders.MakeStarExpression(valueExpr),
				)
			} else {
				valueExpr = builders.MakeCallExpression(
					ast.NewIdent("encryptPassword"),
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
		map[string]*ast.TypeSpec{
			funcInputOptionTypeName: {
				Name: ast.NewIdent(funcInputOptionTypeName),
				Type: &ast.StructType{
					Fields:     &ast.FieldList{List: optionStructFields},
					Incomplete: false,
				},
			},
		},
		[]*ast.Field{
			{
				Names: []*ast.Ident{ast.NewIdent(funcInputOptionName)},
				Type:  ast.NewIdent(funcInputOptionTypeName),
			},
		}
}

/*
	Extracts required and optional parameters from incoming arguments, builds program code
	Returns the body of program code, required type declarations and required input fields
*/
func BuildIncomingArgumentsProcessor(
	funcFilterOptionName string,
	funcFilterOptionTypeName string,
	optionFields []*ast.Field,
	options builderOptions,
) (
	[]ast.Stmt,
	map[string]*ast.TypeSpec,
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
		map[string]*ast.TypeSpec{
			funcFilterOptionTypeName: {
				Name: ast.NewIdent(funcFilterOptionTypeName),
				Type: &ast.StructType{
					Fields:     &ast.FieldList{List: optionFields},
					Incomplete: false,
				},
			},
		},
		[]*ast.Field{
			{
				Names: []*ast.Ident{ast.NewIdent(funcFilterOptionName)},
				Type:  ast.NewIdent(funcFilterOptionTypeName),
			},
		}
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
			scanBlockWrapper scanWrapper
			resultExpr       ast.Expr
			lastReturn       ast.Stmt
		)
		switch variant {
		case findVariantOnce:
			scanBlockWrapper = WrapperFindOne
			resultExpr = ast.NewIdent(rowStructName)
			lastReturn = builders.MakeReturn(
				ast.NewIdent("result"),
				ast.NewIdent("EmptyResult"),
			)
		case findVariantAll:
			scanBlockWrapper = WrapperFindAll
			resultExpr = builders.MakeArrayType(ast.NewIdent(rowStructName))
			lastReturn = builders.MakeEmptyReturn()
		default:
			panic("cannot resolve 'variant'")
		}
		var (
			fieldRefs, columnList = extractFieldRefsAndColumnsFromStruct(rowFields)
		)
		sqlQuery := fmt.Sprintf("select %s from %s where %%s", strings.Join(columnList, ", "), fullTableName)
		functionBody, findTypes, findAttrs := BuildIncomingArgumentsProcessor(
			"find",
			functionName+"Option",
			optionFields,
			FindBuilderOptions,
		)
		functionBody = append(
			functionBody,
			&ast.IfStmt{
				Cond: builders.MakeNotEmptyArrayExpression(filtersVariable.String()),
				Body: builders.MakeBlockStmt(
					builders.MakeAssignment(
						[]string{sqlTextName},
						builders.MakeCallExpression(
							builders.MakeSelectorExpression("fmt", "Sprintf"),
							ast.NewIdent(sqlTextName),
							builders.MakeAddExpressions(
								builders.MakeBasicLiteralString("("),
								builders.MakeCallExpression(
									builders.MakeSelectorExpression("strings", "Join"),
									ast.NewIdent(filtersVariable.String()),
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
						builders.MakeSelectorExpression("fmt", "Sprintf"),
						ast.NewIdent(sqlTextName),
						builders.MakeBasicLiteralString("1 = 1"),
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
			builders.MakeVarValue(
				argsVariable.String(),
				builders.MakeCallExpression(ast.NewIdent("make"), builders.MakeArrayType(builders.MakeEmptyInterface()), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(optionFields))),
			),
			builders.MakeVarValue(
				filtersVariable.String(),
				builders.MakeCallExpression(ast.NewIdent("make"), builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(optionFields))),
			),
		)
		return AstDataChain{
			Types:     findTypes,
			Constants: nil,
			Implementations: map[string]*ast.FuncDecl{
				functionName: makeApiFunction(functionName, resultExpr, functionBody, findAttrs...),
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
			scanBlockWrapper scanWrapper
			resultExpr       ast.Expr
			lastReturn       ast.Stmt
		)
		switch variant {
		case findVariantOnce:
			scanBlockWrapper = WrapperFindOne
			resultExpr = ast.NewIdent(rowStructName)
			lastReturn = builders.MakeReturn(
				ast.NewIdent("result"),
				ast.NewIdent("EmptyResult"),
			)
		case findVariantAll:
			scanBlockWrapper = WrapperFindAll
			resultExpr = builders.MakeArrayType(ast.NewIdent(rowStructName))
			lastReturn = builders.MakeEmptyReturn()
		default:
			panic("cannot resolve 'variant'")
		}
		var (
			fieldRefs, columnList = extractFieldRefsAndColumnsFromStruct(rowFields)
		)
		sqlQuery := fmt.Sprintf("delete from %s where %%s returning %s", fullTableName, strings.Join(columnList, ", "))
		functionBody, findTypes, findAttrs := BuildIncomingArgumentsProcessor(
			"find",
			functionName+"Option",
			optionFields,
			DeleteBuilderOptions,
		)
		functionBody = append(
			functionBody,
			&ast.IfStmt{
				Cond: builders.MakeNotEmptyArrayExpression(filtersVariable.String()),
				Body: builders.MakeBlockStmt(
					builders.MakeAssignment(
						[]string{sqlTextName},
						builders.MakeCallExpression(
							builders.MakeSelectorExpression("fmt", "Sprintf"),
							ast.NewIdent(sqlTextName),
							builders.MakeAddExpressions(
								builders.MakeBasicLiteralString("("),
								builders.MakeCallExpression(
									builders.MakeSelectorExpression("strings", "Join"),
									ast.NewIdent(filtersVariable.String()),
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
						builders.MakeSelectorExpression("fmt", "Sprintf"),
						ast.NewIdent(sqlTextName),
						builders.MakeBasicLiteralString("/* ERROR: CANNOT DELETE ALL */ !"),
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
			builders.MakeVarValue(
				argsVariable.String(),
				builders.MakeCallExpression(ast.NewIdent("make"), builders.MakeArrayType(builders.MakeEmptyInterface()), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(optionFields))),
			),
			builders.MakeVarValue(
				filtersVariable.String(),
				builders.MakeCallExpression(ast.NewIdent("make"), builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(optionFields))),
			),
		)
		return AstDataChain{
			Types:     findTypes,
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
		sqlTextName = "sqlText"
	)
	resultExpr := ast.NewIdent(rowStructName)
	scanBlockWrapper := WrapperFindOne
	lastReturn := builders.MakeReturn(
		ast.NewIdent("result"),
		ast.NewIdent("EmptyResult"),
	)
	var (
		fieldRefs, outColumnList = extractFieldRefsAndColumnsFromStruct(rowFields)
	)
	sqlQuery := fmt.Sprintf("update %s set %%s where %%s returning %s", fullTableName, strings.Join(outColumnList, ", "))
	functionBody, inputTypes, inputAttrs := makeInputParametersProcessorBlock(
		"values",
		makeExportedName(functionName+"Values"),
		mutableFields,
		UpdateBuilderOptions,
	)
	findBlock, findTypes, findAttrs := BuildIncomingArgumentsProcessor(
		"filter",
		makeExportedName(functionName+"Option"),
		optionFields,
		IncomingArgumentsBuilderOptions,
	)
	functionBody = append(functionBody, findBlock...)
	functionBody = append(
		functionBody,
		builders.MakeAssignment(
			[]string{sqlTextName},
			builders.MakeCallExpression(
				builders.MakeSelectorExpression("fmt", "Sprintf"),
				ast.NewIdent(sqlTextName),
				builders.MakeCallExpression(
					builders.MakeSelectorExpression("strings", "Join"),
					ast.NewIdent(fieldsVariable.String()),
					builders.MakeBasicLiteralString(", "),
				),
				builders.MakeAddExpressions(
					builders.MakeBasicLiteralString("("),
					builders.MakeCallExpression(
						builders.MakeSelectorExpression("strings", "Join"),
						ast.NewIdent(filtersVariable.String()),
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
			BuildExecutionBlockForFunction(scanBlockWrapper, fieldRefs, MakeExecutionOption(rowStructName, sqlTextName))...,
		),
		lastReturn,
	)
	functionBody = addVariablesToFunctionBody(
		functionBody,
		sqlTextName,
		sqlQuery,
		builders.MakeVarValue(
			argsVariable.String(),
			builders.MakeCallExpression(ast.NewIdent("make"), builders.MakeArrayType(builders.MakeEmptyInterface()), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
		builders.MakeVarValue(
			fieldsVariable.String(),
			builders.MakeCallExpression(ast.NewIdent("make"), builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
		builders.MakeVarValue(
			filtersVariable.String(),
			builders.MakeCallExpression(ast.NewIdent("make"), builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
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
			functionName: makeApiFunction(functionName, resultExpr, functionBody, append(inputAttrs, findAttrs...)...),
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
	scanBlockWrapper := WrapperFindOne
	lastReturn := builders.MakeReturn(
		ast.NewIdent("result"),
		ast.NewIdent("EmptyResult"),
	)
	var (
		fieldRefs, outColumnList = extractFieldRefsAndColumnsFromStruct(rowFields)
	)
	sqlQuery := fmt.Sprintf("insert into %s (%%s) values (%%s) returning %s", fullTableName, strings.Join(outColumnList, ", "))
	functionBody, functionTypes, functionAttrs := makeInputParametersProcessorBlock(
		"record",
		makeExportedName(functionName+"Values"),
		mutableFields,
		InsertBuilderOptions,
	)
	functionBody = append(
		functionBody,
		builders.MakeAssignment(
			[]string{sqlTextName},
			builders.MakeCallExpression(
				builders.MakeSelectorExpression("fmt", "Sprintf"),
				ast.NewIdent(sqlTextName),
				builders.MakeCallExpression(
					builders.MakeSelectorExpression("strings", "Join"),
					ast.NewIdent(fieldsVariable.String()),
					builders.MakeBasicLiteralString(", "),
				),
				builders.MakeCallExpression(
					builders.MakeSelectorExpression("strings", "Join"),
					ast.NewIdent(valuesVariable.String()),
					builders.MakeBasicLiteralString(", "),
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
		builders.MakeVarValue(
			argsVariable.String(),
			builders.MakeCallExpression(ast.NewIdent("make"), builders.MakeArrayType(builders.MakeEmptyInterface()), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
		builders.MakeVarValue(
			fieldsVariable.String(),
			builders.MakeCallExpression(ast.NewIdent("make"), builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
		builders.MakeVarValue(
			valuesVariable.String(),
			builders.MakeCallExpression(ast.NewIdent("make"), builders.MakeArrayType(ast.NewIdent("string")), builders.MakeBasicLiteralInteger(0), builders.MakeBasicLiteralInteger(len(mutableFields))),
		),
	)
	return AstDataChain{
		Types:     functionTypes,
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
		Name: ast.NewIdent(functionName),
		Type: &ast.FuncType{
			Params: &ast.FieldList{
				List: append(
					[]*ast.Field{
						builders.MakeField("ctx", nil, builders.MakeSelectorExpression("context", "Context")),
					},
					functionArgs...,
				),
			},
			Results: &ast.FieldList{
				List: []*ast.Field{
					builders.MakeField("result", nil, resultExpr),
					builders.MakeField("err", nil, ast.NewIdent("error")),
				},
			},
		},
		Body: &ast.BlockStmt{
			List: functionBody,
		},
	}
}

package dragonfly

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	"github.com/iv-menshenin/go-ast"
	"go/ast"
	"strings"
)

type (
	variableEngine interface {
		makeExpr() ast.Expr
	}
	variableName string
	variableWrap struct {
		variableName variableEngine
		wrapper      func(ast.Expr) ast.Expr
	}
	SQLDataCompareOperator string // TODO try to remove from export

	builderOptions struct {
		appendValueFormat       string
		variableForColumnNames  *variableName
		variableForColumnValues variableName
		variableForColumnExpr   variableName
	}
	executionBlockOptions struct {
		rowVariableName      variableName
		rowStructTypeName    variableName
		variableForSqlText   variableEngine
		variableForArguments variableEngine
	}

	sourceSql interface {
		sqlExpr() string
	}
	sourceSqlColumn struct {
		ColumnName string
	}
	sourceSqlExpression struct {
		Expression string
	}
	sourceSqlSomeColumns struct {
		ColumnNames []string
	}
	dataCellFactory interface {
		getField() *ast.Field
		sqlExpr() string
		isTagExists(tag string) bool
		// generateFindArgumentCode is used to generate intermediate code that processes the value of one of the filter fields for an SQL query; as a result, it returns a piece of code that must perform the actions:
		//  1.checking for null
		//  2.setting additional segments of the where clause in the variable
		//  3.adding values for request placeholders to the array
		//
		// boolean value (the second value of the result) indicates whether it is necessary to create a field for the structure (true) or this value will be generated automatically (false)
		//
		// Example:
		//  // generateFindArgumentCode(filterOptionName, fieldName, option)
		//
		//  if find.Username != nil {
		//    // filterOptionName = 'filter', fieldName = 'Username'
		//    args = append(args, strings.ToLower(find.Username))
		//    filters = append(filters, fmt.Sprintf("%s like '%%'||%s||'%%'", "lower(username)", "$"+strconv.Itoa(len(args))))
		//  }
		generateFindArgumentCode(string, string, builderOptions) ([]ast.Stmt, bool)

		// generateInputArgumentCode is used to generate code that implements the process of processing data inserted into the database of a new record
		//
		// Example:
		//  // generateInputArgumentCode(funcInputOptionName, options, isMaybe, isCustom)
		//
		// 	args = append(args, record.Ogrn) // funcInputOptionName = 'record'
		//	values = append(values, fmt.Sprintf("/* ogrn */ $%d", len(args)))
		generateInputArgumentCode(string, builderOptions, bool, bool) ([]ast.Stmt, bool)
	}
	dataCellField struct {
		field      *ast.Field
		source     sourceSql // sql mirror for field
		tags       []string
		comparator SQLDataCompareOperator
	}
	dataCellFieldCustomType struct {
		dataCell dataCellField
	}
	dataCellFieldMaybeType struct {
		dataCell dataCellField
	}
	dataCellFieldConstant struct {
		dataCell dataCellField
		constant string
	}
	groupedDataCells []dataCellFactory
)

func MakeDataCellFactoryType(
	field *ast.Field,
	source sourceSql,
	tags []string,
	comparator SQLDataCompareOperator,
) dataCellFactory {
	// tagCaseInsensitive
	return dataCellField{
		field:      field,
		source:     source,
		tags:       tags,
		comparator: comparator,
	}
}

func MakeDataCellFactoryConstant(
	field *ast.Field,
	source sourceSql,
	tags []string,
	comparator SQLDataCompareOperator,
	constant string,
) dataCellFactory {
	return dataCellFieldConstant{
		dataCell: dataCellField{
			field:      field,
			source:     source,
			tags:       tags,
			comparator: comparator,
		},
		constant: constant,
	}
}

func MakeDataCellFactoryCustom(
	field *ast.Field,
	source sourceSql,
	tags []string,
	comparator SQLDataCompareOperator,
) dataCellFactory {
	return dataCellFieldCustomType{
		dataCell: dataCellField{
			field:      field,
			source:     source,
			tags:       tags,
			comparator: comparator,
		},
	}
}

func MakeDataCellFactoryMaybe(
	field *ast.Field,
	source sourceSql,
	tags []string,
	comparator SQLDataCompareOperator,
) dataCellFactory {
	return dataCellFieldMaybeType{
		dataCell: dataCellField{
			field:      field,
			source:     source,
			tags:       tags,
			comparator: comparator,
		},
	}
}

func MakeDataCellFactoryGrouped(dataCells []dataCellFactory) dataCellFactory {
	return groupedDataCells(dataCells)
}

func (f dataCellField) getField() *ast.Field {
	return f.field
}

func (f dataCellFieldCustomType) getField() *ast.Field {
	return f.dataCell.getField()
}

func (f dataCellFieldMaybeType) getField() *ast.Field {
	return f.dataCell.getField()
}

func (f dataCellFieldConstant) getField() *ast.Field {
	return f.dataCell.getField()
}

func (f groupedDataCells) getField() *ast.Field {
	panic("unimplemented")
	return nil
}

func (f dataCellField) sqlExpr() string {
	return f.source.sqlExpr()
}

func (f dataCellFieldCustomType) sqlExpr() string {
	return f.dataCell.sqlExpr()
}

func (f dataCellFieldMaybeType) sqlExpr() string {
	return f.dataCell.sqlExpr()
}

func (f dataCellFieldConstant) sqlExpr() string {
	return f.dataCell.sqlExpr()
}

func (f groupedDataCells) sqlExpr() string {
	panic("unimplemented")
	return "null"
}

func (f dataCellField) isTagExists(tag string) bool {
	return utils.ArrayContains(f.tags, tag)
}

func (f dataCellFieldCustomType) isTagExists(tag string) bool {
	return f.dataCell.isTagExists(tag)
}

func (f dataCellFieldMaybeType) isTagExists(tag string) bool {
	return f.dataCell.isTagExists(tag)
}

func (f dataCellFieldConstant) isTagExists(tag string) bool {
	return f.dataCell.isTagExists(tag)
}

func (f groupedDataCells) isTagExists(tag string) bool {
	panic("unimplemented")
	return false
}

func (s sourceSqlColumn) sqlExpr() string {
	return s.ColumnName
}

func (s sourceSqlExpression) sqlExpr() string {
	return s.Expression
}

func (s sourceSqlSomeColumns) sqlExpr() string {
	return strings.Join(s.ColumnNames, ", ")
}

const (
	ArgsVariable    variableName = "args"
	FiltersVariable variableName = "filters"
	FieldsVariable  variableName = "fields"
	ValuesVariable  variableName = "values"

	ScanDestVariable variableName = "row"

	// functions
	generateFunctionHex    = "H"
	generateFunctionAlpha  = "A"
	generateFunctionDigits = "0"
	// column tags
	tagGenerate        = "generate"
	tagEncrypt         = "encrypt"
	tagCaseInsensitive = "ci"
	// sql data comparing variants
	CompareEqual     SQLDataCompareOperator = "equal"
	CompareNotEqual  SQLDataCompareOperator = "notEqual"
	CompareLike      SQLDataCompareOperator = "like"
	CompareNotLike   SQLDataCompareOperator = "notLike"
	CompareIn        SQLDataCompareOperator = "in"
	CompareNotIn     SQLDataCompareOperator = "notIn"
	CompareGreatThan SQLDataCompareOperator = "great"
	CompareLessThan  SQLDataCompareOperator = "less"
	CompareNotGreat  SQLDataCompareOperator = "notGreat"
	CompareNotLess   SQLDataCompareOperator = "notLess"
	CompareStarts    SQLDataCompareOperator = "starts"
	CompareIsNull    SQLDataCompareOperator = "isNull"
)

func (v variableName) String() string {
	return string(v)
}

func (v variableName) makeExpr() ast.Expr {
	return ast.NewIdent(v.String())
}

func (v variableWrap) makeExpr() ast.Expr {
	return v.wrapper(v.variableName.makeExpr())
}

var (
	fieldsVariableRef  = FieldsVariable
	FindBuilderOptions = builderOptions{
		appendValueFormat:       "%s = $%%d",
		variableForColumnNames:  nil,
		variableForColumnValues: "args",
		variableForColumnExpr:   FiltersVariable,
	}
	InsertBuilderOptions = builderOptions{
		appendValueFormat:       "/* %s */ $%%d",
		variableForColumnNames:  &fieldsVariableRef,
		variableForColumnValues: ArgsVariable,
		variableForColumnExpr:   ValuesVariable,
	}
	UpdateBuilderOptions = builderOptions{
		appendValueFormat:       "%s = $%%d",
		variableForColumnNames:  nil,
		variableForColumnValues: ArgsVariable,
		variableForColumnExpr:   FieldsVariable,
	}
	DeleteBuilderOptions = builderOptions{
		appendValueFormat:       "%s = $%%d",
		variableForColumnNames:  nil,
		variableForColumnValues: ArgsVariable,
		variableForColumnExpr:   FiltersVariable,
	}
	IncomingArgumentsBuilderOptions = builderOptions{
		appendValueFormat:       "",
		variableForColumnNames:  nil,
		variableForColumnValues: ArgsVariable,
		variableForColumnExpr:   FiltersVariable,
	}
)

var makeEncryptPasswordCallCustom func(valueForEncrypt ast.Expr) *ast.CallExpr = nil

func makeEncryptPasswordCall(valueForEncrypt ast.Expr) *ast.CallExpr {
	if makeEncryptPasswordCallCustom != nil {
		return makeEncryptPasswordCallCustom(valueForEncrypt)
	}
	return builders.Call(
		builders.CallFunctionDescriber{
			FunctionName:                ast.NewIdent("encryptPassword"),
			MinimumNumberOfArguments:    1,
			ExtensibleNumberOfArguments: false,
		},
		valueForEncrypt,
	)
}

func MakeExecutionOption(rowStructName, sqlVariableName string) executionBlockOptions {
	return executionBlockOptions{
		rowVariableName:      ScanDestVariable,
		rowStructTypeName:    variableName(rowStructName),
		variableForSqlText:   variableName(sqlVariableName),
		variableForArguments: ArgsVariable,
	}
}

func MakeExecutionOptionWithWrappers(rowStructName, sqlVariableName string, sqlText, sqlArgs func(ast.Expr) ast.Expr) executionBlockOptions {
	return executionBlockOptions{
		rowVariableName:   ScanDestVariable,
		rowStructTypeName: variableName(rowStructName),
		variableForSqlText: variableWrap{
			variableName: variableName(sqlVariableName),
			wrapper:      sqlText,
		},
		variableForArguments: variableWrap{
			variableName: ArgsVariable,
			wrapper:      sqlArgs,
		},
	}
}

type (
	ScanWrapper func(...ast.Stmt) ast.Stmt
)

var (
	WrapperFindOne = wrapFetchOnceForScanner
	WrapperFindAll = wrapIteratorForScanner
)

const (
	TagTypeSQL   = "sql"
	TagTypeJSON  = "json"
	TagTypeUnion = "union" // TODO internal, remove from export
)

var (
	compareOperators = []SQLDataCompareOperator{
		CompareEqual,
		CompareNotEqual,
		CompareLike,
		CompareNotLike,
		CompareIn,
		CompareNotIn,
		CompareGreatThan,
		CompareLessThan,
		CompareNotGreat,
		CompareNotLess,
		CompareStarts,
		CompareIsNull,
	}
	multiCompareOperators = []SQLDataCompareOperator{
		CompareIn,
		CompareNotIn,
	}
)

func (c *SQLDataCompareOperator) Check() {
	if c == nil || *c == "" {
		*c = CompareEqual
	}
	for _, op := range compareOperators {
		if op == *c {
			return
		}
	}
	panic(fmt.Sprintf("unknown compare operator '%s'", string(*c)))
}

func (c SQLDataCompareOperator) IsMult() bool {
	for _, op := range multiCompareOperators {
		if op == c {
			return true
		}
	}
	return false
}

var (
	knownOperators = map[SQLDataCompareOperator]iOperator{
		CompareEqual:     opRegular{`%s = %s`},
		CompareNotEqual:  opRegular{`% != %s`},
		CompareLike:      opRegular{`%s like '%%'||%s||'%%'`},
		CompareNotLike:   opRegular{`%s not like '%%'||%s||'%%'`},
		CompareIn:        opRegular{`%s in (%s)`},
		CompareNotIn:     opRegular{`%s not in (%s)`},
		CompareGreatThan: opRegular{`%s > %s`},
		CompareLessThan:  opRegular{`%s < %s`},
		CompareNotGreat:  opRegular{`%s <= %s`},
		CompareNotLess:   opRegular{`%s >= %s`},
		CompareStarts:    opRegular{`%s starts with %s`},
		CompareIsNull:    opInline{`%s is %s`},
	}
)

func (c SQLDataCompareOperator) getBuilder() iOperator {
	c.Check()
	if template, ok := knownOperators[c]; ok {
		return template
	}
	panic(fmt.Sprintf("cannot find template for operator '%s'", string(c)))
}

// ExtractDestinationFieldRefsFromStruct extracts the list of table columns and generates variable field
// references for the output structure. Column and field positions correspond to each other
func ExtractDestinationFieldRefsFromStruct(
	rowVariableName string,
	rowStructureFields []dataCellFactory,
) (
	destinationStructureFields []ast.Expr,
	sourceTableColumnNames []string,
) {
	destinationStructureFields = make([]ast.Expr, 0, len(rowStructureFields))
	sourceTableColumnNames = make([]string, 0, len(rowStructureFields))
	for _, field := range rowStructureFields {
		for _, fName := range field.getField().Names {
			destinationStructureFields = append(destinationStructureFields, builders.Ref(builders.SimpleSelector(rowVariableName, fName.Name)))
			sourceTableColumnNames = append(sourceTableColumnNames, field.sqlExpr())
		}
	}
	return
}

func MakeDatabaseApiFunction(
	functionName string,
	resultExpr []*ast.Field,
	functionBody []ast.Stmt,
	functionArgs ...*ast.Field,
) *ast.FuncDecl {
	return &ast.FuncDecl{
		Name: ast.NewIdent(functionName),
		Type: &ast.FuncType{
			Params: &ast.FieldList{
				List: append(
					[]*ast.Field{
						builders.Field("ctx", nil, builders.ContextType),
					},
					functionArgs...,
				),
			},
			Results: &ast.FieldList{
				List: append(resultExpr, builders.Field("err", nil, ast.NewIdent("error"))),
			},
		},
		Body: &ast.BlockStmt{
			List: functionBody,
		},
	}
}

func BuildExecutionBlockForFunction(
	scanBlock ScanWrapper,
	fieldRefs []ast.Expr,
	options executionBlockOptions,
) []ast.Stmt {
	return []ast.Stmt{
		builders.MakeCallWithErrChecking(
			"rows",
			builders.CallEllipsis(
				builders.DbQueryFn,
				options.variableForSqlText.makeExpr(),
				options.variableForArguments.makeExpr(),
			),
		),
		builders.DeferCall(
			builders.CallFunctionDescriber{
				FunctionName:                builders.SimpleSelector("rows", "Close"),
				MinimumNumberOfArguments:    0,
				ExtensibleNumberOfArguments: false,
			},
		),
		scanBlock(
			builders.Var(builders.VariableType(options.rowVariableName.String(), ast.NewIdent(options.rowStructTypeName.String()))),
			builders.MakeCallWithErrChecking(
				"",
				builders.Call(
					builders.RowsScanFn,
					fieldRefs...,
				),
			),
		),
	}
}

func makeFindProcessorForUnion(
	funcFilterOptionName, fieldName string,
	union []string,
	field dataCellField,
	options builderOptions,
) []ast.Stmt {
	caseInsensitive := field.isTagExists(tagCaseInsensitive)
	if field.comparator.IsMult() {
		panic(fmt.Sprintf("joins cannot be used in multiple expressions, for example '%s' in the expression '%s'", fieldName, field.comparator))
	}
	if _, ok := field.field.Type.(*ast.StarExpr); ok {
		return []ast.Stmt{
			builders.If(
				builders.NotEqual(builders.SimpleSelector(funcFilterOptionName, fieldName), builders.Nil),
				field.comparator.getBuilder().makeUnionQueryOption(builders.Star(builders.SimpleSelector(funcFilterOptionName, fieldName)), union, caseInsensitive, options)...,
			),
		}
	} else {
		return field.comparator.getBuilder().makeUnionQueryOption(builders.SimpleSelector(funcFilterOptionName, fieldName), union, caseInsensitive, options)
	}
}

func (f dataCellField) generateFindArgumentCode(
	funcFilterOptionName, fieldName string,
	options builderOptions,
) (stmt []ast.Stmt, addField bool) {
	addField = true
	caseInsensitive := f.isTagExists(tagCaseInsensitive)
	if f.comparator.IsMult() {
		stmt = f.comparator.getBuilder().makeArrayQueryOption(funcFilterOptionName, fieldName, f.source.sqlExpr(), caseInsensitive, options)
	} else if union, ok := f.source.(sourceSqlSomeColumns); ok {
		stmt = makeFindProcessorForUnion(funcFilterOptionName, fieldName, union.ColumnNames, f, options)
	} else if _, ok := f.field.Type.(*ast.StarExpr); ok {
		stmt = []ast.Stmt{
			builders.If(
				builders.NotEqual(builders.SimpleSelector(funcFilterOptionName, fieldName), builders.Nil),
				f.comparator.getBuilder().makeScalarQueryOption(funcFilterOptionName, fieldName, f.source.sqlExpr(), caseInsensitive, true, options)...,
			),
		}
	} else {
		stmt = f.comparator.getBuilder().makeScalarQueryOption(funcFilterOptionName, fieldName, f.source.sqlExpr(), caseInsensitive, false, options)
	}
	return
}

func (f dataCellFieldConstant) generateFindArgumentCode(
	funcFilterOptionName, _ string,
	options builderOptions,
) (stmt []ast.Stmt, addField bool) {
	caseInsensitive := f.isTagExists(tagCaseInsensitive)
	if f.dataCell.comparator.IsMult() {
		panic("constants cannot be used in multiple expressions")
	}
	var (
		operatorValue = "/* %s */ %s"
		tmpOperator   = f.dataCell.comparator.getBuilder()
	)
	if o, ok := tmpOperator.(opInline); ok {
		operatorValue = o.operator
	} else if o, ok := tmpOperator.(opRegular); ok {
		operatorValue = o.operator
	}
	var newOperator = opConstant{
		opInline: opInline{
			operator: operatorValue,
		},
	}
	stmt = newOperator.makeScalarQueryOption(
		funcFilterOptionName, // TODO ?
		f.constant,
		f.dataCell.source.sqlExpr(),
		caseInsensitive,
		false,
		options,
	)
	addField = false
	return
}

func (f dataCellFieldCustomType) generateFindArgumentCode(
	funcFilterOptionName, fieldName string,
	options builderOptions,
) (stmt []ast.Stmt, addField bool) {
	return f.dataCell.generateFindArgumentCode(funcFilterOptionName, fieldName, options)
}

func (f dataCellFieldMaybeType) generateFindArgumentCode(
	funcFilterOptionName, fieldName string,
	options builderOptions,
) (stmt []ast.Stmt, addField bool) {
	return f.dataCell.generateFindArgumentCode(funcFilterOptionName, fieldName, options)
}

func (f groupedDataCells) generateFindArgumentCode(
	funcFilterOptionName, fieldName string,
	options builderOptions,
) (stmt []ast.Stmt, addField bool) {
	panic("unimplemented")
}

func buildFindArgumentProcessor(
	dataCell dataCellFactory,
	funcFilterOptionName string,
	options builderOptions,
) (
	functionBody []ast.Stmt,
	optionsFieldList []*ast.Field,
) {
	functionBody = make([]ast.Stmt, 0, 10)
	optionsFieldList = make([]*ast.Field, 0, 5)
	if len(dataCell.getField().Names) != 1 {
		panic("not supported names count")
	}
	var fieldName = dataCell.getField().Names[0].Name
	stmts, addField := dataCell.generateFindArgumentCode(funcFilterOptionName, fieldName, options)
	functionBody = append(functionBody, stmts...)
	if addField {
		optionsFieldList = append(optionsFieldList, dataCell.getField())
	}
	return
}

func buildFindSubArgumentProcessor(
	funcFilterOptionName string,
	funcFilterOptionTypeName string,
	optionField groupedDataCells,
	options builderOptions,
) (
	body []ast.Stmt,
	declarations map[string]*ast.TypeSpec,
	optionsFuncField []*ast.Field,
	newVarName variableName,
) {
	var (
		newFieldName       = "Sub"
		allFieldsIsOmitted = true
	)
	for _, mf := range optionField {
		if strings.Index(newFieldName, mf.getField().Names[0].Name) < 0 {
			newFieldName += mf.getField().Names[0].Name
		}
		if _, isConstant := mf.(dataCellFieldConstant); !isConstant {
			allFieldsIsOmitted = false
		}
	}
	var internalOptionName = funcFilterOptionTypeName + newFieldName
	newVarName = options.variableForColumnExpr + variableName(newFieldName)
	var newFieldVarName = "v" + newFieldName
	body, declarations, optionsFuncField = buildFindArgumentsProcessor(newFieldVarName, internalOptionName, optionField, builderOptions{
		appendValueFormat:       options.appendValueFormat,
		variableForColumnNames:  options.variableForColumnNames,
		variableForColumnValues: options.variableForColumnValues,
		variableForColumnExpr:   newVarName,
	})
	var vars []ast.Spec
	if !allFieldsIsOmitted {
		vars = append(vars, builders.VariableValue(newFieldVarName, builders.Selector(ast.NewIdent(funcFilterOptionName), newFieldVarName)))
	}
	vars = append(vars, builders.VariableValue(string(newVarName), builders.Call(builders.MakeFn, builders.ArrayType(builders.String), builders.IntegerConstant(0).Expr())))
	body = append(
		[]ast.Stmt{
			&ast.ExprStmt{X: &ast.BasicLit{Value: "/* process sub-options */"}},
			builders.Var(vars...),
		},
		body...,
	)
	return
}

// buildFindArgumentsProcessor extracts required and optional parameters from incoming arguments, builds program code
//
// returns the body of program code, required type declarations and required input fields
func buildFindArgumentsProcessor(
	funcFilterOptionName string,
	funcFilterOptionTypeName string,
	optionFields []dataCellFactory,
	options builderOptions,
) (
	body []ast.Stmt,
	declarations map[string]*ast.TypeSpec,
	optionsFuncField []*ast.Field, // TODO get rid
) {
	declarations = make(map[string]*ast.TypeSpec)
	var (
		functionBody     = make([]ast.Stmt, 0)
		optionsFieldList = make([]*ast.Field, 0)
	)
	for _, field := range optionFields {
		switch f := field.(type) {
		case groupedDataCells:
			body2, decl2, ff2, newVarName := buildFindSubArgumentProcessor(funcFilterOptionName, funcFilterOptionTypeName, f, options)
			functionBody = append(functionBody, body2...)
			for k, v := range decl2 {
				declarations[k] = v
			}
			// filters = append(filters, "(" + strings.Join(subFilters, " or ") + ")")
			functionBody = append(functionBody, builders.Assign(
				builders.VarNames{options.variableForColumnExpr.String()},
				builders.Assignment,
				builders.Call(builders.AppendFn, options.variableForColumnExpr.makeExpr(), builders.Add(
					builders.StringConstant("(").Expr(),
					builders.Call(builders.StringsJoinFn, newVarName.makeExpr(), builders.StringConstant(" or ").Expr()),
					builders.StringConstant(")").Expr(),
				)),
			))
			optionsFieldList = append(optionsFieldList, ff2...)
		default:
			functionBody = append(
				functionBody,
				&ast.ExprStmt{X: &ast.BasicLit{Value: fmt.Sprintf("/* process find option %v */", field.getField().Names)}},
			)
			functionBodyEx, optionsFieldListEx := buildFindArgumentProcessor(f, funcFilterOptionName, options)
			functionBody = append(functionBody, functionBodyEx...)
			optionsFieldList = append(optionsFieldList, optionsFieldListEx...)
		}
	}
	declarations[funcFilterOptionTypeName] = &ast.TypeSpec{
		Name: ast.NewIdent(funcFilterOptionTypeName),
		Type: &ast.StructType{
			Fields:     &ast.FieldList{List: optionsFieldList},
			Incomplete: false,
		},
	}
	return functionBody,
		declarations,
		[]*ast.Field{
			{
				Names: []*ast.Ident{ast.NewIdent(funcFilterOptionName)},
				Type:  ast.NewIdent(funcFilterOptionTypeName),
			},
		}
}

func (f dataCellField) generateInputArgumentCode(
	funcInputOptionName string,
	options builderOptions,
	isMaybe, isCustom bool, // TODO not clear logic
) (stmt []ast.Stmt, omitted bool) {
	var (
		valueExpr ast.Expr
		tags      = fieldTagToMap(f.field.Tag.Value)
		colName   = f.source
		fieldName = builders.SimpleSelector(funcInputOptionName, f.field.Names[0].Name)
	)
	/* omitted - value will never be requested from the user */
	valueExpr, omitted = makeValuePicker(tags[TagTypeSQL][1:], fieldName)
	/* test wrappers
	if !value.omitted { ... }
	*/
	wrapFunc := func(stmts []ast.Stmt) []ast.Stmt { return stmts }
	if !omitted && isMaybe {
		wrapFunc = func(stmts []ast.Stmt) []ast.Stmt {
			fncName := &ast.SelectorExpr{
				X:   fieldName,
				Sel: ast.NewIdent("IsOmitted"),
			}
			return []ast.Stmt{
				builders.If(
					builders.Not(builders.Call(
						builders.CallFunctionDescriber{
							FunctionName:                fncName,
							MinimumNumberOfArguments:    0,
							ExtensibleNumberOfArguments: false,
						},
					)),
					stmts...,
				),
			}
		}
	}
	_, isStarExpression := f.field.Type.(*ast.StarExpr)
	if isStarExpression && !omitted {
		wrapFunc = func(stmts []ast.Stmt) []ast.Stmt {
			return []ast.Stmt{
				builders.If(builders.NotNil(fieldName), stmts...),
			}
		}
	}
	if !isStarExpression && isCustom {
		valueExpr = builders.Ref(valueExpr)
	}
	if utils.ArrayFind(tags[TagTypeSQL], tagEncrypt) > 0 { // first word is column name
		if _, star := f.field.Type.(*ast.StarExpr); star {
			valueExpr = builders.Star(valueExpr)
		} else if isMaybe {
			valueExpr = builders.Selector(valueExpr, "value")
		}
		valueExpr = makeEncryptPasswordCall(valueExpr)
	}
	stmt = make([]ast.Stmt, 0, 10)
	comment := []ast.Stmt{&ast.ExprStmt{X: &ast.BasicLit{Value: fmt.Sprintf("/* process the input data of %s */", f.getField().Names)}}}
	if options.variableForColumnNames != nil {
		stmt = append(stmt, builders.Assign(
			builders.MakeVarNames(options.variableForColumnNames.String()),
			builders.Assignment,
			builders.Call(builders.AppendFn, ast.NewIdent(options.variableForColumnNames.String()), builders.StringConstant(colName.sqlExpr()).Expr()),
		))
	}
	stmt = append(stmt, makeInputValueProcessor(
		fmt.Sprintf(options.appendValueFormat, colName.sqlExpr()),
		valueExpr,
		options.variableForColumnValues.String(),
		options.variableForColumnExpr.String(),
	)...)
	stmt = append(comment, wrapFunc(stmt)...)
	return
}

func (f dataCellFieldConstant) generateInputArgumentCode(
	funcInputOptionName string,
	options builderOptions,
	isMaybe, isCustom bool,
) (stmt []ast.Stmt, omitted bool) {
	// TODO what we must to do?
	return f.dataCell.generateInputArgumentCode(funcInputOptionName, options, isMaybe, isCustom)
}

func (f dataCellFieldCustomType) generateInputArgumentCode(
	funcInputOptionName string,
	options builderOptions,
	isMaybe, isCustom bool,
) (stmt []ast.Stmt, omitted bool) {
	return f.dataCell.generateInputArgumentCode(funcInputOptionName, options, isMaybe, true)
}

func (f dataCellFieldMaybeType) generateInputArgumentCode(
	funcInputOptionName string,
	options builderOptions,
	isMaybe, isCustom bool,
) (stmt []ast.Stmt, omitted bool) {
	return f.dataCell.generateInputArgumentCode(funcInputOptionName, options, true, isCustom)
}

func (f groupedDataCells) generateInputArgumentCode(
	funcInputOptionName string,
	options builderOptions,
	isMaybe, isCustom bool,
) (stmt []ast.Stmt, omitted bool) {
	panic("not implemented")
}

func buildInputValuesProcessor(
	funcInputOptionName string,
	funcInputOptionTypeName string,
	optionFields []dataCellFactory,
	options builderOptions,
) (
	functionBody []ast.Stmt,
	declarations map[string]*ast.TypeSpec,
	optionsFuncField []*ast.Field, // TODO get rid
) {
	var optionStructFields = make([]*ast.Field, 0, len(optionFields))
	functionBody = make([]ast.Stmt, 0, len(optionFields)*3)
	for _, field := range optionFields {
		stmt, omitted := field.generateInputArgumentCode(funcInputOptionName, options, false, false)
		if !omitted {
			optionStructFields = append(optionStructFields, field.getField())
		}
		functionBody = append(functionBody, stmt...)
	}
	if len(optionStructFields) == 0 {
		return functionBody, map[string]*ast.TypeSpec{}, []*ast.Field{}
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

var (
	stringArray   = ast.NewIdent("SqlStringArray")
	integerArray  = ast.NewIdent("SqlIntegerArray")
	unsignedArray = ast.NewIdent("SqlUnsignedArray")
	floatArray    = ast.NewIdent("SqlFloatArray")
)

func MakeSqlFieldArrayType(expr ast.Expr) ast.Expr {
	if i, ok := expr.(*ast.Ident); ok {
		switch i.Name {
		case "string":
			return stringArray
		case "int", "int4", "int8", "int16", "int32", "int64":
			return integerArray
		case "uint", "uint4", "uint8", "uint16", "uint32", "uint64":
			return unsignedArray
		case "float32", "float64":
			return floatArray
		default:
			return builders.ArrayType(expr)
		}
	} else {
		return builders.ArrayType(expr)
	}
}

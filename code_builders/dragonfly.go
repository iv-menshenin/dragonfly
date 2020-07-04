package builders

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	"go/ast"
	"strings"
)

var (
	registeredGenerators = map[string]CallFunctionDescriber{
		"now": TimeNowFn,
	}
)

func AddNewGenerator(name string, descr CallFunctionDescriber) {
	registeredGenerators[name] = descr
}

func RegisterSqlFieldEncryptFunction(encryptFn func(valueForEncrypt ast.Expr) *ast.CallExpr) {
	if makeEncryptPasswordCallCustom == nil {
		makeEncryptPasswordCallCustom = encryptFn
	} else {
		panic("custom function already registered")
	}
}

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

	SourceSql interface {
		sqlExpr() string
	}
	SourceSqlColumn struct {
		ColumnName string
	}
	SourceSqlExpression struct {
		Expression string
	}
	SourceSqlSomeColumns struct {
		ColumnNames []string
	}
	MetaField struct {
		Field           *ast.Field
		SourceSql       SourceSql // sql mirror for field
		CaseInsensitive bool
		IsMaybeType     bool
		IsCustomType    bool
		CompareOperator SQLDataCompareOperator
		Constant        string
	}
)

func (s SourceSqlColumn) sqlExpr() string {
	return s.ColumnName
}

func (s SourceSqlExpression) sqlExpr() string {
	return s.Expression
}

func (s SourceSqlSomeColumns) sqlExpr() string {
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
	tagGenerate = "generate"
	tagEncrypt  = "encrypt"
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
	return Call(
		CallFunctionDescriber{
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
	WrapperFindOne = scanBlockForFindOnce
	WrapperFindAll = scanBlockForFindAll
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

// get a list of table columns and variable fields references for the output structure.
// column and field positions correspond to each other
func ExtractDestinationFieldRefsFromStruct(
	rowVariableName string,
	rowStructureFields []MetaField,
) (
	destinationStructureFields []ast.Expr,
	sourceTableColumnNames []string,
) {
	destinationStructureFields = make([]ast.Expr, 0, len(rowStructureFields))
	sourceTableColumnNames = make([]string, 0, len(rowStructureFields))
	for _, field := range rowStructureFields {
		for _, fName := range field.Field.Names {
			destinationStructureFields = append(destinationStructureFields, Ref(SimpleSelector(rowVariableName, fName.Name)))
			sourceTableColumnNames = append(sourceTableColumnNames, field.SourceSql.sqlExpr())
		}
	}
	return
}

type (
	resultPair struct {
		n string
		t ast.Expr
	}
)

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
						Field("ctx", nil, ContextType),
					},
					functionArgs...,
				),
			},
			Results: &ast.FieldList{
				List: append(resultExpr, Field("err", nil, ast.NewIdent("error"))),
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
		MakeCallWithErrChecking(
			"rows",
			CallEllipsis(
				DbQueryFn,
				options.variableForSqlText.makeExpr(),
				options.variableForArguments.makeExpr(),
			),
		),
		DeferCall(
			CallFunctionDescriber{SimpleSelector("rows", "Close"), 0, false},
		),
		scanBlock(
			Var(VariableType(options.rowVariableName.String(), ast.NewIdent(options.rowStructTypeName.String()))),
			MakeCallWithErrChecking(
				"",
				Call(
					RowsScanFn,
					fieldRefs...,
				),
			),
		),
	}
}

func makeFindProcessorForUnion(
	funcFilterOptionName, fieldName string,
	union []string,
	field MetaField,
	options builderOptions,
) []ast.Stmt {
	if field.CompareOperator.IsMult() {
		panic(fmt.Sprintf("joins cannot be used in multiple expressions, for example '%s' in the expression '%s'", fieldName, field.CompareOperator))
	}
	if _, ok := field.Field.Type.(*ast.StarExpr); ok {
		return []ast.Stmt{
			If(
				NotEqual(SimpleSelector(funcFilterOptionName, fieldName), Nil),
				field.CompareOperator.getBuilder().makeUnionQueryOption(Star(SimpleSelector(funcFilterOptionName, fieldName)), union, field.CaseInsensitive, options)...,
			),
		}
	} else {
		return field.CompareOperator.getBuilder().makeUnionQueryOption(SimpleSelector(funcFilterOptionName, fieldName), union, field.CaseInsensitive, options)
	}
}

func makeFindProcessorForSingle(
	funcFilterOptionName, fieldName string,
	field MetaField,
	options builderOptions,
) []ast.Stmt {
	if _, ok := field.Field.Type.(*ast.StarExpr); ok {
		return []ast.Stmt{
			If(
				NotEqual(SimpleSelector(funcFilterOptionName, fieldName), Nil),
				field.CompareOperator.getBuilder().makeScalarQueryOption(funcFilterOptionName, fieldName, field.SourceSql.sqlExpr(), field.CaseInsensitive, true, options)...,
			),
		}
	} else {
		return field.CompareOperator.getBuilder().makeScalarQueryOption(funcFilterOptionName, fieldName, field.SourceSql.sqlExpr(), field.CaseInsensitive, false, options)
	}
}

func makeFindProcessorForConst(
	funcFilterOptionName, fieldName string,
	field MetaField,
	options builderOptions,
) []ast.Stmt {
	var (
		operatorValue = "/* %s */ %s"
		tmpOperator   = field.CompareOperator.getBuilder()
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
	return newOperator.makeScalarQueryOption(funcFilterOptionName, field.Constant, field.SourceSql.sqlExpr(), field.CaseInsensitive, false, options)
}

/*
	Extracts required and optional parameters from incoming arguments, builds program code
	Returns the body of program code, required type declarations and required input fields
*/
func BuildFindArgumentsProcessor(
	funcFilterOptionName string,
	funcFilterOptionTypeName string,
	optionFields []MetaField,
	options builderOptions,
) (
	body []ast.Stmt,
	declarations map[string]*ast.TypeSpec,
	optionsFuncField []*ast.Field, // TODO get rid
) {
	var (
		functionBody     = make([]ast.Stmt, 0, len(optionFields)*3)
		optionsFieldList = make([]*ast.Field, 0, len(optionFields))
	)
	for _, field := range optionFields {
		if len(field.Field.Names) != 1 {
			panic("not supported names count")
		}
		var fieldName = field.Field.Names[0].Name
		if union, ok := field.SourceSql.(SourceSqlSomeColumns); ok {
			functionBody = append(functionBody, makeFindProcessorForUnion(funcFilterOptionName, fieldName, union.ColumnNames, field, options)...)
			optionsFieldList = append(optionsFieldList, field.Field)
		} else {
			if field.CompareOperator.IsMult() {
				functionBody = append(
					functionBody,
					field.CompareOperator.getBuilder().makeArrayQueryOption(funcFilterOptionName, fieldName, field.SourceSql.sqlExpr(), field.CaseInsensitive, options)...,
				)
				optionsFieldList = append(optionsFieldList, field.Field)
			} else {
				if field.Constant != "" {
					functionBody = append(functionBody, makeFindProcessorForConst(funcFilterOptionName, fieldName, field, options)...)
				} else {
					functionBody = append(functionBody, makeFindProcessorForSingle(funcFilterOptionName, fieldName, field, options)...)
					optionsFieldList = append(optionsFieldList, field.Field)
				}
			}
		}
	}
	return functionBody,
		map[string]*ast.TypeSpec{
			funcFilterOptionTypeName: {
				Name: ast.NewIdent(funcFilterOptionTypeName),
				Type: &ast.StructType{
					Fields:     &ast.FieldList{List: optionsFieldList},
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

func BuildInputValuesProcessor(
	funcInputOptionName string,
	funcInputOptionTypeName string,
	optionFields []MetaField,
	options builderOptions,
) (
	body []ast.Stmt,
	declarations map[string]*ast.TypeSpec,
	optionsFuncField []*ast.Field, // TODO get rid
) {
	var (
		optionStructFields = make([]*ast.Field, 0, len(optionFields))
		functionBody       = make([]ast.Stmt, 0, len(optionFields)*3)
	)
	for _, field := range optionFields {
		var (
			tags      = utils.FieldTagToMap(field.Field.Tag.Value)
			colName   = field.SourceSql
			fieldName = SimpleSelector(funcInputOptionName, field.Field.Names[0].Name)
		)
		/* isOmittedField - value will never be requested from the user */
		valueExpr, isOmittedField := makeValuePicker(tags[TagTypeSQL][1:], fieldName)
		if !isOmittedField {
			optionStructFields = append(optionStructFields, field.Field)
		}
		/* test wrappers
		if !value.omitted { ... }
		*/
		wrapFunc := func(stmts []ast.Stmt) []ast.Stmt { return stmts }
		if !isOmittedField && field.IsMaybeType {
			wrapFunc = func(stmts []ast.Stmt) []ast.Stmt {
				fncName := &ast.SelectorExpr{
					X:   fieldName,
					Sel: ast.NewIdent("IsOmitted"),
				}
				return []ast.Stmt{
					If(
						Not(Call(
							CallFunctionDescriber{
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
		_, isStarExpression := field.Field.Type.(*ast.StarExpr)
		if isStarExpression && !isOmittedField {
			wrapFunc = func(stmts []ast.Stmt) []ast.Stmt {
				return []ast.Stmt{
					If(NotNil(fieldName), stmts...),
				}
			}
		}
		if !isStarExpression && field.IsCustomType {
			valueExpr = Ref(valueExpr)
		}
		if utils.ArrayFind(tags[TagTypeSQL], tagEncrypt) > 0 {
			if _, star := field.Field.Type.(*ast.StarExpr); star {
				valueExpr = Star(valueExpr)
			} else if field.IsMaybeType {
				valueExpr = Selector(valueExpr, "value")
			}
			valueExpr = makeEncryptPasswordCall(valueExpr)
		}
		functionBody = append(
			functionBody,
			wrapFunc(processValueWrapper(
				colName.sqlExpr(), valueExpr, options,
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
			return ArrayType(expr)
		}
	} else {
		return ArrayType(expr)
	}
}

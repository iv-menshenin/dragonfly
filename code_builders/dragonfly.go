package builders

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	"go/ast"
	"strings"
)

type (
	variableName           string
	SQLDataCompareOperator string // TODO try to remove from export

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
	return MakeCallExpression(
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
		rowVariableName:          ScanDestVariable,
		rowStructTypeName:        variableName(rowStructName),
		variableNameForSqlText:   variableName(sqlVariableName),
		variableNameForArguments: ArgsVariable,
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
	TagMaybeVal  = "maybe"
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
			destinationStructureFields = append(destinationStructureFields, MakeRef(MakeSelectorExpression(rowVariableName, fName.Name)))
			sourceTableColumnNames = append(sourceTableColumnNames, field.SourceSql.sqlExpr())
		}
	}
	return
}

func MakeDatabaseApiFunction(
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
						MakeField("ctx", nil, ContextType),
					},
					functionArgs...,
				),
			},
			Results: &ast.FieldList{
				List: []*ast.Field{
					MakeField("result", nil, resultExpr),
					MakeField("err", nil, ast.NewIdent("error")),
				},
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
		MakeAssignmentWithErrChecking(
			"rows",
			MakeCallExpressionEllipsis(
				DbQueryFn,
				ast.NewIdent(options.variableNameForSqlText.String()),
				ast.NewIdent(options.variableNameForArguments.String()),
			),
		),
		MakeDeferCallStatement(
			CallFunctionDescriber{MakeSelectorExpression("rows", "Close"), 0, false},
		),
		scanBlock(
			MakeVarStatement(MakeVarType(options.rowVariableName.String(), ast.NewIdent(options.rowStructTypeName.String()))),
			MakeAssignmentWithErrChecking(
				"",
				MakeCallExpression(
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
			MakeSimpleIfStatement(
				MakeNotEqualExpression(MakeSelectorExpression(funcFilterOptionName, fieldName), Nil),
				field.CompareOperator.getBuilder().makeUnionQueryOption(MakeStarExpression(MakeSelectorExpression(funcFilterOptionName, fieldName)), union, field.CaseInsensitive, options)...,
			),
		}
	} else {
		return field.CompareOperator.getBuilder().makeUnionQueryOption(MakeSelectorExpression(funcFilterOptionName, fieldName), union, field.CaseInsensitive, options)
	}
}

func makeFindProcessorForSingle(
	funcFilterOptionName, fieldName string,
	field MetaField,
	options builderOptions,
) []ast.Stmt {
	if _, ok := field.Field.Type.(*ast.StarExpr); ok {
		return []ast.Stmt{
			MakeSimpleIfStatement(
				MakeNotEqualExpression(MakeSelectorExpression(funcFilterOptionName, fieldName), Nil),
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
			fieldName = MakeSelectorExpression(funcInputOptionName, field.Field.Names[0].Name)
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
					MakeSimpleIfStatement(
						MakeNotExpression(MakeCallExpression(
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
		if _, ok := field.Field.Type.(*ast.StarExpr); !isOmittedField && ok {
			wrapFunc = func(stmts []ast.Stmt) []ast.Stmt {
				return []ast.Stmt{
					MakeSimpleIfStatement(MakeNotNullExpression(fieldName), stmts...),
				}
			}
		}
		if utils.ArrayFind(tags[TagTypeSQL], tagEncrypt) > 0 {
			if _, star := field.Field.Type.(*ast.StarExpr); star {
				valueExpr = MakeStarExpression(valueExpr)
			} else if field.IsMaybeType {
				valueExpr = MakeSelectorExpressionEx(valueExpr, "value")
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

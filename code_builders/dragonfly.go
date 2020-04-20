package builders

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	"go/ast"
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
)

const (
	ArgsVariable    variableName = "args"
	FiltersVariable variableName = "filters"
	FieldsVariable  variableName = "fields"
	ValuesVariable  variableName = "values"

	ScanDestVariable variableName = "row"

	// functions
	generateFunctionNow    = "now"
	generateFunctionHex    = "H"
	generateFunctionAlpha  = "A"
	generateFunctionDigits = "0"
	// column tags
	tagGenerate        = "generate"
	tagCaseInsensitive = "ci"
	tagEncrypt         = "encrypt"
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
	TagTypeJSON  = "json"
	TagTypeUnion = "union"    // TODO internal, remove from export
	TagTypeOp    = "operator" // TODO internal, remove from export
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
		CompareLike:      opRegular{`%s like %s`},
		CompareNotLike:   opRegular{`%s not like %s`},
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
	rowStructureFields []*ast.Field,
) (
	destinationStructureFields []ast.Expr,
	sourceTableColumnNames []string,
) {
	destinationStructureFields = make([]ast.Expr, 0, len(rowStructureFields))
	sourceTableColumnNames = make([]string, 0, len(rowStructureFields))
	for _, field := range rowStructureFields {
		if field.Tag != nil {
			tags := utils.FieldTagToMap(field.Tag.Value)
			if sqlTags, ok := tags[TagTypeSQL]; ok && len(sqlTags) > 0 && sqlTags[0] != "-" {
				for _, fName := range field.Names {
					destinationStructureFields = append(
						destinationStructureFields,
						MakeRef(MakeSelectorExpression(rowVariableName, fName.Name)),
					)
					sourceTableColumnNames = append(sourceTableColumnNames, sqlTags[0])
				}
			}
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

/*
	Extracts required and optional parameters from incoming arguments, builds program code
	Returns the body of program code, required type declarations and required input fields
*/
func BuildFindArgumentsProcessor(
	funcFilterOptionName string,
	funcFilterOptionTypeName string,
	optionFields []*ast.Field,
	options builderOptions,
) (
	body []ast.Stmt,
	declarations map[string]*ast.TypeSpec,
	optionsFuncField []*ast.Field, // TODO get rid
) {
	var (
		functionBody = make([]ast.Stmt, 0, len(optionFields)*3)
	)
	for _, field := range optionFields {
		tags := utils.FieldTagToMap(field.Tag.Value)
		colName := tags[TagTypeSQL][0]
		ci := utils.ArrayFind(tags[TagTypeSQL], tagCaseInsensitive) > 0
		opTagValue, ok := tags[TagTypeOp]
		if !ok || len(opTagValue) < 1 {
			opTagValue = []string{string(CompareEqual)}
		}
		operator := SQLDataCompareOperator(opTagValue[0])
		if utils.ArrayFind(tags[TagTypeSQL], TagTypeUnion) > 0 {
			columns := tags[TagTypeUnion]
			if operator.IsMult() {
				panic(fmt.Sprintf("joins cannot be used in multiple expressions, for example '%s' in the expression '%s'", field.Names[0].Name, opTagValue[0]))
			}
			functionBody = append(
				functionBody,
				operator.getBuilder().makeUnionQueryOption(MakeSelectorExpression(funcFilterOptionName, field.Names[0].Name), columns, ci, options)...,
			)
		} else {
			if operator.IsMult() {
				functionBody = append(
					functionBody,
					operator.getBuilder().makeArrayQueryOption(funcFilterOptionName, field.Names[0].Name, colName, ci, options)...,
				)
			} else {
				if _, ok := field.Type.(*ast.StarExpr); ok {
					functionBody = append(
						functionBody,
						operator.getBuilder().makeStarQueryOption(funcFilterOptionName, field.Names[0].Name, colName, ci, options)...,
					)
				} else {
					functionBody = append(
						functionBody,
						operator.getBuilder().makeScalarQueryOption(funcFilterOptionName, field.Names[0].Name, colName, ci, false, options)...,
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

func BuildInputValuesProcessor(
	funcInputOptionName string,
	funcInputOptionTypeName string,
	optionFields []*ast.Field,
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
			tags      = utils.FieldTagToMap(field.Tag.Value)
			colName   = tags[TagTypeSQL][0]
			fieldName = MakeSelectorExpression(funcInputOptionName, field.Names[0].Name)
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
						Cond: MakeNotNullExpression(fieldName),
						Body: MakeBlockStmt(stmts...),
					},
				}
			}
		}
		if utils.ArrayFind(tags[TagTypeSQL], tagEncrypt) > 0 {
			encryptPasswordFn := CallFunctionDescriber{
				FunctionName:                ast.NewIdent("encryptPassword"),
				MinimumNumberOfArguments:    1,
				ExtensibleNumberOfArguments: false,
			}
			if _, star := field.Type.(*ast.StarExpr); star {
				valueExpr = MakeCallExpression(
					encryptPasswordFn,
					MakeStarExpression(valueExpr),
				)
			} else {
				valueExpr = MakeCallExpression(
					encryptPasswordFn,
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

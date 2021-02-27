package dragonfly

import (
	"fmt"
	builders "github.com/iv-menshenin/go-ast"
	"go/ast"
	"go/token"
	"regexp"
	"strconv"
	"strings"
)

const (
	sqlSingletonViolationErrorName = "SingletonViolation"
)

type (
	iOperator interface {
		makeArrayQueryOption(string, string, string, bool, builderOptions) []ast.Stmt
		makeUnionQueryOption(ast.Expr, []string, bool, builderOptions) []ast.Stmt
		makeScalarQueryOption(string, string, string, bool, bool, builderOptions) []ast.Stmt
	}
	// the usual type of filter makes `where` statements using placeholders
	// filter values ​​are passed as request arguments in a safe manner
	opRegular struct {
		operator string
	}
	// use this if you want to create a statement without using placeholders
	// then the argument values ​​will be embedded directly into the sql query text
	opInline struct {
		operator string
	}
	// use this option if the filter value is always fixed and not user selectable
	opConstant struct {
		opInline
	}
)

func (op opRegular) makeArrayQueryOption(
	optionName, fieldName, columnName string,
	ci bool,
	options builderOptions,
) []ast.Stmt {
	const (
		localVariable = "opt"
	)
	var optionExpr ast.Expr = ast.NewIdent(localVariable)
	if ci {
		columnName = fmt.Sprintf("lower(%s)", columnName)
		optionExpr = builders.Call(builders.ToLowerFn, optionExpr)
	}
	// for placeholders only
	var arrVariableName = fmt.Sprintf("array%s", fieldName)
	return []ast.Stmt{
		builders.Var(builders.VariableType(arrVariableName, builders.ArrayType(ast.NewIdent("string")))),
		&ast.RangeStmt{
			Key:   ast.NewIdent("_"),
			Value: ast.NewIdent(localVariable),
			X:     builders.SimpleSelector(optionName, fieldName),
			Tok:   token.DEFINE,
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					builders.Assign(builders.MakeVarNames(options.variableForColumnValues.String()), builders.Assignment, builders.Call(builders.AppendFn, ast.NewIdent(options.variableForColumnValues.String()), optionExpr)),
					builders.Assign(
						builders.MakeVarNames(arrVariableName),
						builders.Assignment,
						builders.Call(
							builders.AppendFn,
							ast.NewIdent(arrVariableName),
							builders.Add(
								builders.StringConstant("$").Expr(),
								builders.Call(
									builders.ConvertItoaFn,
									builders.Call(builders.LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
								),
							),
						),
					),
				},
			},
		},
		builders.If(
			builders.MakeLenGreatThanZero(arrVariableName),
			builders.Assign(
				builders.MakeVarNames(options.variableForColumnExpr.String()),
				builders.Assignment,
				builders.Call(
					builders.AppendFn,
					ast.NewIdent(options.variableForColumnExpr.String()),
					builders.Call(
						builders.SprintfFn,
						builders.StringConstant(op.operator).Expr(),
						builders.StringConstant(columnName).Expr(),
						builders.Call(
							builders.StringsJoinFn,
							ast.NewIdent(arrVariableName),
							builders.StringConstant(", ").Expr(),
						),
					),
				),
			),
		),
	}
}

func (op opRegular) makeUnionQueryOption(
	optionExpr ast.Expr,
	columnNames []string,
	ci bool,
	options builderOptions,
) []ast.Stmt {
	if ci {
		for i, c := range columnNames {
			columnNames[i] = fmt.Sprintf("lower(%s)", c)
		}
		optionExpr = builders.Call(builders.ToLowerFn, optionExpr)
	}
	operators := make([]string, 0, len(op.operator))
	for _, _ = range columnNames {
		operators = append(operators, op.operator)
	}
	callArgs := make([]ast.Expr, 0, len(columnNames)*2)
	for _, c := range columnNames {
		callArgs = append(
			callArgs,
			builders.StringConstant(c).Expr(),
			builders.Add(
				builders.StringConstant("$").Expr(),
				builders.Call(
					builders.ConvertItoaFn,
					builders.Call(builders.LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
				),
			),
		)
	}
	return []ast.Stmt{
		builders.Assign(
			builders.MakeVarNames(options.variableForColumnValues.String()),
			builders.Assignment,
			builders.Call(
				builders.AppendFn,
				ast.NewIdent(options.variableForColumnValues.String()),
				optionExpr,
			),
		),
		builders.Assign(
			builders.MakeVarNames(options.variableForColumnExpr.String()),
			builders.Assignment,
			builders.Call(
				builders.AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				builders.Call(
					builders.SprintfFn,
					append(builders.E(builders.StringConstant(strings.Join(operators, " or ")).Expr()), callArgs...)...,
				),
			),
		),
	}
}

func (op opRegular) makeScalarQueryOption(
	optionName, fieldName, columnName string,
	ci, ref bool,
	options builderOptions,
) []ast.Stmt {
	var optionExpr = builders.SimpleSelector(optionName, fieldName)
	if ref {
		optionExpr = builders.Star(optionExpr)
	}
	if ci {
		columnName = fmt.Sprintf("lower(%s)", columnName)
		optionExpr = builders.Call(builders.ToLowerFn, optionExpr)
	}
	return []ast.Stmt{
		builders.Assign(
			builders.MakeVarNames(options.variableForColumnValues.String()),
			builders.Assignment,
			builders.Call(
				builders.AppendFn,
				ast.NewIdent(options.variableForColumnValues.String()),
				optionExpr,
			),
		),
		builders.Assign(
			builders.MakeVarNames(options.variableForColumnExpr.String()),
			builders.Assignment,
			builders.Call(
				builders.AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				builders.Call(
					builders.SprintfFn,
					builders.StringConstant(op.operator).Expr(),
					builders.StringConstant(columnName).Expr(),
					builders.Add(
						builders.StringConstant("$").Expr(),
						builders.Call(
							builders.ConvertItoaFn,
							builders.Call(builders.LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
						),
					),
				),
			),
		),
	}
}

// TODO
func (op opInline) makeArrayQueryOption(
	optionName, fieldName, columnName string,
	ci bool,
	options builderOptions,
) []ast.Stmt {
	const (
		localVariable = "opt"
	)
	var optionExpr ast.Expr = ast.NewIdent(localVariable)
	if ci {
		columnName = fmt.Sprintf("lower(%s)", columnName)
		optionExpr = builders.Call(builders.ToLowerFn, optionExpr)
	}
	// for placeholders only
	var arrVariableName = fmt.Sprintf("array%s", fieldName)
	return []ast.Stmt{
		builders.Var(builders.VariableType(arrVariableName, builders.ArrayType(ast.NewIdent("string")))),
		&ast.RangeStmt{
			Key:   ast.NewIdent("_"),
			Value: ast.NewIdent(localVariable),
			X:     builders.SimpleSelector(optionName, fieldName),
			Tok:   token.DEFINE,
			Body: builders.Block(
				builders.Assign(builders.MakeVarNames(options.variableForColumnValues.String()), builders.Assignment, builders.Call(builders.AppendFn, ast.NewIdent(options.variableForColumnValues.String()), optionExpr)),
				builders.Assign(
					builders.MakeVarNames(arrVariableName),
					builders.Assignment,
					builders.Call(
						builders.AppendFn,
						ast.NewIdent(arrVariableName),
						builders.Add(
							builders.StringConstant("$").Expr(),
							builders.Call(
								builders.ConvertItoaFn,
								builders.Call(builders.LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
							),
						),
					),
				),
			),
		},
		builders.If(
			builders.MakeLenGreatThanZero(arrVariableName),
			builders.Assign(
				builders.MakeVarNames(options.variableForColumnExpr.String()),
				builders.Assignment,
				builders.Call(
					builders.AppendFn,
					ast.NewIdent(options.variableForColumnExpr.String()),
					builders.Call(
						builders.SprintfFn,
						builders.StringConstant(op.operator).Expr(),
						builders.StringConstant(columnName).Expr(),
						builders.Call(
							builders.StringsJoinFn,
							ast.NewIdent(arrVariableName),
							builders.StringConstant(", ").Expr(),
						),
					),
				),
			),
		),
	}
}

// it is not advisable to use inline expressions in the sql query text
// one of the few examples for proper application is isNull or isNotNull
// given this, the implementation of union expressions failed
func (op opInline) makeUnionQueryOption(
	optionExpr ast.Expr,
	columnNames []string,
	ci bool,
	options builderOptions,
) []ast.Stmt {
	panic("not implemented")
}

func (op opInline) makeScalarQueryOption(
	optionName, fieldName, columnName string,
	ci, ref bool,
	options builderOptions,
) []ast.Stmt {
	var optionExpr = builders.SimpleSelector(optionName, fieldName)
	if ref {
		optionExpr = builders.Star(optionExpr)
	}
	return []ast.Stmt{
		builders.Assign(
			builders.MakeVarNames(options.variableForColumnExpr.String()),
			builders.Assignment,
			builders.Call(
				builders.AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				builders.Call(
					builders.SprintfFn,
					builders.StringConstant(op.operator).Expr(),
					builders.StringConstant(columnName).Expr(),
					optionExpr,
				),
			),
		),
	}
}

func (op *opConstant) makeScalarQueryOption(
	optionName, constantValue, columnName string,
	ci, ref bool,
	options builderOptions,
) []ast.Stmt {
	return []ast.Stmt{
		builders.Assign(
			builders.MakeVarNames(options.variableForColumnExpr.String()),
			builders.Assignment,
			builders.Call(
				builders.AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				builders.Call(
					builders.SprintfFn,
					builders.StringConstant(op.operator).Expr(),
					builders.StringConstant(columnName).Expr(),
					builders.StringConstant(constantValue).Expr(),
				),
			),
		),
	}
}

func doFuncPicker(funcName string, funcArgs ...string) ast.Expr {
	switch funcName {
	case tagGenerate:
		if len(funcArgs) == 0 {
			panic("tag contains 'generate' function without any argument")
		}
		if userDefinedFunction, ok := registeredGenerators[funcArgs[0]]; ok {
			var args = make([]ast.Expr, 0, len(funcArgs)-1)
			for _, arg := range funcArgs[1:] {
				args = append(args, ast.NewIdent(arg))
			}
			return builders.Call(userDefinedFunction, args...)
		}
		var funcNames = map[string]string{
			generateFunctionHex:    "randomHex",
			generateFunctionAlpha:  "randomAlpha",
			generateFunctionDigits: "randomDigits",
		}
		if goFncName, ok := funcNames[funcArgs[0]]; ok {
			var l = 16
			if len(funcArgs) > 1 {
				i, err := strconv.ParseInt(funcArgs[1], 10, 64)
				if err != nil {
					panic(err)
				}
				l = int(i)
			}
			return builders.Call(
				builders.CallFunctionDescriber{
					FunctionName:                ast.NewIdent(goFncName),
					MinimumNumberOfArguments:    1,
					ExtensibleNumberOfArguments: false,
				},
				builders.IntegerConstant(l).Expr(),
			)
		}
	}
	return nil
}

var (
	fncTemplate = regexp.MustCompile(`^(\w+)\(([^)]*)\)$`)
)

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

func processValueWrapper(
	colName string,
	field ast.Expr,
	options builderOptions,
) []ast.Stmt {
	stmts := make([]ast.Stmt, 0, 3)
	if options.variableForColumnNames != nil {
		stmts = append(stmts, builders.Assign(
			builders.MakeVarNames(options.variableForColumnNames.String()),
			builders.Assignment,
			builders.Call(builders.AppendFn, ast.NewIdent(options.variableForColumnNames.String()), builders.StringConstant(colName).Expr()),
		))
	}
	return append(
		stmts,
		builders.Assign(
			builders.MakeVarNames(options.variableForColumnValues.String()),
			builders.Assignment,
			builders.Call(builders.AppendFn, ast.NewIdent(options.variableForColumnValues.String()), field),
		),
		builders.Assign(
			builders.MakeVarNames(options.variableForColumnExpr.String()),
			builders.Assignment,
			builders.Call(
				builders.AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				builders.Call(
					builders.SprintfFn,
					builders.StringConstant(fmt.Sprintf(options.appendValueFormat, colName)).Expr(),
					builders.Call(builders.LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
				),
			),
		),
	)
}

func scanBlockForFindOnce(stmts ...ast.Stmt) ast.Stmt {
	return builders.If(
		builders.Call(builders.RowsNextFn),
		append(
			append(
				[]ast.Stmt{
					builders.MakeCallWithErrChecking(
						"",
						builders.Call(
							builders.RowsErrFn,
						),
					),
				},
				stmts...,
			),
			&ast.IfStmt{
				Cond: builders.Call(builders.RowsNextFn),
				Body: builders.Block(
					builders.Return(
						ast.NewIdent("row"),
						ast.NewIdent(sqlSingletonViolationErrorName),
					),
				),
				Else: builders.Return(
					ast.NewIdent("row"),
					builders.Nil,
				),
			},
		)...,
	)
}

func scanBlockForFindAll(stmts ...ast.Stmt) ast.Stmt {
	return &ast.ForStmt{
		Cond: builders.Call(builders.RowsNextFn),
		Body: builders.Block(
			append(
				append(
					[]ast.Stmt{
						builders.MakeCallWithErrChecking(
							"",
							builders.Call(
								builders.RowsErrFn,
							),
						),
					},
					stmts...,
				),
				builders.Assign(
					builders.MakeVarNames("result"),
					builders.Assignment,
					builders.Call(
						builders.AppendFn,
						ast.NewIdent("result"),
						ast.NewIdent("row"),
					),
				),
			)...,
		),
	}
}

func arrayFind(a []string, s string) int {
	for i, elem := range a {
		if elem == s {
			return i
		}
	}
	return -1
}

var (
	tagsPattern   = regexp.MustCompile("^`((\\s*[a-z]+\\s*:\\s*\"[^\"]*\"\\s*)*)`$")
	tagsExtractor = regexp.MustCompile("\\s*[a-z]+\\s*:\\s*\"[^\"]*\"\\s*")
	tagPattern    = regexp.MustCompile("\\s*([a-z]+)\\s*:\\s*\"([^\"]*)\"\\s*")
)

func fieldTagToMap(tag string) (result map[string][]string) {
	result = make(map[string][]string, 10)
	sub := tagsPattern.FindAllStringSubmatch(tag, -1)
	if len(sub) > 0 {
		tagsUnquoted := sub[0][1]
		extracted := tagsExtractor.FindAllString(tagsUnquoted, -1)
		for _, tagChain := range extracted {
			tagSmt := tagPattern.FindAllStringSubmatch(tagChain, -1)
			list := strings.Split(tagSmt[0][2], ",")
			for i, current := range list {
				list[i] = strings.TrimSpace(current)
			}
			result[tagSmt[0][1]] = list
		}
	}
	return
}

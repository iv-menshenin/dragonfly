package builders

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
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
		makeStarQueryOption(string, string, string, bool, builderOptions) []ast.Stmt
	}
	opRegular struct {
		operator string
	}
	opInline struct {
		operator string
	}
)

func makeImportSpec(lPos *token.Pos, imports map[string]string) []ast.Spec {
	var impSpec = make([]ast.Spec, 0, len(imports))
	for packageKey, packagePath := range imports {
		packageAlias := ast.NewIdent(packageKey)
		pathSgms := strings.Split(packagePath, "/")
		if pathSgms[len(pathSgms)-1] == packageKey {
			packageAlias = nil
		} else {
			packageAlias.NamePos = *lPos
		}
		impSpec = append(impSpec, &ast.ImportSpec{
			Name: packageAlias,
			Path: &ast.BasicLit{
				ValuePos: *lPos,
				Kind:     token.STRING,
				Value:    fmt.Sprintf("\"%s\"", packagePath),
			},
		})
		*lPos++
	}
	return impSpec
}

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
		optionExpr = MakeCallExpression(ToLowerFn, optionExpr)
	}
	// for placeholders only
	var arrVariableName = fmt.Sprintf("array%s", fieldName)
	return []ast.Stmt{
		MakeVarStatement(MakeVarType(arrVariableName, MakeArrayType(ast.NewIdent("string")))),
		&ast.RangeStmt{
			Key:   ast.NewIdent("_"),
			Value: ast.NewIdent(localVariable),
			X:     MakeSelectorExpression(optionName, fieldName),
			Tok:   token.DEFINE,
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					MakeAssignment([]string{options.variableForColumnValues.String()}, MakeCallExpression(AppendFn, ast.NewIdent(options.variableForColumnValues.String()), optionExpr)),
					MakeAssignment(
						[]string{arrVariableName},
						MakeCallExpression(
							AppendFn,
							ast.NewIdent(arrVariableName),
							MakeAddExpressions(
								MakeBasicLiteralString("$"),
								MakeCallExpression(
									ConvertItoaFn,
									MakeCallExpression(LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
								),
							),
						),
					),
				},
			},
		},
		&ast.IfStmt{
			Cond: MakeNotEmptyArrayExpression(arrVariableName),
			Body: MakeBlockStmt(
				MakeAssignment(
					[]string{options.variableForColumnExpr.String()},
					MakeCallExpression(
						AppendFn,
						ast.NewIdent(options.variableForColumnExpr.String()),
						MakeCallExpression(
							SprintfFn,
							MakeBasicLiteralString(op.operator),
							MakeBasicLiteralString(columnName),
							MakeCallExpression(
								StringsJoinFn,
								ast.NewIdent(arrVariableName),
								MakeBasicLiteralString(", "),
							),
						),
					),
				),
			),
		},
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
		optionExpr = MakeCallExpression(ToLowerFn, optionExpr)
	}
	operators := make([]string, 0, len(op.operator))
	for _, _ = range columnNames {
		operators = append(operators, op.operator)
	}
	callArgs := make([]ast.Expr, 0, len(columnNames)*2)
	for _, c := range columnNames {
		callArgs = append(
			callArgs,
			MakeBasicLiteralString(c),
			MakeAddExpressions(
				MakeBasicLiteralString("$"),
				MakeCallExpression(
					ConvertItoaFn,
					MakeCallExpression(LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
				),
			),
		)
	}
	return []ast.Stmt{
		MakeAssignment(
			[]string{options.variableForColumnValues.String()},
			MakeCallExpression(
				AppendFn,
				ast.NewIdent(options.variableForColumnValues.String()),
				optionExpr,
			),
		),
		MakeAssignment(
			[]string{options.variableForColumnExpr.String()},
			MakeCallExpression(
				AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				MakeCallExpression(
					SprintfFn,
					append([]ast.Expr{MakeBasicLiteralString(strings.Join(operators, " or "))}, callArgs...)...,
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
	var optionExpr = MakeSelectorExpression(optionName, fieldName)
	if ref {
		optionExpr = MakeStarExpression(optionExpr)
	}
	if ci {
		columnName = fmt.Sprintf("lower(%s)", columnName)
		optionExpr = MakeCallExpression(ToLowerFn, optionExpr)
	}
	return []ast.Stmt{
		MakeAssignment(
			[]string{options.variableForColumnValues.String()},
			MakeCallExpression(
				AppendFn,
				ast.NewIdent(options.variableForColumnValues.String()),
				optionExpr,
			),
		),
		MakeAssignment(
			[]string{options.variableForColumnExpr.String()},
			MakeCallExpression(
				AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				MakeCallExpression(
					SprintfFn,
					MakeBasicLiteralString(op.operator),
					MakeBasicLiteralString(columnName),
					MakeAddExpressions(
						MakeBasicLiteralString("$"),
						MakeCallExpression(
							ConvertItoaFn,
							MakeCallExpression(LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
						),
					),
				),
			),
		),
	}
}

func (op opRegular) makeStarQueryOption(
	optionName, fieldName, columnName string,
	ci bool,
	options builderOptions,
) []ast.Stmt {
	return []ast.Stmt{
		&ast.IfStmt{
			Cond: MakeNotEqualExpression(MakeSelectorExpression(optionName, fieldName), Nil),
			Body: MakeBlockStmt(
				op.makeScalarQueryOption(optionName, fieldName, columnName, ci, true, options)...,
			),
		},
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
		optionExpr = MakeCallExpression(ToLowerFn, optionExpr)
	}
	// for placeholders only
	var arrVariableName = fmt.Sprintf("array%s", fieldName)
	return []ast.Stmt{
		MakeVarStatement(MakeVarType(arrVariableName, MakeArrayType(ast.NewIdent("string")))),
		&ast.RangeStmt{
			Key:   ast.NewIdent("_"),
			Value: ast.NewIdent(localVariable),
			X:     MakeSelectorExpression(optionName, fieldName),
			Tok:   token.DEFINE,
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					MakeAssignment([]string{options.variableForColumnValues.String()}, MakeCallExpression(AppendFn, ast.NewIdent(options.variableForColumnValues.String()), optionExpr)),
					MakeAssignment(
						[]string{arrVariableName},
						MakeCallExpression(
							AppendFn,
							ast.NewIdent(arrVariableName),
							MakeAddExpressions(
								MakeBasicLiteralString("$"),
								MakeCallExpression(
									ConvertItoaFn,
									MakeCallExpression(LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
								),
							),
						),
					),
				},
			},
		},
		&ast.IfStmt{
			Cond: MakeNotEmptyArrayExpression(arrVariableName),
			Body: MakeBlockStmt(
				MakeAssignment(
					[]string{options.variableForColumnExpr.String()},
					MakeCallExpression(
						AppendFn,
						ast.NewIdent(options.variableForColumnExpr.String()),
						MakeCallExpression(
							SprintfFn,
							MakeBasicLiteralString(op.operator),
							MakeBasicLiteralString(columnName),
							MakeCallExpression(
								StringsJoinFn,
								ast.NewIdent(arrVariableName),
								MakeBasicLiteralString(", "),
							),
						),
					),
				),
			),
		},
	}
}

// TODO
func (op opInline) makeUnionQueryOption(
	optionExpr ast.Expr,
	columnNames []string,
	ci bool,
	options builderOptions,
) []ast.Stmt {
	if ci {
		for i, c := range columnNames {
			columnNames[i] = fmt.Sprintf("lower(%s)", c)
		}
		optionExpr = MakeCallExpression(ToLowerFn, optionExpr)
	}
	operators := make([]string, 0, len(op.operator))
	for _, _ = range columnNames {
		operators = append(operators, op.operator)
	}
	callArgs := make([]ast.Expr, 0, len(columnNames)*2)
	for _, c := range columnNames {
		callArgs = append(
			callArgs,
			MakeBasicLiteralString(c),
			MakeAddExpressions(
				MakeBasicLiteralString("$"),
				MakeCallExpression(
					ConvertItoaFn,
					MakeCallExpression(LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
				),
			),
		)
	}
	return []ast.Stmt{
		MakeAssignment(
			[]string{options.variableForColumnValues.String()},
			MakeCallExpression(
				AppendFn,
				ast.NewIdent(options.variableForColumnValues.String()),
				optionExpr,
			),
		),
		MakeAssignment(
			[]string{options.variableForColumnExpr.String()},
			MakeCallExpression(
				AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				MakeCallExpression(
					SprintfFn,
					append([]ast.Expr{MakeBasicLiteralString(strings.Join(operators, " or "))}, callArgs...)...,
				),
			),
		),
	}
}

func (op opInline) makeScalarQueryOption(
	optionName, fieldName, columnName string,
	ci, ref bool,
	options builderOptions,
) []ast.Stmt {
	var optionExpr = MakeSelectorExpression(optionName, fieldName)
	if ref {
		optionExpr = MakeStarExpression(optionExpr)
	}
	return []ast.Stmt{
		MakeAssignment(
			[]string{options.variableForColumnExpr.String()},
			MakeCallExpression(
				AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				MakeCallExpression(
					SprintfFn,
					MakeBasicLiteralString(op.operator),
					MakeBasicLiteralString(columnName),
					optionExpr,
				),
			),
		),
	}
}

func (op opInline) makeStarQueryOption(
	optionName, fieldName, columnName string,
	ci bool,
	options builderOptions,
) []ast.Stmt {
	return []ast.Stmt{
		&ast.IfStmt{
			Cond: MakeNotEqualExpression(MakeSelectorExpression(optionName, fieldName), Nil),
			Body: MakeBlockStmt(
				op.makeScalarQueryOption(optionName, fieldName, columnName, ci, true, options)...,
			),
		},
	}
}

func doFuncPicker(funcName string, funcArgs ...string) ast.Expr {
	switch funcName {
	case tagGenerate:
		if len(funcArgs) == 0 {
			panic("tag contains 'generate' function without any argument")
		}
		if strings.EqualFold(funcArgs[0], generateFunctionNow) {
			return MakeCallExpression(TimeNowFn)
		}
		// functions with 'len' argument
		if utils.ArrayContains([]string{
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
			return MakeCallExpression(
				CallFunctionDescriber{
					FunctionName:                ast.NewIdent(goFncName),
					MinimumNumberOfArguments:    1,
					ExtensibleNumberOfArguments: false,
				},
				MakeBasicLiteralInteger(l),
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
		stmts = append(stmts, MakeAssignment(
			[]string{options.variableForColumnNames.String()},
			MakeCallExpression(AppendFn, ast.NewIdent(options.variableForColumnNames.String()), MakeBasicLiteralString(colName)),
		))
	}
	return append(
		stmts,
		MakeAssignment(
			[]string{options.variableForColumnValues.String()},
			MakeCallExpression(AppendFn, ast.NewIdent(options.variableForColumnValues.String()), field),
		),
		MakeAssignment(
			[]string{options.variableForColumnExpr.String()},
			MakeCallExpression(
				AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				MakeCallExpression(
					SprintfFn,
					MakeBasicLiteralString(fmt.Sprintf(options.appendValueFormat, colName)),
					MakeCallExpression(LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
				),
			),
		),
	)
}

func scanBlockForFindOnce(stmts ...ast.Stmt) ast.Stmt {
	return &ast.IfStmt{
		Cond: MakeCallExpression(RowsNextFn),
		Body: MakeBlockStmt(
			append(
				append(
					[]ast.Stmt{
						MakeAssignmentWithErrChecking(
							"",
							MakeCallExpression(
								RowsErrFn,
							),
						),
					},
					stmts...,
				),
				&ast.IfStmt{
					Cond: MakeCallExpression(RowsNextFn),
					Body: MakeBlockStmt(
						MakeReturn(
							ast.NewIdent("row"),
							ast.NewIdent(sqlSingletonViolationErrorName),
						),
					),
					Else: MakeReturn(
						ast.NewIdent("row"),
						Nil,
					),
				},
			)...,
		),
	}
}

func scanBlockForFindAll(stmts ...ast.Stmt) ast.Stmt {
	return &ast.ForStmt{
		Cond: MakeCallExpression(RowsNextFn),
		Body: MakeBlockStmt(
			append(
				append(
					[]ast.Stmt{
						MakeAssignmentWithErrChecking(
							"",
							MakeCallExpression(
								RowsErrFn,
							),
						),
					},
					stmts...,
				),
				MakeAssignment(
					[]string{"result"},
					MakeCallExpression(
						AppendFn,
						ast.NewIdent("result"),
						ast.NewIdent("row"),
					),
				),
			)...,
		),
	}
}

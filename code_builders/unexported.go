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
		optionExpr = Call(ToLowerFn, optionExpr)
	}
	// for placeholders only
	var arrVariableName = fmt.Sprintf("array%s", fieldName)
	return []ast.Stmt{
		Var(VariableType(arrVariableName, ArrayType(ast.NewIdent("string")))),
		&ast.RangeStmt{
			Key:   ast.NewIdent("_"),
			Value: ast.NewIdent(localVariable),
			X:     SimpleSelector(optionName, fieldName),
			Tok:   token.DEFINE,
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					Assign(MakeVarNames(options.variableForColumnValues.String()), Assignment, Call(AppendFn, ast.NewIdent(options.variableForColumnValues.String()), optionExpr)),
					Assign(
						MakeVarNames(arrVariableName),
						Assignment,
						Call(
							AppendFn,
							ast.NewIdent(arrVariableName),
							Add(
								StringConstant("$").Expr(),
								Call(
									ConvertItoaFn,
									Call(LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
								),
							),
						),
					),
				},
			},
		},
		If(
			MakeLenGreatThanZero(arrVariableName),
			Assign(
				MakeVarNames(options.variableForColumnExpr.String()),
				Assignment,
				Call(
					AppendFn,
					ast.NewIdent(options.variableForColumnExpr.String()),
					Call(
						SprintfFn,
						StringConstant(op.operator).Expr(),
						StringConstant(columnName).Expr(),
						Call(
							StringsJoinFn,
							ast.NewIdent(arrVariableName),
							StringConstant(", ").Expr(),
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
		optionExpr = Call(ToLowerFn, optionExpr)
	}
	operators := make([]string, 0, len(op.operator))
	for _, _ = range columnNames {
		operators = append(operators, op.operator)
	}
	callArgs := make([]ast.Expr, 0, len(columnNames)*2)
	for _, c := range columnNames {
		callArgs = append(
			callArgs,
			StringConstant(c).Expr(),
			Add(
				StringConstant("$").Expr(),
				Call(
					ConvertItoaFn,
					Call(LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
				),
			),
		)
	}
	return []ast.Stmt{
		Assign(
			MakeVarNames(options.variableForColumnValues.String()),
			Assignment,
			Call(
				AppendFn,
				ast.NewIdent(options.variableForColumnValues.String()),
				optionExpr,
			),
		),
		Assign(
			MakeVarNames(options.variableForColumnExpr.String()),
			Assignment,
			Call(
				AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				Call(
					SprintfFn,
					append(E(StringConstant(strings.Join(operators, " or ")).Expr()), callArgs...)...,
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
	var optionExpr = SimpleSelector(optionName, fieldName)
	if ref {
		optionExpr = Star(optionExpr)
	}
	if ci {
		columnName = fmt.Sprintf("lower(%s)", columnName)
		optionExpr = Call(ToLowerFn, optionExpr)
	}
	return []ast.Stmt{
		Assign(
			MakeVarNames(options.variableForColumnValues.String()),
			Assignment,
			Call(
				AppendFn,
				ast.NewIdent(options.variableForColumnValues.String()),
				optionExpr,
			),
		),
		Assign(
			MakeVarNames(options.variableForColumnExpr.String()),
			Assignment,
			Call(
				AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				Call(
					SprintfFn,
					StringConstant(op.operator).Expr(),
					StringConstant(columnName).Expr(),
					Add(
						StringConstant("$").Expr(),
						Call(
							ConvertItoaFn,
							Call(LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
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
		optionExpr = Call(ToLowerFn, optionExpr)
	}
	// for placeholders only
	var arrVariableName = fmt.Sprintf("array%s", fieldName)
	return []ast.Stmt{
		Var(VariableType(arrVariableName, ArrayType(ast.NewIdent("string")))),
		&ast.RangeStmt{
			Key:   ast.NewIdent("_"),
			Value: ast.NewIdent(localVariable),
			X:     SimpleSelector(optionName, fieldName),
			Tok:   token.DEFINE,
			Body: Block(
				Assign(MakeVarNames(options.variableForColumnValues.String()), Assignment, Call(AppendFn, ast.NewIdent(options.variableForColumnValues.String()), optionExpr)),
				Assign(
					MakeVarNames(arrVariableName),
					Assignment,
					Call(
						AppendFn,
						ast.NewIdent(arrVariableName),
						Add(
							StringConstant("$").Expr(),
							Call(
								ConvertItoaFn,
								Call(LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
							),
						),
					),
				),
			),
		},
		If(
			MakeLenGreatThanZero(arrVariableName),
			Assign(
				MakeVarNames(options.variableForColumnExpr.String()),
				Assignment,
				Call(
					AppendFn,
					ast.NewIdent(options.variableForColumnExpr.String()),
					Call(
						SprintfFn,
						StringConstant(op.operator).Expr(),
						StringConstant(columnName).Expr(),
						Call(
							StringsJoinFn,
							ast.NewIdent(arrVariableName),
							StringConstant(", ").Expr(),
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
	var optionExpr = SimpleSelector(optionName, fieldName)
	if ref {
		optionExpr = Star(optionExpr)
	}
	return []ast.Stmt{
		Assign(
			MakeVarNames(options.variableForColumnExpr.String()),
			Assignment,
			Call(
				AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				Call(
					SprintfFn,
					StringConstant(op.operator).Expr(),
					StringConstant(columnName).Expr(),
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
		Assign(
			MakeVarNames(options.variableForColumnExpr.String()),
			Assignment,
			Call(
				AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				Call(
					SprintfFn,
					StringConstant(op.operator).Expr(),
					StringConstant(columnName).Expr(),
					StringConstant(constantValue).Expr(),
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
			return Call(userDefinedFunction, args...)
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
			return Call(
				CallFunctionDescriber{
					FunctionName:                ast.NewIdent(goFncName),
					MinimumNumberOfArguments:    1,
					ExtensibleNumberOfArguments: false,
				},
				IntegerConstant(l).Expr(),
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
		stmts = append(stmts, Assign(
			MakeVarNames(options.variableForColumnNames.String()),
			Assignment,
			Call(AppendFn, ast.NewIdent(options.variableForColumnNames.String()), StringConstant(colName).Expr()),
		))
	}
	return append(
		stmts,
		Assign(
			MakeVarNames(options.variableForColumnValues.String()),
			Assignment,
			Call(AppendFn, ast.NewIdent(options.variableForColumnValues.String()), field),
		),
		Assign(
			MakeVarNames(options.variableForColumnExpr.String()),
			Assignment,
			Call(
				AppendFn,
				ast.NewIdent(options.variableForColumnExpr.String()),
				Call(
					SprintfFn,
					StringConstant(fmt.Sprintf(options.appendValueFormat, colName)).Expr(),
					Call(LengthFn, ast.NewIdent(options.variableForColumnValues.String())),
				),
			),
		),
	)
}

func scanBlockForFindOnce(stmts ...ast.Stmt) ast.Stmt {
	return If(
		Call(RowsNextFn),
		append(
			append(
				[]ast.Stmt{
					MakeCallWithErrChecking(
						"",
						Call(
							RowsErrFn,
						),
					),
				},
				stmts...,
			),
			&ast.IfStmt{
				Cond: Call(RowsNextFn),
				Body: Block(
					Return(
						ast.NewIdent("row"),
						ast.NewIdent(sqlSingletonViolationErrorName),
					),
				),
				Else: Return(
					ast.NewIdent("row"),
					Nil,
				),
			},
		)...,
	)
}

func scanBlockForFindAll(stmts ...ast.Stmt) ast.Stmt {
	return &ast.ForStmt{
		Cond: Call(RowsNextFn),
		Body: Block(
			append(
				append(
					[]ast.Stmt{
						MakeCallWithErrChecking(
							"",
							Call(
								RowsErrFn,
							),
						),
					},
					stmts...,
				),
				Assign(
					MakeVarNames("result"),
					Assignment,
					Call(
						AppendFn,
						ast.NewIdent("result"),
						ast.NewIdent("row"),
					),
				),
			)...,
		),
	}
}

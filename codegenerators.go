package dragonfly

import (
	"fmt"
	builders "github.com/iv-menshenin/go-ast"
	"go/ast"
	"go/token"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var (
	registeredGenerators = map[string]builders.CallFunctionDescriber{
		"now": builders.TimeNowFn,
	}
)

func addNewGenerator(name string, descr builders.CallFunctionDescriber) {
	registeredGenerators[name] = descr
}

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

//  args = append(args, filter.Id)
//  filters = append(filters, fmt.Sprintf("%s = %s", "id", "$"+strconv.Itoa(len(args))))
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
		// &ast.ExprStmt{X: &ast.BasicLit{Value: "/* opInline:makeScalarQueryOption */"}},
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
		// &ast.ExprStmt{X: &ast.BasicLit{Value: "/* opConstant:makeScalarQueryOption */"}},
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

// doFuncPicker searches for a suitable function by name and implements its calling code,
// taking into account the passed arguments
//
// returns nil if the function is not found
func doFuncPicker(funcName string, funcArgs ...string) (ast.Expr, bool) {
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
			return builders.Call(userDefinedFunction, args...), userDefinedFunction.MultipleReturnValues
		}
		// TODO move it to registeredGenerators
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
					MultipleReturnValues:        false,
				},
				builders.IntegerConstant(l).Expr(),
			), false
		}
	default:
		panic("not implemented")
	}
	return nil, false
}

// makeInputValueProcessor generates handler code for one cell of incoming data (one field)
//
//   Example:
//   // makeInputValueProcessor("type = $%d", Selector{values, Type}, "args", "fields")
//
//   args = append(args, values.Type)
//   fields = append(fields, fmt.Sprintf("type = $%d", len(args)))
//
// the `fields` variable are further used to build a sql query,
//
// and the `args` variable is used as a tuple of values for placeholders
func makeInputValueProcessor(
	sqlExpr string,
	goExpr ast.Expr,
	valueVarName, columnVarName string,
) []ast.Stmt {
	return []ast.Stmt{
		builders.Assign(
			builders.MakeVarNames(valueVarName),
			builders.Assignment,
			builders.Call(builders.AppendFn, ast.NewIdent(valueVarName), goExpr),
		),
		builders.Assign(
			builders.MakeVarNames(columnVarName),
			builders.Assignment,
			builders.Call(
				builders.AppendFn,
				ast.NewIdent(columnVarName),
				builders.Call(
					builders.SprintfFn,
					builders.StringConstant(sqlExpr).Expr(),
					builders.Call(builders.LengthFn, ast.NewIdent(columnVarName)),
				),
			),
		),
	}
}

// wrapFetchOnceForScanner generates a code to traverse once record received from the database
//  Example:
//  if rows.Next() {
//    if err = rows.Err(); err != nil {
//      return
//    }
//    // *** starts printing `stmts` from arguments
//    var row BaseAccountServiceRow
//    if err = rows.Scan(&row.Id, &row.AccountId, &row.ServiceId); err != nil {
//      return
//    }
//    // *** ends printing `stmts` from arguments
//    if rows.Next() {
//      return row, SingletonViolation
//    } else {
//      return row, nil
//    }
//  }
// thus wraps the stmts argument, which is useful code
func wrapFetchOnceForScanner(stmts ...ast.Stmt) ast.Stmt {
	return builders.If(
		builders.Call(builders.RowsNextFn),
		append(
			append(
				[]ast.Stmt{
					builders.MakeCallWithErrChecking("", builders.Call(builders.RowsErrFn)),
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

// wrapIteratorForScanner generates a code to traverse all records received from the database
//  Example:
//  for rows.Next() {
//    if err = rows.Err(); err != nil {
//      return
//    }
//    // *** starts printing `stmts` from arguments
//    var row BaseAccountServiceRow
//    if err = rows.Scan(&row.Id, &row.AccountId, &row.ServiceId); err != nil {
//      return
//    }
//    // *** ends printing `stmts` from arguments
//    result = append(result, row)
//  }
// thus wraps the stmts argument, which is useful code
func wrapIteratorForScanner(stmts ...ast.Stmt) ast.Stmt {
	return &ast.ForStmt{
		Cond: builders.Call(builders.RowsNextFn),
		Body: builders.Block(
			append(
				append(
					[]ast.Stmt{
						// if err = rows.Err(); err != nil {
						builders.MakeCallWithErrChecking("", builders.Call(builders.RowsErrFn)),
					},
					stmts...,
				),
				builders.Assign(
					// result = append(result, row)
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

var (
	// tagsPattern - full tag validation template including quotes
	tagsPattern = regexp.MustCompile("^`((\\s*[a-z]+\\s*:\\s*\"[^\"]*\"\\s*)*)`$")
	// tagsExtractor - a template for separating different tags from each other
	tagsExtractor = regexp.MustCompile("\\s*[a-z]+\\s*:\\s*\"[^\"]*\"\\s*")
	// tagPattern - a pattern for separating a tag name from values
	tagPattern = regexp.MustCompile("\\s*([a-z]+)\\s*:\\s*\"([^\"]*)\"\\s*")
)

// fieldTagToMap parses go tags and provides them as a map of string arrays
//
// Example:
//  fieldTagToMap('`sql:"field_name,omitempty"` json:"-"')
// Return:
//  map[string][]string{
//    "sql": {"field_name", "omitempty"},
//    "json": {"-"},
//  }
func fieldTagToMap(tag string) map[string][]string {
	var result = make(map[string][]string, 10)
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
	return result
}

// funcDeclUniqueName represents simple types in string format, has a very narrow purpose -
// used to form a unique function name taking into account the receiver parameter
func funcDeclUniqueName(f *ast.FuncDecl) string {
	if f.Name == nil {
		return ""
	}
	if f.Recv == nil || len(f.Recv.List) < 1 {
		return f.Name.Name
	}
	var identToStr func(e ast.Expr) string
	identToStr = func(e ast.Expr) string {
		switch v := e.(type) {
		case *ast.StarExpr:
			return ":" + identToStr(v.X)
		case *ast.Ident:
			return ":" + v.Name
		case *ast.SelectorExpr:
			return ":" + identToStr(v.X) + "." + identToStr(v.Sel)
		default:
			return ":0"
		}
	}
	return f.Name.Name + identToStr(f.Recv.List[0].Type)
}

// funcDeclsToMap grouping implementations based on function names.
// panics if different implementations with the same name are found
func funcDeclsToMap(functions []*ast.FuncDecl) map[string]*ast.FuncDecl {
	result := make(map[string]*ast.FuncDecl, len(functions))
	for i, f := range functions {
		funcName := funcDeclUniqueName(f)
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

var (
	// fncTemplate - the pattern matches the function call format:
	//  funcName(arg1;arg2)
	fncTemplate = regexp.MustCompile(`^(\w+)\(([^)]*)\)$`)
)

// makeValuePicker generates code to get field values taking into account data generation tags,
// if tags do not provide data generation, def value is used
//
// a boolean value indicates whether the generation was performed (true) or the default value was taken (false)
func makeValuePicker(sourceName string, tags []string, def ast.Expr) ([]ast.Stmt, ast.Expr, bool) {
	for _, tag := range tags {
		sub := fncTemplate.FindAllStringSubmatch(tag, -1)
		if len(sub) > 0 {
			funcName := sub[0][1]
			funcArgs := strings.Split(sub[0][2], ";")
			if expr, needCheck := doFuncPicker(funcName, funcArgs...); expr != nil {
				if needCheck {
					varName := "var" + sourceName
					return []ast.Stmt{
						builders.Var(
							builders.VariableType(varName, builders.EmptyInterface),
						),
						builders.IfInit(
							builders.Assign(builders.MakeVarNames(varName, "err"), builders.Assignment, expr),
							builders.NotEqual(ast.NewIdent("err"), builders.Nil),
							builders.ReturnEmpty(),
						),
					}, ast.NewIdent(varName), true
				}
				return nil, expr, true
			}
		}
	}
	return nil, def, false
}

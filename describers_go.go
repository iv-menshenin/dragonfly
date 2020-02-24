package dragonfly

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/code_builders"
	"go/ast"
	"go/token"
	"strings"
)

const (
	methodReceiverLit = "c"

	enumFunctionName  = "Enum"
	checkFunctionName = "Check"
)

type (
	fieldDescriber interface {
		getFile() []AstDataChain
		fieldTypeExpr() ast.Expr
	}
	simpleTypeDescriber struct {
		typeLit     string // type   time.[Time]
		typePrefix  string // prefix [time].Time
		packagePath string // package to be included, e.g. "net/http"
	}
	sliceTypeDescriber struct {
		simpleTypeDescriber
	}
	enumTypeDescriber struct {
		simpleTypeDescriber
		typeName string
		domain   *TypeSchema
	}
	mapTypeDescriber struct {
		simpleTypeDescriber
		typeName  string
		keyType   *ColumnSchemaRef
		valueType *ColumnSchemaRef
	}
	recordTypeDescriber struct {
		simpleTypeDescriber
		typeName string
		domain   *TypeSchema
	}
	jsonTypeDescriber struct {
		recordTypeDescriber
	}
	makeDescriber func(string, *DomainSchema) fieldDescriber
)

/*
  simpleTypeDescriber
*/

func makeSimpleDescriber(t, p, x string) makeDescriber {
	return func(string, *DomainSchema) fieldDescriber {
		return simpleTypeDescriber{t, p, x}
	}
}

func (c simpleTypeDescriber) getFile() []AstDataChain {
	return nil
}

func (c simpleTypeDescriber) fieldTypeExpr() ast.Expr {
	if c.typePrefix == "" {
		return ast.NewIdent(c.typeLit) // just type string
	} else {
		return builders.MakeSelectorExpression(c.typePrefix, c.typeLit) // like "package.type"
	}
}

/*
  sliceTypeDescriber
*/

func makeSliceDescriber(t, p, x string) makeDescriber {
	return func(string, *DomainSchema) fieldDescriber {
		return sliceTypeDescriber{
			simpleTypeDescriber{
				typeLit:     t,
				typePrefix:  p,
				packagePath: x,
			},
		}
	}
}

func (c sliceTypeDescriber) fieldTypeExpr() ast.Expr {
	return &ast.ArrayType{Elt: c.simpleTypeDescriber.fieldTypeExpr()}
}

/*
  enumTypeDescriber
*/

func makeEnumDescriberDirectly(typeName string, domain *TypeSchema) fieldDescriber {
	return enumTypeDescriber{
		simpleTypeDescriber: simpleTypeDescriber{typeLit: typeName},
		domain:              domain,
		typeName:            typeName,
	}
}

func (c enumTypeDescriber) getFile() []AstDataChain {
	var (
		mainTypeName     = ast.NewIdent(c.typeName)
		enumValues       = make([]ast.Expr, 0, len(c.domain.Enum))
		allowedValues    = make(map[string]*ast.ValueSpec, len(c.domain.Enum))
		objMethodArgSelf = []*ast.Field{
			{
				Names: []*ast.Ident{
					ast.NewIdent(methodReceiverLit),
				},
				Type: mainTypeName,
			},
		}
	)
	for _, entity := range c.domain.Enum {
		entityName := ast.NewIdent(c.typeName + makeExportedName(entity.Value))
		entityValue := builders.MakeBasicLiteralString(entity.Value)
		allowedValues[entityName.Name] = &ast.ValueSpec{
			Names:  []*ast.Ident{entityName},
			Type:   ast.NewIdent(c.typeName),
			Values: []ast.Expr{entityValue},
		}
		enumValues = append(enumValues, builders.MakeCallExpression(ast.NewIdent("string"), entityName))
	}
	returnTypeValueErrorExpr := builders.MakeReturn(
		builders.MakeCallExpression(
			ast.NewIdent("makeTypeValueError"),
			builders.MakeCallExpression(
				builders.MakeSelectorExpression("fmt", "Sprintf"),
				builders.MakeBasicLiteralString("%T"),
				ast.NewIdent(methodReceiverLit),
			),
			builders.MakeCallExpression(ast.NewIdent("string"), ast.NewIdent(methodReceiverLit)),
		),
	)
	rangeBody := builders.MakeBlockStmt(
		&ast.IfStmt{
			Cond: builders.MakeCallExpression(
				builders.MakeSelectorExpression("strings", "EqualFold"),
				ast.NewIdent("s"),
				builders.MakeCallExpression(ast.NewIdent("string"), ast.NewIdent(methodReceiverLit)),
			),
			Body: builders.MakeBlockStmt(builders.MakeReturn(ast.NewIdent("nil"))),
		},
	)
	main := AstDataChain{
		Types: map[string]*ast.TypeSpec{
			mainTypeName.Name: {
				Name: mainTypeName,
				Type: ast.NewIdent("string"),
			},
		},
		Constants: allowedValues,
		Implementations: funcDeclsToMap(
			[]*ast.FuncDecl{
				{
					Recv: &ast.FieldList{
						List: objMethodArgSelf,
					},
					Name: ast.NewIdent(enumFunctionName),
					Type: &ast.FuncType{
						Results: &ast.FieldList{
							List: []*ast.Field{
								{
									Type: builders.MakeArrayType(ast.NewIdent("string")),
								},
							},
						},
					},
					Body: builders.MakeBlockStmt(
						builders.MakeReturn(
							&ast.CompositeLit{
								Type: builders.MakeArrayType(ast.NewIdent("string")),
								Elts: enumValues,
							},
						),
					),
				},
				{
					Recv: &ast.FieldList{
						List: objMethodArgSelf,
					},
					Name: ast.NewIdent(checkFunctionName),
					Type: &ast.FuncType{
						Params: &ast.FieldList{},
						Results: &ast.FieldList{
							List: []*ast.Field{
								{
									Type: ast.NewIdent("error"),
								},
							},
						},
					},
					Body: &ast.BlockStmt{
						List: []ast.Stmt{
							&ast.RangeStmt{
								Key:   ast.NewIdent("_"),
								Value: ast.NewIdent("s"),
								Tok:   token.DEFINE,
								X: &ast.CallExpr{
									Fun: builders.MakeSelectorExpression(methodReceiverLit, enumFunctionName),
								},
								Body: rangeBody,
							},
							returnTypeValueErrorExpr,
						},
					},
				},
			},
		),
	}
	return append(c.simpleTypeDescriber.getFile(), main)
}

/*
	makeMapDescriberDirectly
*/

func makeMapDescriberDirectly(typeName string, domain *TypeSchema) fieldDescriber {
	return mapTypeDescriber{
		simpleTypeDescriber: simpleTypeDescriber{typeLit: typeName},
		keyType:             domain.KeyType,
		valueType:           domain.ValueType,
		typeName:            typeName,
	}
}

func (c mapTypeDescriber) getFile() []AstDataChain {
	return []AstDataChain{
		{
			Types: map[string]*ast.TypeSpec{
				ast.NewIdent(c.typeName).Name: {
					Name: ast.NewIdent(c.typeName),
					Type: &ast.MapType{
						Key:   c.keyType.Value.describeGO(c.keyType.Value.Type).fieldTypeExpr(),
						Value: c.valueType.Value.describeGO(c.keyType.Value.Type).fieldTypeExpr(),
					},
				},
			},
		},
	}
}

/*
	makeRecordDescriberDirectly
*/

func makeRecordDescriberDirectly(typeName string, domain *TypeSchema) fieldDescriber {
	return recordTypeDescriber{
		simpleTypeDescriber: simpleTypeDescriber{typeLit: typeName},
		domain:              domain,
		typeName:            typeName,
	}
}

func (c recordTypeDescriber) getFile() []AstDataChain {
	var (
		enumValues   = make([]ast.Expr, 0, len(c.domain.Enum))
		formatLiters = make([]string, 0, len(c.domain.Fields))
		objFields    = make([]*ast.Field, 0, len(c.domain.Fields))
		formatArgs   = make([]ast.Expr, 0, len(c.domain.Fields))
	)
	for _, entity := range c.domain.Enum {
		enumValues = append(enumValues, builders.MakeBasicLiteralString(entity.Value))
	}
	for _, f := range c.domain.Fields {
		intDesc := f.Value.describeGO()
		objFields = append(objFields, &ast.Field{
			Names: []*ast.Ident{ast.NewIdent(makeExportedName(f.Value.Name))},
			Type:  intDesc.fieldTypeExpr(),
		})
		if fmtLiter, ok := formatTypes[f.Value.Schema.Value.Type]; ok {
			formatLiters = append(formatLiters, fmtLiter)
		} else {
			panic(fmt.Sprintf("we cannot Scan field '%s' in struct '%s' due to type '%s'", f.Value.Name, c.typeName, f.Value.Schema.Value.Type))
		}
		formatArgs = append(formatArgs, &ast.UnaryExpr{
			Op: token.AND,
			X:  builders.MakeSelectorExpression("c", makeExportedName(f.Value.Name)),
		})
	}
	formatArgs = append([]ast.Expr{
		&ast.CallExpr{
			Fun: builders.MakeSelectorExpression("bytes", "NewReader"),
			Args: []ast.Expr{
				&ast.TypeAssertExpr{
					X:    ast.NewIdent("value"),
					Type: builders.MakeArrayType(ast.NewIdent("uint8")),
				},
			},
		}, builders.MakeBasicLiteralString("(" + strings.Join(formatLiters, ",") + ")"),
	}, formatArgs...)
	main := AstDataChain{
		Types: map[string]*ast.TypeSpec{
			ast.NewIdent(c.typeName).Name: {
				Name: ast.NewIdent(c.typeName),
				Type: &ast.StructType{
					Fields: &ast.FieldList{List: objFields},
				},
			},
		},
		Constants: nil,
		Implementations: funcDeclsToMap(
			[]*ast.FuncDecl{
				{
					Recv: &ast.FieldList{
						List: []*ast.Field{
							{
								Names: []*ast.Ident{
									{
										Name: "c",
										Obj: &ast.Object{
											Kind: ast.Var,
											Name: "c",
										},
									},
								},
								Type: &ast.StarExpr{
									X: &ast.Ident{
										Name: c.typeName,
									},
								},
							},
						},
					},
					Name: &ast.Ident{
						Name: "Scan",
					},
					Type: &ast.FuncType{
						Params: &ast.FieldList{
							List: []*ast.Field{
								{
									Names: []*ast.Ident{
										{
											Name: "value",
											Obj: &ast.Object{
												Kind: ast.Var,
												Name: "value",
											},
										},
									},
									Type: &ast.InterfaceType{
										Methods: &ast.FieldList{},
									},
								},
							},
						},
						Results: &ast.FieldList{
							List: []*ast.Field{
								{
									Type: &ast.Ident{
										Name: "error",
									},
								},
							},
						},
					},
					Body: &ast.BlockStmt{
						List: []ast.Stmt{
							&ast.IfStmt{
								Cond: &ast.BinaryExpr{
									X: &ast.Ident{
										Name: "value",
									},
									Op: token.EQL,
									Y: &ast.Ident{
										Name: "nil",
									},
								},
								Body: &ast.BlockStmt{
									List: []ast.Stmt{
										&ast.ReturnStmt{
											Results: []ast.Expr{
												&ast.Ident{
													Name: "nil",
												},
											},
										},
									},
								},
							},
							&ast.AssignStmt{
								Lhs: []ast.Expr{
									&ast.Ident{
										Name: "_",
										Obj: &ast.Object{
											Kind: ast.Var,
											Name: "_",
										},
									},
									&ast.Ident{
										Name: "err",
										Obj: &ast.Object{
											Kind: ast.Var,
											Name: "err",
										},
									},
								},
								Tok: token.DEFINE,
								Rhs: []ast.Expr{
									&ast.CallExpr{
										Fun: &ast.SelectorExpr{
											X: &ast.Ident{
												Name: "fmt",
											},
											Sel: &ast.Ident{
												Name: "Fscanf",
											},
										},
										Args: formatArgs,
									},
								},
							},
							&ast.ReturnStmt{
								Results: []ast.Expr{
									&ast.Ident{
										Name: "err",
									},
								},
							},
						},
					},
				},
			},
		),
	}
	return append(c.simpleTypeDescriber.getFile(), main)
}

/*
	makeJsonDescriberDirectly
*/

func makeJsonDescriberDirectly(typeName string, domain *TypeSchema) fieldDescriber {
	return jsonTypeDescriber{
		recordTypeDescriber: recordTypeDescriber{
			simpleTypeDescriber: simpleTypeDescriber{typeLit: typeName},
			domain:              domain,
			typeName:            typeName,
		},
	}
}

func (c jsonTypeDescriber) getFile() []AstDataChain {
	var (
		enumValues = make([]ast.Expr, 0, len(c.domain.Enum))
		objFields  = make([]*ast.Field, 0, len(c.domain.Fields))
	)
	for _, entity := range c.domain.Enum {
		enumValues = append(enumValues, builders.MakeBasicLiteralString(entity.Value))
	}
	for _, f := range c.domain.Fields {
		intDesc := f.Value.describeGO()
		objFields = append(objFields, &ast.Field{
			Names: []*ast.Ident{ast.NewIdent(f.Value.Name)},
			Type:  intDesc.fieldTypeExpr(),
		})
	}
	main := AstDataChain{
		Types: map[string]*ast.TypeSpec{
			ast.NewIdent(c.typeName).Name: {
				Name: ast.NewIdent(c.typeName),
				Type: &ast.StructType{
					Fields: &ast.FieldList{List: objFields},
				},
			},
		},
		Constants: nil,
		Implementations: funcDeclsToMap(
			[]*ast.FuncDecl{
				{
					Recv: &ast.FieldList{
						List: []*ast.Field{
							{
								Names: []*ast.Ident{
									{
										Name: "c",
										Obj: &ast.Object{
											Kind: ast.Var,
											Name: "c",
										},
									},
								},
								Type: &ast.StarExpr{
									X: &ast.Ident{
										Name: c.typeName,
									},
								},
							},
						},
					},
					Name: &ast.Ident{
						Name: "Scan",
					},
					Type: &ast.FuncType{
						Params: &ast.FieldList{
							List: []*ast.Field{
								{
									Names: []*ast.Ident{
										{
											Name: "value",
											Obj: &ast.Object{
												Kind: ast.Var,
												Name: "value",
											},
										},
									},
									Type: &ast.InterfaceType{
										Methods:    &ast.FieldList{},
										Incomplete: false,
									},
								},
							},
						},
						Results: &ast.FieldList{
							List: []*ast.Field{
								{
									Type: &ast.Ident{
										Name: "error",
									},
								},
							},
						},
					},
					Body: &ast.BlockStmt{
						List: []ast.Stmt{
							&ast.IfStmt{
								Cond: &ast.BinaryExpr{
									X: &ast.Ident{
										Name: "value",
									},
									Op: token.EQL,
									Y: &ast.Ident{
										Name: "nil",
									},
								},
								Body: &ast.BlockStmt{
									List: []ast.Stmt{
										&ast.ReturnStmt{
											Results: []ast.Expr{
												&ast.Ident{
													Name: "nil",
												},
											},
										},
									},
								},
							},
							&ast.ReturnStmt{
								Results: []ast.Expr{
									&ast.CallExpr{
										Fun: &ast.SelectorExpr{
											X: &ast.Ident{
												Name: "json",
											},
											Sel: &ast.Ident{
												Name: "Unmarshal",
											},
										},
										Args: []ast.Expr{
											&ast.TypeAssertExpr{
												X: &ast.Ident{
													Name: "value",
												},
												Type: &ast.ArrayType{
													Elt: &ast.Ident{
														Name: "uint8",
													},
												},
											},
											&ast.Ident{
												Name: "c",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		),
	}
	return append(c.simpleTypeDescriber.getFile(), main)
}

var (
	knownTypes = map[string]makeDescriber{
		"smallserial": makeSimpleDescriber("int16", "", ""),
		"serial":      makeSimpleDescriber("int32", "", ""),
		"bigserial":   makeSimpleDescriber("int64", "", ""),
		"bigint":      makeSimpleDescriber("int64", "", ""),
		"int2":        makeSimpleDescriber("int16", "", ""),
		"int4":        makeSimpleDescriber("int32", "", ""),
		"int8":        makeSimpleDescriber("int64", "", ""),
		// "integer": not supported, use types with explicit size, e.g. int8 or int16
		"varchar":     makeSimpleDescriber("string", "", ""),
		"character":   makeSimpleDescriber("string", "", ""),
		"char":        makeSimpleDescriber("string", "", ""),
		"bit":         makeSliceDescriber("byte", "", ""),
		"bool":        makeSimpleDescriber("bool", "", ""),
		"boolean":     makeSimpleDescriber("bool", "", ""),
		"date":        makeSimpleDescriber("Time", "time", "\"time\""),
		"timestamp":   makeSimpleDescriber("Time", "time", "\"time\""),
		"timestamptz": makeSimpleDescriber("Time", "time", "\"time\""),
		"timetz":      makeSimpleDescriber("Time", "time", "\"time\""),
		// "float": not supported, use types with explicit size, e.g. float8 or float16
		"float4": makeSimpleDescriber("float32", "", ""),
		"float8": makeSimpleDescriber("float64", "", ""),
		// ?! "float32":  makeSimpleDescriber("float64", "", ""),
		"smallint":         makeSimpleDescriber("int16", "", ""),
		"real":             makeSimpleDescriber("float32", "", ""),
		"numeric":          makeSimpleDescriber("float32", "", ""),
		"decimal":          makeSimpleDescriber("float32", "", ""),
		"double precision": makeSimpleDescriber("float64", "", ""),
	}
	formatTypes = map[string]string{
		"smallserial": "%d",
		"serial":      "%d",
		"bigserial":   "%d",
		"bigint":      "%d",
		"int2":        "%d",
		"int4":        "%d",
		"int8":        "%d",
		"int16":       "%d",
		"varchar":     "%s",
		"character":   "%s",
		"char":        "%s",
		// "bit": not supported
		"bool":    "%t",
		"boolean": "%t",
		// "date":        not supported
		// "timestamp":   not supported
		// "timestamptz": not supported
		// "timetz":      not supported
		"float4":           "%f",
		"float8":           "%f",
		"double precision": "%f",
		"smallint":         "%d",
		"real":             "%f",
		"numeric":          "%f",
		"decimal":          "%f",
		// "json":   not supported
		// "enum":   not supported
		// "map":    not supported
		// "record": not supported
	}
	typeAliases = [][]string{
		{"bigint", "int8"},
		{"bigserial", "serial8"},
		{"double precision", "float8"},
		{"integer", "int", "int4"},
		{"numeric", "decimal"},
		{"float4", "real"},
		{"smallint", "int2"},
		{"smallserial", "serial2"},
		{"serial", "serial4"},
		{"time", "timetz"},
		{"timestamp", "timestamptz"},
		{"character varying", "varchar"},
		{"character", "char"},
		{"boolean", "bool"},
		{"bit varying", "varbit"},
	}
	lengthLimited = []string{"numeric", "decimal", "character varying", "varchar", "character", "char", "bit varying", "varbit"}
)

func goTypeParametersBySqlType(typeName string, c *DomainSchema) fieldDescriber {
	if makeFn, ok := knownTypes[strings.ToLower(c.Type)]; ok {
		return makeFn(typeName, c)
	}
	if t := strings.Split(c.Type, "."); len(t) == 2 {
		return simpleTypeDescriber{
			typeLit:     makeExportedName(c.Type),
			typePrefix:  "",
			packagePath: "",
		}
	}
	panic(fmt.Sprintf("unknown field type '%s'", c.Type))
}

func isMatchedTypes(a, b TypeBase) bool {
	if !strings.EqualFold(a.Type, b.Type) {
		var typeMatched = false
		for _, aliases := range typeAliases {
			if iArrayContains(aliases, a.Type) {
				if iArrayContains(aliases, b.Type) {
					typeMatched = true
					break
				} else {
					return false
				}
			}
		}
		if !typeMatched {
			return false
		}
	}
	if iArrayContains(lengthLimited, a.Type) {
		if a.Length != nil {
			if b.Length == nil || *b.Length != *a.Length {
				return false
			}
		} else if b.Length != nil {
			return false
		}
		if a.Precision != nil {
			if b.Precision == nil || *b.Precision != *a.Precision {
				return false
			}
		} else if b.Precision != nil {
			return false
		}
	}
	return true
}

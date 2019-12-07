package dragonfly

import (
	"fmt"
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
		getFile() *ast.File
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
		domain   *DomainSchema
	}
	recordTypeDescriber struct {
		simpleTypeDescriber
		typeName string
		domain   *DomainSchema
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

func (c simpleTypeDescriber) getFile() *ast.File {
	if c.packagePath != "" {
		return &ast.File{
			Decls: []ast.Decl{
				&ast.GenDecl{
					Tok: token.IMPORT,
					Specs: []ast.Spec{
						&ast.ImportSpec{
							Path: &ast.BasicLit{Value: c.packagePath},
						},
					},
				},
			},
		}
	}
	return nil
}

func (c simpleTypeDescriber) fieldTypeExpr() ast.Expr {
	if c.typePrefix == "" {
		return makeTypeIdent(c.typeLit) // just type string
	} else {
		return makeTypeSelector(c.typePrefix, c.typeLit) // like "package.type"
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

func makeEnumDescriberDirectly(typeName string, domain *DomainSchema) fieldDescriber {
	return enumTypeDescriber{
		simpleTypeDescriber: simpleTypeDescriber{typeLit: typeName},
		domain:              domain,
		typeName:            typeName,
	}
}

// TODO temporary
var (
	alreadyDeclared = make(map[string]bool)
)

func (c enumTypeDescriber) getFile() *ast.File {
	if _, ok := alreadyDeclared[c.typeName]; ok {
		return nil
	}
	var (
		f          ast.File
		enumValues = make([]ast.Expr, 0, len(c.domain.Enum))
	)
	allowedValues := make([]ast.Spec, 0, len(c.domain.Enum))
	for _, entity := range c.domain.Enum {
		entityName := makeName(c.typeName + makeExportedName(entity.Value))
		entityValue := makeBasicLiteralString(entity.Value)
		allowedValues = append(allowedValues, &ast.ValueSpec{
			Names:  []*ast.Ident{entityName},
			Type:   makeName(c.typeName),
			Values: []ast.Expr{entityValue},
		})
		enumValues = append(enumValues, makeCall(makeName("string"), entityName))
	}
	objMethodArgSelf := []*ast.Field{
		{
			Names: []*ast.Ident{
				makeName(methodReceiverLit),
			},
			Type: makeName(c.typeName),
		},
	}
	returnTypeValueErrorExpr := makeReturn(
		makeCall(
			makeName("makeTypeValueError"),
			makeCall(
				makeTypeSelector("fmt", "Sprintf"),
				makeBasicLiteralString("%T"),
				makeName(methodReceiverLit),
			),
			makeCall(makeName("string"), makeName(methodReceiverLit)),
		),
	)
	rangeBody := makeBlock(
		&ast.IfStmt{
			Cond: makeCall(
				makeTypeSelector("strings", "EqualFold"),
				makeName("s"),
				makeCall(makeName("string"), makeName(methodReceiverLit)),
			),
			Body: makeBlock(makeReturn(makeName("nil"))),
		},
	)
	f.Name = makeName("generated")
	f.Decls = []ast.Decl{
		&ast.GenDecl{
			Tok: token.TYPE,
			Specs: []ast.Spec{
				&ast.TypeSpec{
					Name: makeName(c.typeName),
					Type: makeTypeIdent("string"),
				},
			},
		},
		&ast.GenDecl{
			Tok:   token.CONST,
			Specs: allowedValues,
		},
		&ast.FuncDecl{
			Recv: &ast.FieldList{
				List: objMethodArgSelf,
			},
			Name: makeName(enumFunctionName),
			Type: &ast.FuncType{
				Results: &ast.FieldList{
					List: []*ast.Field{
						{
							Type: makeTypeArray(makeName("string")),
						},
					},
				},
			},
			Body: makeBlock(
				makeReturn(
					&ast.CompositeLit{
						Type: makeTypeArray(makeName("string")),
						Elts: enumValues,
					},
				),
			),
		},
		&ast.FuncDecl{
			Recv: &ast.FieldList{
				List: objMethodArgSelf,
			},
			Name: makeName(checkFunctionName),
			Type: &ast.FuncType{
				Params: &ast.FieldList{},
				Results: &ast.FieldList{
					List: []*ast.Field{
						{
							Type: makeName("error"),
						},
					},
				},
			},
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					&ast.RangeStmt{
						Key:   makeName("_"),
						Value: makeName("s"),
						Tok:   token.DEFINE,
						X: &ast.CallExpr{
							Fun: makeTypeSelector(methodReceiverLit, enumFunctionName),
						},
						Body: rangeBody,
					},
					returnTypeValueErrorExpr,
				},
			},
		},
	}
	mergeCodeBase(&f, c.simpleTypeDescriber.getFile())
	alreadyDeclared[c.typeName] = true
	return &f
}

/*
	makeRecordDescriberDirectly
*/

func makeRecordDescriberDirectly(typeName string, domain *DomainSchema) fieldDescriber {
	return recordTypeDescriber{
		simpleTypeDescriber: simpleTypeDescriber{typeLit: typeName},
		domain:              domain,
		typeName:            typeName,
	}
}

func (c recordTypeDescriber) getFile() *ast.File {
	if _, ok := alreadyDeclared[c.typeName]; ok {
		return nil
	}
	var (
		f          ast.File
		enumValues = make([]ast.Expr, 0, len(c.domain.Enum))
	)
	for _, entity := range c.domain.Enum {
		enumValues = append(enumValues, makeBasicLiteralString(entity.Value))
	}
	formatLiters := make([]string, 0, len(c.domain.Fields))
	objFields := make([]*ast.Field, 0, len(c.domain.Fields))
	formatArgs := make([]ast.Expr, 0, len(c.domain.Fields))
	for _, f := range c.domain.Fields {
		intDesc := f.describeGO()
		objFields = append(objFields, &ast.Field{
			Names: []*ast.Ident{makeName(f.Name)},
			Type:  intDesc.fieldTypeExpr(),
		})
		if fmtLiter, ok := formatTypes[f.Schema.Value.Type]; ok {
			formatLiters = append(formatLiters, fmtLiter)
		} else {
			panic(fmt.Sprintf("we cannot Scan field '%s' in struct '%s' due to type '%s'", f.Name, c.typeName, f.Schema.Value.Type))
		}
		formatArgs = append(formatArgs, &ast.UnaryExpr{
			Op: token.AND,
			X:  makeTypeSelector("c", f.Name),
		})
	}
	formatArgs = append([]ast.Expr{
		&ast.CallExpr{
			Fun: makeTypeSelector("bytes", "NewReader"),
			Args: []ast.Expr{
				&ast.TypeAssertExpr{
					X:    makeName("value"),
					Type: makeTypeArray(makeName("uint8")),
				},
			},
		}, makeBasicLiteralString("(" + strings.Join(formatLiters, ",") + ")"),
	}, formatArgs...)
	f.Name = makeName("generated")
	f.Decls = []ast.Decl{
		&ast.GenDecl{
			Tok: token.IMPORT,
			Specs: []ast.Spec{
				&ast.ImportSpec{
					Path: &ast.BasicLit{
						Kind:  token.STRING,
						Value: "\"bytes\"",
					},
				},
			},
		},
		&ast.GenDecl{
			Tok: token.TYPE,
			Specs: []ast.Spec{
				&ast.TypeSpec{
					Name: makeName(c.typeName),
					Type: &ast.StructType{
						Fields: &ast.FieldList{List: objFields},
					},
				},
			},
		},
		&ast.FuncDecl{
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
	}
	mergeCodeBase(&f, c.simpleTypeDescriber.getFile())
	alreadyDeclared[c.typeName] = true
	return &f
}

/*
	makeJsonDescriberDirectly
*/

func makeJsonDescriberDirectly(typeName string, domain *DomainSchema) fieldDescriber {
	return jsonTypeDescriber{
		recordTypeDescriber: recordTypeDescriber{
			simpleTypeDescriber: simpleTypeDescriber{typeLit: typeName},
			domain:              domain,
			typeName:            typeName,
		},
	}
}

func (c jsonTypeDescriber) getFile() *ast.File {
	if _, ok := alreadyDeclared[c.typeName]; ok {
		return nil
	}
	var (
		f          ast.File
		enumValues = make([]ast.Expr, 0, len(c.domain.Enum))
	)
	for _, entity := range c.domain.Enum {
		enumValues = append(enumValues, makeBasicLiteralString(entity.Value))
	}
	objFields := make([]*ast.Field, 0, len(c.domain.Fields))
	for _, f := range c.domain.Fields {
		intDesc := f.describeGO()
		objFields = append(objFields, &ast.Field{
			Names: []*ast.Ident{makeName(f.Name)},
			Type:  intDesc.fieldTypeExpr(),
		})
	}
	f.Name = makeName("generated")
	f.Decls = []ast.Decl{
		&ast.GenDecl{
			Tok: token.IMPORT,
			Specs: []ast.Spec{
				&ast.ImportSpec{
					Path: &ast.BasicLit{
						Kind:  token.STRING,
						Value: "\"encoding/json\"",
					},
				},
			},
		},
		&ast.GenDecl{
			Tok: token.TYPE,
			Specs: []ast.Spec{
				&ast.TypeSpec{
					Name: makeName(c.typeName),
					Type: &ast.StructType{
						Fields: &ast.FieldList{List: objFields},
					},
				},
			},
		},
		&ast.FuncDecl{
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
	}
	mergeCodeBase(&f, c.simpleTypeDescriber.getFile())
	alreadyDeclared[c.typeName] = true
	return &f
}

var (
	knownTypes = map[string]makeDescriber{
		"smallserial": makeSimpleDescriber("int16", "", ""),
		"serial":      makeSimpleDescriber("int32", "", ""),
		"bigserial":   makeSimpleDescriber("int64", "", ""),
		"bigint":      makeSimpleDescriber("int64", "", ""),
		"int4":        makeSimpleDescriber("int16", "", ""),
		"int8":        makeSimpleDescriber("int32", "", ""),
		"int16":       makeSimpleDescriber("int64", "", ""),
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
		"float4":  makeSimpleDescriber("float32", "", ""),
		"float8":  makeSimpleDescriber("float32", "", ""),
		"float16": makeSimpleDescriber("float64", "", ""),
		// ?! "float32":  makeSimpleDescriber("float64", "", ""),
		"smallint": makeSimpleDescriber("int16", "", ""),
		"real":     makeSimpleDescriber("float32", "", ""),
		"numeric":  makeSimpleDescriber("float32", "", ""),
		"decimal":  makeSimpleDescriber("float32", "", ""),
		// ------------------ TODO
		"json":   makeJsonDescriberDirectly,
		"enum":   makeEnumDescriberDirectly,
		"map":    makeSimpleDescriber("map[string]string", "", ""),
		"record": makeRecordDescriberDirectly,
	}
	formatTypes = map[string]string{
		"smallserial": "%d",
		"serial":      "%d",
		"bigserial":   "%d",
		"bigint":      "%d",
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
		"float4":   "%f",
		"float8":   "%f",
		"float16":  "%f",
		"float32":  "%f",
		"smallint": "%d",
		"real":     "%f",
		"numeric":  "%f",
		"decimal":  "%f",
		// "json":   not supported
		// "enum":   not supported
		// "map":    not supported
		// "record": not supported
	}
)

func goTypeParametersBySqlType(typeName string, c *DomainSchema) fieldDescriber {
	if makeFn, ok := knownTypes[strings.ToLower(c.Type)]; ok {
		return makeFn(typeName, c)
	}
	panic(fmt.Sprintf("unknown field type '%s'", c.Type))
}

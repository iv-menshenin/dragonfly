package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
)

const (
	enumFunctionName = "Enum"
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
	for _, entity := range c.domain.Enum {
		enumValues = append(enumValues, makeBasicLiteralString(entity.Value))
	}
	f.Name = makeName("generated")
	f.Decls = []ast.Decl{
		&ast.GenDecl{
			Tok: token.TYPE,
			Specs: []ast.Spec{
				&ast.TypeSpec{
					Name: &ast.Ident{
						Name: c.typeName,
						Obj: &ast.Object{
							Kind: ast.Typ,
							Name: c.typeName,
						},
					},
					Type: makeTypeIdent("string"),
				},
			},
		},
		&ast.FuncDecl{
			Recv: &ast.FieldList{
				List: []*ast.Field{
					{
						/*
							Obj: &ast.Object {
									Kind: var
									Name: "c"
									Decl: *(obj @ 59)
								}
						*/
						Names: []*ast.Ident{
							makeName("c"),
						},
						Type: makeName(c.typeName),
					},
				},
			},
			Name: makeName(enumFunctionName),
			Type: &ast.FuncType{
				Params: &ast.FieldList{}, // TODO maybe nil?
				Results: &ast.FieldList{
					List: []*ast.Field{
						{
							Type: makeTypeArray(makeName("string")),
						},
					},
				},
			},
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					&ast.ReturnStmt{
						Results: []ast.Expr{
							&ast.CompositeLit{
								Type: &ast.ArrayType{
									Elt: makeName("string"),
								},
								Elts: enumValues,
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
		"smallserial": makeSimpleDescriber("int", "", ""),
		"serial":      makeSimpleDescriber("int32", "", ""),
		"bigserial":   makeSimpleDescriber("int64", "", ""),
		"bigint":      makeSimpleDescriber("int64", "", ""),
		"int4":        makeSimpleDescriber("int16", "", ""),
		"int8":        makeSimpleDescriber("int32", "", ""),
		"int16":       makeSimpleDescriber("int64", "", ""),
		// "integer":     makeSimpleDescriber("int64", "", ""),
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
		"float":       makeSimpleDescriber("float64", "", ""),
		"float8":      makeSimpleDescriber("float64", "", ""),
		"float16":     makeSimpleDescriber("float64", "", ""),
		"float32":     makeSimpleDescriber("float64", "", ""),
		"smallint":    makeSimpleDescriber("int", "", ""),
		"real":        makeSimpleDescriber("float64", "", ""),
		"numeric":     makeSimpleDescriber("float64", "", ""),
		"decimal":     makeSimpleDescriber("float64", "", ""),
		// ------------------ TODO
		"json":   makeSimpleDescriber("json", "", ""),
		"enum":   makeEnumDescriberDirectly,
		"map":    makeSimpleDescriber("map[string]string", "", ""),
		"record": makeSimpleDescriber("map[string]string", "", ""),
	}
)

func goTypeParametersBySqlType(typeName string, c *DomainSchema) fieldDescriber {
	if makeFn, ok := knownTypes[strings.ToLower(c.Type)]; ok {
		return makeFn(typeName, c)
	}
	panic(fmt.Sprintf("unknown field type '%s'", c.Type))
}

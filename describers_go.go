package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
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
		domain *DomainSchema
	}
	makeDescriber func(schema *DomainSchema) fieldDescriber
)

/*
  simpleTypeDescriber
*/

func makeSimpleDescriber(t, p, x string) makeDescriber {
	return func(*DomainSchema) fieldDescriber {
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
	return func(*DomainSchema) fieldDescriber {
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

func makeEnumDescriberDirectly(domain *DomainSchema) fieldDescriber {
	return enumTypeDescriber{
		simpleTypeDescriber: simpleTypeDescriber{typeLit: "string"},
		domain:              domain,
	}
}

func (c enumTypeDescriber) getFile() *ast.File {
	return c.simpleTypeDescriber.getFile()
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

func goTypeParametersBySqlType(c *DomainSchema) fieldDescriber {
	if makeFn, ok := knownTypes[strings.ToLower(c.Type)]; ok {
		return makeFn(c)
	}
	panic(fmt.Sprintf("unknown field type '%s'", c.Type))
}

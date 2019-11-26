package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"reflect"
	"strings"
)

func makeTypeString() ast.Expr {
	return &ast.BasicLit{
		Kind:  token.STRING,
		Value: "string",
	}
}

func makeBasicLiteralString(s string) ast.Expr {
	return &ast.BasicLit{
		Kind:  token.STRING,
		Value: fmt.Sprintf("\"%s\"", s),
	}
}

func makeTypeIdent(structType string) ast.Expr {
	return &ast.Ident{
		Name: structType,
	}
}

func makeTypeSelector(pack, name string) ast.Expr {
	return &ast.SelectorExpr{
		X: makeTypeIdent(pack),
		Sel: &ast.Ident{
			Name: name,
		},
	}
}

func makeTypeStar(expr ast.Expr) ast.Expr {
	return &ast.StarExpr{
		Star: 0,
		X:    expr,
	}
}

func makeTypeArray(expr ast.Expr) ast.Expr {
	return &ast.ArrayType{
		Elt: expr,
	}
}

func makeName(name string) *ast.Ident {
	return &ast.Ident{Name: name}
}

func makeComment(comment []string) *ast.CommentGroup {
	if len(comment) == 0 {
		return nil
	}
	return &ast.CommentGroup{
		List: []*ast.Comment{
			{
				Text: " /* " + strings.Join(comment, "\n") + " */",
			},
		},
	}
}

func makeTagsForField(tags map[string][]string) *ast.BasicLit {
	if len(tags) == 0 {
		return nil
	}
	arrTags := make([]string, 0, len(tags))
	for key, val := range tags {
		arrTags = append(arrTags, fmt.Sprintf("%s:\"%s\"", key, strings.Join(val, ",")))
	}
	return &ast.BasicLit{
		ValuePos: 0,
		Kind:     0,
		Value:    "`" + strings.Join(arrTags, " ") + "`",
	}
}

func insertTypeSpec(w *ast.File, newType ast.TypeSpec) {
	var genDecls *ast.Decl
	for i, dec := range w.Decls {
		if t, ok := dec.(*ast.GenDecl); ok {
			if t.Tok != token.TYPE {
				continue
			}
			if genDecls == nil {
				genDecls = &w.Decls[i]
			}
			for _, spec := range t.Specs {
				if s, ok := spec.(*ast.TypeSpec); ok {
					if s.Name.Name == newType.Name.Name {
						if !reflect.DeepEqual(s.Type, newType.Type) {
							panic(fmt.Sprintf("%s type is already declared", newType.Name.Name))
						} else {
							return
						}
					}
				}
			}
		}
	}
	if genDecls == nil {
		w.Decls = append(w.Decls, &ast.GenDecl{
			Tok:   token.TYPE,
			Specs: []ast.Spec{&newType},
		})
	} else {
		(*genDecls).(*ast.GenDecl).Specs = append((*genDecls).(*ast.GenDecl).Specs, &newType)
	}
}

func insertNewStructure(w *ast.File, name string, fields []*ast.Field, comments []string) {
	var newType = ast.TypeSpec{
		Doc:  nil,
		Name: makeName(name),
		Type: &ast.StructType{
			Fields:     &ast.FieldList{List: fields},
			Incomplete: false,
		},
		Comment: makeComment(comments),
	}
	insertTypeSpec(w, newType)
}
func isImportSpec(decl ast.Decl, callback func(spec *ast.ImportSpec)) bool {
	if gen, ok := decl.(*ast.GenDecl); ok {
		if gen.Tok == token.IMPORT {
			if callback != nil {
				for _, spec := range gen.Specs {
					if imp, ok := spec.(*ast.ImportSpec); ok {
						callback(imp)
					}
				}
			}
			return true
		}
	}
	return false
}

func addImport(w *ast.File, imp *ast.ImportSpec) {
	isImportPathExists := func(in *ast.GenDecl, what ast.Spec) bool {
		if p, ok := what.(*ast.ImportSpec); ok {
			path := p.Path.Value
			for _, imp := range in.Specs {
				if p, ok := imp.(*ast.ImportSpec); ok {
					if p.Path != nil && p.Path.Value == path {
						return true
					}
				}
			}
		}
		return false
	}
	getIdForImport := func() int {
		for i, decl := range w.Decls {
			if isImportSpec(decl, nil) {
				return i
			}
		}
		return -1
	}
	importInd := getIdForImport()
	if importInd < 0 {
		importInd = 0
		newDecls := make([]ast.Decl, 1, len(w.Decls)+1)
		newDecls[0] = &ast.GenDecl{
			Tok:   token.IMPORT,
			Specs: nil,
		}
		w.Decls = append(newDecls, w.Decls...)
	}
	if gen, ok := w.Decls[importInd].(*ast.GenDecl); ok {
		if !isImportPathExists(gen, imp) {
			gen.Specs = append(gen.Specs, imp)
			w.Decls[importInd] = gen
		}
	}
}

func mergeCodeBase(main, next *ast.File) {
	if next == nil {
		return
	}
	if main == nil {
		main = next
		return
	}
	for _, decl := range next.Decls {
		if isImportSpec(decl, func(imp *ast.ImportSpec) {
			addImport(main, imp)
		}) {
			continue
		}
		main.Decls = append(main.Decls, decl)
	}
}

func makeCall(name ast.Expr, args ...ast.Expr) *ast.CallExpr {
	return &ast.CallExpr{
		Fun:  name,
		Args: args,
	}
}

func makeReturn(results ...ast.Expr) *ast.ReturnStmt {
	return &ast.ReturnStmt{
		Results: results,
	}
}

func makeBlock(statements ...ast.Stmt) *ast.BlockStmt {
	return &ast.BlockStmt{
		List: statements,
	}
}

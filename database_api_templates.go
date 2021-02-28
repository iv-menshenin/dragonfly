package dragonfly

import (
	"github.com/iv-menshenin/dragonfly/utils"
	"github.com/iv-menshenin/go-ast"
	"go/ast"
	"go/token"
	"math"
	"sort"
)

type (
	AstDataChain struct {
		Types           map[string]*ast.TypeSpec
		Constants       map[string]*ast.ValueSpec
		Implementations map[string]*ast.FuncDecl
	}
	AstData struct {
		Chains []AstDataChain
	}
	ApiFuncBuilder func(
		fullTableName, functionName, rowStructName string,
		optionsFields, mutableFields, resultFields []dataCellFactory,
	) AstDataChain
)

func sortedDataChainTypes(source map[string]*ast.TypeSpec) []ast.Spec {
	var (
		result = make([]ast.Spec, 0, len(source))
		names  = make([]string, 0, len(source))
	)
	for key := range source {
		names = append(names, key)
	}
	sort.StringSlice(names).Sort()
	for _, typeName := range names {
		if typeDecl, ok := source[typeName]; ok && typeDecl != nil {
			typeDecl.Name.NamePos = 1
			result = append(result, typeDecl)
		}
	}
	return result
}

func sortedDataChainConstants(source map[string]*ast.ValueSpec) []ast.Spec {
	var (
		result = make([]ast.Spec, 0, len(source))
		names  = make([]string, 0, len(source))
	)
	for key := range source {
		names = append(names, key)
	}
	sort.StringSlice(names).Sort()
	for _, constName := range names {
		if constDecl, ok := source[constName]; ok && constDecl != nil {
			for i := range constDecl.Names {
				constDecl.Names[i].NamePos = 1
			}
			result = append(result, constDecl)
		}
	}
	return result
}

func sortedDataChainImplementations(source map[string]*ast.FuncDecl) []ast.Decl {
	var (
		result = make([]ast.Decl, 0, len(source))
		names  = make([]string, 0, len(source))
	)
	for key := range source {
		names = append(names, key)
	}
	sort.StringSlice(names).Sort()
	for _, funcName := range names {
		if funcDecl, ok := source[funcName]; ok && funcDecl != nil {
			funcDecl.Type.Func = 1
			if funcDecl.Type.Params != nil {
				funcDecl.Type.Params.Opening = 1
			}
			funcDecl.Name.NamePos = 1
			result = append(result, funcDecl)
		}
	}
	return result
}

func getPackagePath(pack string) string {
	// TODO
	packs := map[string]string{
		"bytes":   "bytes",
		"context": "context",
		"fmt":     "fmt",
		"regexp":  "regexp",
		"strconv": "strconv",
		"strings": "strings",
		"time":    "time",
		"rand":    "math/rand",
		"sql":     "database/sql",
		"json":    "encoding/json",
		"driver":  "database/sql/driver",
	}
	if path, ok := packs[pack]; ok {
		return path
	} else {
		return ""
	}
}

func inScopePackageAppend(scope, packages []string, x ...string) []string {
	for _, pack := range x {
		if !utils.ArrayContains(scope, pack) {
			packages = append(packages, pack)
		}
	}
	return packages
}

func extractPackagesFromBlock(t ast.BlockStmt, scopes []string) []string {
	var packages []string
	imp := make([]string, 0, len(t.List))
	for _, stmt := range t.List {
		switch s := stmt.(type) {
		case *ast.BlockStmt:
			packages = extractPackagesFromBlock(*s, scopes)
			imp = inScopePackageAppend(scopes, imp, packages...)
		case *ast.ReturnStmt:
			if r, ok := stmt.(*ast.ReturnStmt); ok {
				for _, expr := range r.Results {
					packages, scopes = extractPackagesFromExpression(expr, scopes)
					imp = inScopePackageAppend(scopes, imp, packages...)
				}
			}
		case *ast.AssignStmt:
			for _, expr := range s.Rhs {
				packages, scopes = extractPackagesFromExpression(expr, scopes)
				imp = inScopePackageAppend(scopes, imp, packages...)
			}
		case *ast.DeclStmt:
			if g, ok := s.Decl.(*ast.GenDecl); ok {
				if g.Tok == token.VAR {
					for _, spec := range g.Specs {
						if v, ok := spec.(*ast.ValueSpec); ok {
							if v.Type != nil {
								packages, scopes = extractPackagesFromExpression(v.Type, scopes)
								imp = inScopePackageAppend(scopes, imp, packages...)
							}
						}
					}
				}
			}
		case *ast.IfStmt:
			if s.Init != nil {
				packages = extractPackagesFromBlock(ast.BlockStmt{List: []ast.Stmt{s.Init}}, scopes)
				imp = inScopePackageAppend(scopes, imp, packages...)
			}
			if s.Body != nil {
				packages = extractPackagesFromBlock(*s.Body, scopes)
				imp = inScopePackageAppend(scopes, imp, packages...)
			}
			if s.Else != nil {
				if b, ok := s.Else.(*ast.BlockStmt); ok {
					packages = extractPackagesFromBlock(*b, scopes)
					imp = inScopePackageAppend(scopes, imp, packages...)
				} else {
					packages = extractPackagesFromBlock(ast.BlockStmt{List: []ast.Stmt{s.Else}}, scopes)
					imp = inScopePackageAppend(scopes, imp, packages...)
				}
			}
		case *ast.ForStmt:
			if s.Body != nil {
				packages = extractPackagesFromBlock(*s.Body, scopes)
				imp = inScopePackageAppend(scopes, imp, packages...)
			}
		}
	}
	return imp
}

func extractPackagesFromFieldList(t ast.FieldList, scopes []string) ([]string, []string) {
	newScopes := make([]string, 0, 0)
	imp := make([]string, 0, len(t.List))
	for _, fld := range t.List {
		for _, name := range fld.Names {
			newScopes = append(newScopes, name.Name)
		}
		var packages []string
		packages, scopes = extractPackagesFromExpression(fld.Type, scopes)
		imp = inScopePackageAppend(scopes, imp, packages...)
	}
	return imp, append(scopes, newScopes...)
}

func extractPackagesFromExpression(t ast.Expr, scopes []string) ([]string, []string) {
	switch s := t.(type) {
	case *ast.StarExpr:
		return extractPackagesFromExpression(s.X, scopes)
	case *ast.SelectorExpr:
		if i, ok := s.X.(*ast.Ident); ok {
			return []string{i.Name}, scopes
		}
	case *ast.StructType:
		if s.Fields != nil {
			return extractPackagesFromFieldList(*s.Fields, scopes)
		}
	case *ast.CallExpr:
		imp, sc := extractPackagesFromExpression(s.Fun, scopes)
		for _, arg := range s.Args {
			var packages []string
			packages, sc = extractPackagesFromExpression(arg, sc)
			imp = inScopePackageAppend(scopes, imp, packages...)
		}
		return imp, sc
	case *ast.BinaryExpr:
		imp1, _ := extractPackagesFromExpression(s.X, scopes)
		imp2, _ := extractPackagesFromExpression(s.Y, scopes)
		return append(imp1, imp2...), scopes
	}
	return nil, scopes
}

func (c AstDataChain) extractImports() map[string]string {
	// TODO except field of struct
	var (
		imports   = make(map[string]string, 0)
		addImport = func(pack string) {
			path := getPackagePath(pack)
			if path != "" {
				imports[pack] = path
			}
		}
	)
	for _, t := range c.Types {
		packages, _ := extractPackagesFromExpression(t.Type, nil)
		for _, pack := range packages {
			addImport(pack)
		}
	}
	for _, t := range c.Constants {
		packages, _ := extractPackagesFromExpression(t.Type, nil)
		for _, pack := range packages {
			addImport(pack)
		}
	}
	for _, f := range c.Implementations {
		var (
			scopes   = make([]string, 0, 10)
			packages = make([]string, 0, 10)
		)
		if f.Recv != nil {
			packages, scopes = extractPackagesFromFieldList(*f.Recv, scopes)
			for _, pack := range packages {
				addImport(pack)
			}
		}
		if f.Type.Params != nil {
			packages, scopes = extractPackagesFromFieldList(*f.Type.Params, scopes)
			for _, pack := range packages {
				addImport(pack)
			}
		}
		if f.Type.Results != nil {
			packages, scopes = extractPackagesFromFieldList(*f.Type.Results, scopes)
			for _, pack := range packages {
				addImport(pack)
			}
		}
		if f.Body != nil {
			for _, pack := range extractPackagesFromBlock(*f.Body, scopes) {
				addImport(pack)
			}
		}
	}
	return imports
}

func (c *AstData) makeAstFile(packageName string) (*ast.File, *token.FileSet) {
	var file ast.File
	file.Name = ast.NewIdent(packageName)
	imports := make(map[string]string, 0)
	var lPos token.Pos = 1
	for _, chain := range c.Chains {
		imports = utils.MergeStringMap(imports, chain.extractImports())
		if typeSpecs := sortedDataChainTypes(chain.Types); len(typeSpecs) > 0 {
			file.Decls = append(file.Decls, &ast.GenDecl{
				Tok:   token.TYPE,
				Specs: typeSpecs,
			})
		}
		constSpecs := sortedDataChainConstants(chain.Constants)
		if len(constSpecs) > 0 {
			file.Decls = append(file.Decls, &ast.GenDecl{
				Tok:   token.CONST,
				Specs: constSpecs,
			})
		}
		funcSpecs := sortedDataChainImplementations(chain.Implementations)
		if len(funcSpecs) > 0 {
			file.Decls = append(file.Decls, funcSpecs...)
		}
	}
	if len(imports) > 0 {
		file.Decls = append(
			[]ast.Decl{
				builders.Import(&lPos, imports),
			},
			file.Decls...,
		)
	}
	fset := token.NewFileSet()
	fset.AddFile(file.Name.String(), 1, math.MaxInt8)
	ast.SortImports(fset, &file)
	return &file, fset
}

var (
	funcTemplates = map[ApiType]ApiFuncBuilder{
		apiTypeFindAll:         makeFindFunction(findVariantAll),
		apiTypeFindAllPaginate: makeFindFunction(findVariantPaginate),
		apiTypeFindOne:         makeFindFunction(findVariantOnce),
		apiTypeLookUp:          makeFindFunction(findVariantOnce),
		apiTypeInsertOne:       insertOneBuilder,
		apiTypeUpdateOne:       makeUpdateFunction(findVariantOnce),
		apiTypeUpdateAll:       makeUpdateFunction(findVariantAll),
		apiTypeUpsertOne:       upsertBuilder,
		apiTypeDeleteOne:       makeDeleteFunction(findVariantOnce),
		apiTypeDeleteAll:       makeDeleteFunction(findVariantAll),
	}
)

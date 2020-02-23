package dragonfly

import (
	"go/ast"
	"go/token"
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
		optionsFields, mutableFields, resultFields []*ast.Field,
	) AstDataChain
)

func getPackagePath(pack string) string {
	packs := map[string]string{
		"rand": "math/rand",
		"sql":  "database/sql",
		"json": "encoding/json",
	}
	if path, ok := packs[pack]; ok {
		return path
	} else {
		return pack
	}
}

func inScopePackageAppend(scope, packages []string, x ...string) []string {
	for _, pack := range x {
		if !arrayContains(scope, pack) {
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
		imports = make(map[string]string, 0)
	)
	for _, t := range c.Types {
		packages, _ := extractPackagesFromExpression(t.Type, nil)
		for _, pack := range packages {
			imports[pack] = getPackagePath(pack)
		}
	}
	for _, t := range c.Constants {
		packages, _ := extractPackagesFromExpression(t.Type, nil)
		for _, pack := range packages {
			imports[pack] = getPackagePath(pack)
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
				imports[pack] = getPackagePath(pack)
			}
		}
		if f.Type.Params != nil {
			packages, scopes = extractPackagesFromFieldList(*f.Type.Params, scopes)
			for _, pack := range packages {
				imports[pack] = getPackagePath(pack)
			}
		}
		if f.Body != nil {
			for _, pack := range extractPackagesFromBlock(*f.Body, scopes) {
				imports[pack] = getPackagePath(pack)
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
		imports = mergeStringMap(imports, chain.extractImports())
		typeSpecs := make([]ast.Spec, 0, len(chain.Types))
		for _, typeDecl := range chain.Types {
			if typeDecl != nil {
				typeDecl.Name.NamePos = lPos
				lPos++
				typeSpecs = append(typeSpecs, typeDecl)
			}
		}
		if len(typeSpecs) > 0 {
			file.Decls = append(file.Decls, &ast.GenDecl{
				Tok:   token.TYPE,
				Specs: typeSpecs,
			})
		}
		constSpecs := make([]ast.Spec, 0, len(chain.Constants))
		for _, constDecl := range chain.Constants {
			if constDecl != nil {
				for i := range constDecl.Names {
					constDecl.Names[i].NamePos = lPos
					lPos++
				}
				constSpecs = append(constSpecs, constDecl)
			}
		}
		if len(constSpecs) > 0 {
			file.Decls = append(file.Decls, &ast.GenDecl{
				Tok:   token.CONST,
				Specs: constSpecs,
			})
		}
		funcSpecs := make([]ast.Decl, 0, len(chain.Implementations))
		for _, funcDecl := range chain.Implementations {
			if funcDecl != nil {
				funcDecl.Type.Func = lPos
				if funcDecl.Type.Params != nil {
					funcDecl.Type.Params.Opening = lPos
				}
				funcDecl.Name.NamePos = lPos
				lPos++
				funcSpecs = append(funcSpecs, funcDecl)
			}
		}
		if len(funcSpecs) > 0 {
			file.Decls = append(file.Decls, funcSpecs...)
		}
	}
	if len(imports) > 0 {
		file.Decls = append(
			[]ast.Decl{
				makeImportDecl(&lPos, imports),
			},
			file.Decls...,
		)
	}
	fset := token.NewFileSet()
	ast.SortImports(fset, &file)
	return &file, fset
}

// get a list of table columns and string field descriptors for the output structure. column and field positions correspond to each other
func extractFieldsAndColumnsFromStruct(rowFields []*ast.Field) (fieldNames, columnNames []string) {
	fieldNames = make([]string, 0, len(rowFields))
	columnNames = make([]string, 0, len(rowFields))
	for _, field := range rowFields {
		if field.Tag != nil {
			tags := tagToMap(field.Tag.Value)
			if sqlTags, ok := tags[TagTypeSQL]; ok && len(sqlTags) > 0 && sqlTags[0] != "-" {
				fieldNames = append(fieldNames, "&row."+field.Names[0].Name)
				columnNames = append(columnNames, sqlTags[0])
			}
		}
	}
	return
}

var (
	funcTemplates = map[string]ApiFuncBuilder{
		"findAll":   makeFindFunction(findVariantAll),
		"findOne":   makeFindFunction(findVariantOnce),
		"lookUp":    makeFindFunction(findVariantOnce),
		"insertOne": insertOneBuilder,
		"updateOne": updateOneBuilder,
		"deleteOne": makeDeleteFunction(findVariantOnce),
		"deleteAll": makeDeleteFunction(findVariantAll),
	}
)

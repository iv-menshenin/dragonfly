package builders

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
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

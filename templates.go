package dragonfly

import (
	"go/ast"
)

type (
	AstData struct {
		// Imports    map[string]*ast.ImportSpec Automatically
		Types           map[string]*ast.TypeSpec
		Constants       map[string]*ast.ValueSpec
		Implementations map[string]*ast.FuncDecl
	}
	ApiFuncBuilder func(
		fullTableName, functionName, rowStructName string,
		optionsFields, mutableFields, resultFields []*ast.Field,
	) *AstData
)

func (astData *AstData) makeAstFile(packageName string) *ast.File {
	var file ast.File
	file.Name = makeName(packageName)

	return &file
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

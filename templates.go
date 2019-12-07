package main

import (
	"go/ast"
)

type (
	ApiFuncBuilder func(
		fullTableName, functionName, rowStructName string,
		optionsFields, mutableFields, resultFields []*ast.Field,
	) *ast.File

	templateApi struct {
		TemplateData ApiFuncBuilder
	}
)

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
	funcTemplates = map[string]templateApi{
		"findAll": {
			TemplateData: makeFindFunction(findVariantAll),
		},
		"findOne": {
			TemplateData: makeFindFunction(findVariantOnce),
		},
		"lookUp": {
			TemplateData: makeFindFunction(findVariantOnce),
		},
		"insertOne": {
			TemplateData: insertOneBuilder,
		},

		"updateOne": {
			TemplateData: updateOneBuilder,
		},
		"deleteOne": {
			TemplateData: makeFindFunction(findVariantOnce),
		},
	}
)

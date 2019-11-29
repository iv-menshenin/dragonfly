package main

import (
	"go/ast"
	"regexp"
)

// TODO refactoring

const (
	findAll = `
package generated

import (
	{%Imports}
	"context"
	"database/sql"
)

func {%FunctionName}(ctx context.Context, options {%OptionName}) (result []{%RowName}, err error) {
	var (
		db       *sql.DB
		rows     *sql.Rows
		argms    = make([]interface{}, 0)
		sqlWhere = make([]string, 0)
		sqlText  = {%SqlText}
	)
	if db, err = getDatabase(ctx); err != nil {
		return
	}
	{%ArgmsGenerator}
	if len(sqlWhere) > 0 {
		sqlText += " where (" + strings.Join(sqlWhere, ") and (") + ")"
	}
	if rows, err = db.Query(sqlText, argms...); err != nil {
		return
	}
	for rows.Next() {
		if err = rows.Err(); err != nil {
			return
		}
		var row {%RowName}
		if err = rows.Scan({%Fields}); err != nil {
			return
		}
		result = append(result, row)
	}
	return
}`
	// TODO check option for using UNIQUE key
	findOne = `
package generated

import (
	{%Imports}
	"context"
	"database/sql"
)

func {%FunctionName}(ctx context.Context, options {%OptionName}) (row {%RowName}, err error) {
	var(
		db *sql.DB
		rows *sql.Rows
		sqlText = {%SqlText}
	)
	if db, err = getDatabase(ctx); err != nil {
		return
	}
	if rows, err = db.Query(sqlText, {%Options}); err != nil {
		return
	}
	if rows.Next() {
		if err = rows.Err(); err != nil {
			return
		}
		if err = rows.Scan({%Fields}); err != nil {
			return
		}
	} else {
		err = EmptyResult
	}
	return
}`
)

type (
	ApiFuncBuilder func(
		fullTableName, functionName, rowStructName string,
		optionFields, rowFields []*ast.Field,
	) *ast.File

	templateApi struct {
		Template     string
		TemplateData ApiFuncBuilder
	}
)

var (
	repeaterPattern = regexp.MustCompile(`{(\d+)\*([^\n]*)`)
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
			Template:     findAll,
			TemplateData: makeFindFunction(findVariantAll),
		},
		"findOne": {
			Template:     findOne,
			TemplateData: makeFindFunction(findVariantOnce),
		},
		"lookUp": {
			Template:     findOne,
			TemplateData: makeFindFunction(findVariantOnce),
		},
		"insertOne": {
			Template:     findOne,
			TemplateData: makeInsertFunction(findVariantOnce),
		},
		"updateOne": { // TODO
			Template:     findOne,
			TemplateData: makeFindFunction(findVariantOnce),
		},
		"deleteOne": {
			Template:     findOne,
			TemplateData: makeFindFunction(findVariantOnce),
		},
	}
)

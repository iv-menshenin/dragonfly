package main

import (
	"fmt"
	"go/ast"
	"regexp"
	"strconv"
	"strings"
)

// TODO refactoring

const (
	argmsGenScalar = `
	argms = append(argms, {%Option})
	sqlWhere = append(sqlWhere, fmt.Sprintf("{%Expression}"{{%ColumnsCount}*, "$" + strconv.Itoa(len(argms))}))
`
	argmGenStar = `
	if {%Option} != nil {
		argms = append(argms, {%Option})
		sqlWhere = append(sqlWhere, fmt.Sprintf("{%Expression}"{{%ColumnsCount}*, "$" + strconv.Itoa(len(argms))}))
	}
`
	argmGenArray = `
	var {%FieldName}Array []string
	for _, opt := range options.{%FieldName} {
		argms = append(argms, opt)
		{%FieldName}Array = append({%FieldName}Array, "$" + strconv.Itoa(len(argms)))
	}
	if len({%FieldName}Array) > 0 {
		sqlWhere = append(sqlWhere, fmt.Sprintf("{%Expression}"{{%ColumnsCount}*, strings.Join({%FieldName}Array, ", ")}))
	}
`
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
		sqlText += " where " + strings.Join(sqlWhere, " and ")
	}
	if rows, err = db.Query(sqlText, argms...); err != nil {
		return
	}
	for rows.Next() {
		if err = rows.Err(); err != nil {
			return
		}
		var row AuthAccountsRow
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
	// TODO refactoring: lets simplify
	templateDataCreator func(*SchemaRef, string, string, *TableApi, []*ast.Field, []*ast.Field) map[string]string
	templateApi         struct {
		Template     string
		TemplateData templateDataCreator
	}
)

var (
	repeaterPattern = regexp.MustCompile(`{(\d+)\*([^\n]*)`)
)

const (
	cFunctionName   = "FunctionName"
	cOptionName     = "OptionName"
	cRowName        = "RowName"
	cSqlText        = "SqlText"
	cOptions        = "Options"
	cFields         = "Fields"
	cColumnsCount   = "ColumnsCount"
	cImports        = "Imports"
	cArgmsGenerator = "ArgmsGenerator"
	cFieldName      = "FieldName"
	cExpression     = "Expression"
	cOption         = "Option"
)

func makeSqlBuilderParametersWithWhereClause(
	schema *SchemaRef,
	tableName, rowStructName string,
	api *TableApi,
	optionFields, rowFields []*ast.Field,
	sqlQueryTemplate string,
) map[string]string {
	table := schema.Value.Tables[tableName]
	imports := make([]string, 0)
	whereClause := []string{"1 = 1"}
	functionName := makeExportedName(schema.Value.Name + "-" + tableName + "-" + api.Type)
	if api.Name != "" {
		functionName = makeExportedName(api.Name)
	}
	optionName := functionName + "Option"
	parameters := make([]string, 0, len(optionFields))

	for _, field := range optionFields {
		if field.Tag != nil {
			tags := tagToMap(field.Tag.Value)
			colNames := make([]string, 0, 1)
			// first element is column name
			ci := arrayFind(tags[TagTypeSQL], tagCaseInsensitive) > 0
			if ci {
				// strings.ToLower
				parameters = append(parameters, fmt.Sprintf("strings.ToLower(options.%s)", field.Names[0].Name))
				imports = append(imports, "\"strings\"")
			} else {
				parameters = append(parameters, fmt.Sprintf("options.%s", field.Names[0].Name))
			}
			opTagValue, ok := tags[TagTypeOp]
			if !ok || len(opTagValue) < 1 {
				opTagValue = []string{string(CompareEqual)}
			}
			operator := SqlCompareOperator(opTagValue[0])
			if arrayFind(tags[TagTypeSQL], TagTypeUnion) > 0 {
				if ci {
					for _, colName := range tags[TagTypeUnion] {
						colNames = append(colNames, fmt.Sprintf("lower(%s)", colName))
					}
				} else {
					colNames = tags[TagTypeUnion]
				}
			} else {
				if ci {
					colNames = append(colNames, fmt.Sprintf("lower(%s)", tags[TagTypeSQL][0]))
				} else {
					colNames = append(colNames, tags[TagTypeSQL][0])
				}
			}
			colExpressions := make([]string, 0, len(colNames))
			for _, colName := range colNames {
				colExpressions = append(colExpressions, operator.getExpression(colName, "$"+strconv.Itoa(len(parameters))))
			}
			whereClause = append(whereClause, strings.Join(colExpressions, " or "))
		}
	}
	fieldNames := make([]string, 0, len(rowFields))
	columnNames := make([]string, 0, len(rowFields))
	for i, field := range rowFields {
		fieldNames = append(fieldNames, "&row."+field.Names[0].Name)
		columnNames = append(columnNames, table.Columns[i].Value.Name)
	}
	sqlText := evalTemplateParameters(
		sqlQueryTemplate,
		map[string]string{
			"ReturningColumns": strings.Join(columnNames, ", "),
			"TableFullName":    fmt.Sprintf("%s.%s", schema.Value.Name, tableName),
			"WhereExpression":  "(" + strings.Join(whereClause, ") and (") + ")",
		},
	)
	return map[string]string{
		cFunctionName: functionName,
		cOptionName:   optionName,
		cRowName:      rowStructName,
		cSqlText:      sqlText,
		cOptions:      strings.Join(parameters, ", "),
		cFields:       strings.Join(fieldNames, ", "),
		cImports:      strings.Join(imports, "\n\t"),
	}
}

func createSimple(schema *SchemaRef, tableName, rowStructName string, api *TableApi, optionFields, rowFields []*ast.Field) map[string]string {
	queryTemplate := "`select {%ReturningColumns} from {%TableFullName} where {%WhereExpression};`"
	return makeSqlBuilderParametersWithWhereClause(
		schema,
		tableName,
		rowStructName,
		api,
		optionFields,
		rowFields,
		queryTemplate,
	)
}

func createDynamic(schema *SchemaRef, tableName, rowStructName string, api *TableApi, optionFields, rowFields []*ast.Field) map[string]string {
	table := schema.Value.Tables[tableName]
	imports := []string{`"strconv"`, `"fmt"`}
	functionName := makeExportedName(schema.Value.Name + "-" + tableName + "-" + api.Type)
	if api.Name != "" {
		functionName = makeExportedName(api.Name)
	}
	optionName := functionName + "Option"
	parameters := make([]string, 0, len(optionFields))
	ArgmsGenerator := make([]string, 0, len(optionFields))

	for _, field := range optionFields {
		if field.Tag != nil {
			tags := tagToMap(field.Tag.Value)
			colNames := make([]string, 0, 1)
			// first element is column name
			ci := arrayFind(tags[TagTypeSQL], tagCaseInsensitive) > 0
			opTagValue, ok := tags[TagTypeOp]
			if !ok || len(opTagValue) < 1 {
				opTagValue = []string{string(CompareEqual)}
			}
			operator := SqlCompareOperator(opTagValue[0])
			if arrayFind(tags[TagTypeSQL], TagTypeUnion) > 0 {
				if ci {
					for _, colName := range tags[TagTypeUnion] {
						colNames = append(colNames, fmt.Sprintf("lower(%s)", colName))
					}
				} else {
					colNames = tags[TagTypeUnion]
				}
			} else {
				if ci {
					colNames = append(colNames, fmt.Sprintf("lower(%s)", tags[TagTypeSQL][0]))
				} else {
					colNames = append(colNames, tags[TagTypeSQL][0])
				}
			}
			if SqlCompareOperator(opTagValue[0]).isMult() {
				colExpressions := make([]string, 0, len(colNames))
				for _, colName := range colNames {
					colExpressions = append(colExpressions, operator.getExpression(colName, "%s"))
				}
				ArgmsGenerator = append(
					ArgmsGenerator,
					evalTemplateParameters(
						argmGenArray,
						map[string]string{
							cColumnsCount: strconv.Itoa(len(colExpressions)),
							cFieldName:    field.Names[0].String(),
							cExpression:   strings.Join(colExpressions, " or "),
						},
					),
				)
			} else {
				option := fmt.Sprintf("options.%s", field.Names[0].Name)
				if ci {
					// strings.ToLower
					option = fmt.Sprintf("strings.ToLower(options.%s)", field.Names[0].Name)
					imports = append(imports, "\"strings\"")
				}
				colExpressions := make([]string, 0, len(colNames))
				for _, colName := range colNames {
					colExpressions = append(colExpressions, operator.getExpression(colName, "%s"))
				}
				if _, ok := field.Type.(*ast.StarExpr); ok {
					ArgmsGenerator = append(
						ArgmsGenerator,
						evalTemplateParameters(
							argmGenStar,
							map[string]string{
								cColumnsCount: strconv.Itoa(len(colExpressions)),
								cFieldName:    field.Names[0].String(),
								cExpression:   strings.Join(colExpressions, " or "),
								cOption:       option,
							},
						),
					)
				} else {
					ArgmsGenerator = append(
						ArgmsGenerator,
						evalTemplateParameters(
							argmsGenScalar,
							map[string]string{
								cColumnsCount: strconv.Itoa(len(colExpressions)),
								cFieldName:    field.Names[0].String(),
								cExpression:   strings.Join(colExpressions, " or "),
								cOption:       option,
							},
						),
					)
				}
			}
		}
	}
	fieldNames := make([]string, 0, len(rowFields))
	columnNames := make([]string, 0, len(rowFields))
	for i, field := range rowFields {
		fieldNames = append(fieldNames, "&row."+field.Names[0].Name)
		columnNames = append(columnNames, table.Columns[i].Value.Name)
	}
	return map[string]string{
		cFunctionName:   functionName,
		cOptionName:     optionName,
		cRowName:        rowStructName,
		cSqlText:        fmt.Sprintf("`select %s from %s.%s `", strings.Join(columnNames, ", "), schema.Value.Name, tableName),
		cOptions:        strings.Join(parameters, ", "),
		cFields:         strings.Join(fieldNames, ", "),
		cImports:        strings.Join(imports, "\n\t"),
		cArgmsGenerator: strings.Join(ArgmsGenerator, "\n"),
	}
}

func createInsertOne(schema *SchemaRef, tableName, rowStructName string, api *TableApi, optionFields, rowFields []*ast.Field) map[string]string {
	table := schema.Value.Tables[tableName]
	imports := make([]string, 0)
	functionName := makeExportedName(schema.Value.Name + "-" + tableName + "-" + api.Type)
	if api.Name != "" {
		functionName = makeExportedName(api.Name)
	}
	optionName := functionName + "Option"
	parameters := make([]string, 0, len(optionFields))
	inputColumnNames := make([]string, 0, len(optionFields))

	for _, field := range optionFields {
		if field.Tag != nil {
			tags := tagToMap(field.Tag.Value)
			if sqlTag, ok := tags[TagTypeSQL]; ok && len(sqlTag) > 0 && tags[TagTypeSQL][0] != "-" {
				inputColumnNames = append(inputColumnNames, tags[TagTypeSQL][0])
				parameters = append(parameters, fmt.Sprintf("options.%s", field.Names[0].Name))
			}
		}
	}
	fieldNames := make([]string, 0, len(rowFields))
	columnNames := make([]string, 0, len(rowFields))
	for i, field := range rowFields {
		fieldNames = append(fieldNames, "&row."+field.Names[0].Name)
		columnNames = append(columnNames, table.Columns[i].Value.Name)
	}
	placeholders := make([]string, 0, len(parameters))
	for i, _ := range parameters {
		placeholders = append(placeholders, "$"+strconv.Itoa(i+1))
	}
	return map[string]string{
		cFunctionName: functionName,
		cOptionName:   optionName,
		cRowName:      rowStructName,
		cSqlText:      fmt.Sprintf("`insert into %s.%s (%s) values (%s) returning %s;`", schema.Value.Name, tableName, strings.Join(inputColumnNames, ", "), strings.Join(placeholders, ", "), strings.Join(columnNames, ", ")),
		cOptions:      strings.Join(parameters, ", "),
		cFields:       strings.Join(fieldNames, ", "),
		cImports:      strings.Join(imports, "\n\t"),
	}
}

func createDeleteOne(schema *SchemaRef, tableName, rowStructName string, api *TableApi, optionFields, rowFields []*ast.Field) map[string]string {
	queryTemplate := "`delete from {%TableFullName} where {%WhereExpression} returning {%ReturningColumns};`"
	return makeSqlBuilderParametersWithWhereClause(
		schema,
		tableName,
		rowStructName,
		api,
		optionFields,
		rowFields,
		queryTemplate,
	)
}

var (
	funcTemplates = map[string]templateApi{
		"findAll": {
			Template:     findAll,
			TemplateData: createDynamic,
		},
		"findOne": {
			Template:     findOne,
			TemplateData: createSimple,
		},
		"lookUp": {
			Template:     findOne,
			TemplateData: createSimple,
		},
		"insertOne": {
			Template:     findOne,
			TemplateData: createInsertOne,
		},
		"updateOne": { // TODO
			Template:     findOne,
			TemplateData: createDynamic,
		},
		"deleteOne": {
			Template:     findOne,
			TemplateData: createDeleteOne,
		},
	}
)

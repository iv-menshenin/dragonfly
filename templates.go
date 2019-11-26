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
	if {%RawOption} != nil {
		argms = append(argms, {%Option})
		sqlWhere = append(sqlWhere, fmt.Sprintf("{%Expression}"{{%ColumnsCount}*, "$" + strconv.Itoa(len(argms))}))
	}
`
	argmGenArray = `
	var arr{%FieldName} []string
	for _, opt := range options.{%FieldName} {
		argms = append(argms, opt)
		arr{%FieldName} = append(arr{%FieldName}, "$" + strconv.Itoa(len(argms)))
	}
	if len(arr{%FieldName}) > 0 {
		sqlWhere = append(sqlWhere, fmt.Sprintf("{%Expression}"{{%ColumnsCount}*, strings.Join(arr{%FieldName}, ", ")}))
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
	templateDataCreator func(
		fullTableName, functionName, rowStructName string,
		optionFields, rowFields []*ast.Field,
	) map[string]string

	templateApi struct {
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
	cRawOption      = "RawOption"
	cFields         = "Fields"
	cColumnsCount   = "ColumnsCount"
	cImports        = "Imports"
	cArgmsGenerator = "ArgmsGenerator"
	cFieldName      = "FieldName"
	cExpression     = "Expression"
	cOption         = "Option"
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

func extractQueryArgumentsAndColumnsFromStruct(optionFields []*ast.Field) (queryArguments, columnNames []string) {
	queryArguments = make([]string, 0, len(optionFields))
	columnNames = make([]string, 0, len(optionFields))
	for _, field := range optionFields {
		if field.Tag != nil {
			tags := tagToMap(field.Tag.Value)
			if sqlTag, ok := tags[TagTypeSQL]; ok && len(sqlTag) > 0 && tags[TagTypeSQL][0] != "-" {
				queryArguments = append(queryArguments, "options."+field.Names[0].Name)
				columnNames = append(columnNames, tags[TagTypeSQL][0])
			}
		}
	}
	return
}

func makeSqlBuilderParametersWithWhereClause(
	functionName, rowStructName string,
	optionFields, rowFields []*ast.Field,
	sqlQueryTemplate string,
) map[string]string {
	imports := make([]string, 0)
	whereClause := []string{"1 = 1"}
	optionName := functionName + "Option"
	parameters := make([]string, 0, len(optionFields))
	// preparing options
	for _, field := range optionFields {
		if field.Tag != nil {
			tags := tagToMap(field.Tag.Value)
			colNames := make([]string, 0, 1)
			// first element is column name
			ci := arrayFind(tags[TagTypeSQL], tagCaseInsensitive) > 0
			if ci {
				// strings.ToLower
				extOption := ""
				if _, ok := field.Type.(*ast.StarExpr); ok {
					extOption = "*"
				}
				parameters = append(parameters, fmt.Sprintf("strings.ToLower(%soptions.%s)", extOption, field.Names[0].Name))
				imports = append(imports, "\"strings\"")
			} else {
				parameters = append(parameters, fmt.Sprintf("options.%s", field.Names[0].Name))
			}
			opTagValue, ok := tags[TagTypeOp]
			if !ok || len(opTagValue) < 1 {
				opTagValue = []string{string(CompareEqual)}
			}
			operator := sqlCompareOperator(opTagValue[0])
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

	fieldNames, columnNames := extractFieldsAndColumnsFromStruct(rowFields)

	sqlText := evalTemplateParameters(
		sqlQueryTemplate,
		map[string]string{
			"ReturningColumns": strings.Join(columnNames, ", "),
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

func createSimple(
	fullTableName, functionName, rowStructName string,
	optionFields, rowFields []*ast.Field,
) map[string]string {
	queryTemplate := "`select {%ReturningColumns} from " + fullTableName + " where {%WhereExpression};`"
	return makeSqlBuilderParametersWithWhereClause(
		functionName,
		rowStructName,
		optionFields,
		rowFields,
		queryTemplate,
	)
}

func createDynamic(
	fullTableName, functionName, rowStructName string,
	optionFields, rowFields []*ast.Field,
) map[string]string {
	// TODO rebuild it with the makeSqlBuilderParametersWithWhereClause function helpful
	imports := []string{`"strconv"`, `"fmt"`}
	optionName := functionName + "Option"
	parameters := make([]string, 0, len(optionFields))
	ArgmsGenerator := make([]string, 0, len(optionFields))

	for _, field := range optionFields {
		if field.Tag != nil {
			tags := tagToMap(field.Tag.Value)
			colNames := make([]string, 0, 1)
			colExpressions := make([]string, 0, len(colNames))
			// first element is column name
			ci := arrayFind(tags[TagTypeSQL], tagCaseInsensitive) > 0
			opTagValue, ok := tags[TagTypeOp]
			if !ok || len(opTagValue) < 1 {
				opTagValue = []string{string(CompareEqual)}
			}
			genTemplate := argmsGenScalar
			operator := sqlCompareOperator(opTagValue[0])
			rawOption := fmt.Sprintf("options.%s", field.Names[0].Name)
			option := rawOption
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
			if sqlCompareOperator(opTagValue[0]).isMult() {
				for _, colName := range colNames {
					colExpressions = append(colExpressions, operator.getExpression(colName, "%s"))
				}
				genTemplate = argmGenArray
			} else {
				if ci {
					// strings.ToLower
					extOption := ""
					if _, ok := field.Type.(*ast.StarExpr); ok {
						extOption = "*"
					}
					option = fmt.Sprintf("strings.ToLower(%soptions.%s)", extOption, field.Names[0].Name)
					imports = append(imports, "\"strings\"")
				}
				for _, colName := range colNames {
					colExpressions = append(colExpressions, operator.getExpression(colName, "%s"))
				}
				if _, ok := field.Type.(*ast.StarExpr); ok {
					genTemplate = argmGenStar
				}
			}
			ArgmsGenerator = append(
				ArgmsGenerator,
				evalTemplateParameters(
					genTemplate,
					map[string]string{
						cColumnsCount: strconv.Itoa(len(colExpressions)),
						cFieldName:    field.Names[0].String(),
						cExpression:   strings.Join(colExpressions, " or "),
						cOption:       option,
						cRawOption:    rawOption,
					},
				),
			)
		}
	}

	fieldNames, columnNames := extractFieldsAndColumnsFromStruct(rowFields)

	return map[string]string{
		cFunctionName:   functionName,
		cOptionName:     optionName,
		cRowName:        rowStructName,
		cSqlText:        fmt.Sprintf("`select %s from %s `", strings.Join(columnNames, ", "), fullTableName),
		cOptions:        strings.Join(parameters, ", "),
		cFields:         strings.Join(fieldNames, ", "),
		cImports:        strings.Join(imports, "\n\t"),
		cArgmsGenerator: strings.Join(ArgmsGenerator, "\n"),
	}
}

func createInsertOne(
	fullTableName, functionName, rowStructName string,
	optionFields, rowFields []*ast.Field,
) map[string]string {
	imports := make([]string, 0)
	optionName := functionName + "Option"
	queryArgs, inputColumnNames := extractQueryArgumentsAndColumnsFromStruct(optionFields)
	fieldNames, columnNames := extractFieldsAndColumnsFromStruct(rowFields)
	placeholders := make([]string, 0, len(queryArgs))
	for i, _ := range queryArgs {
		placeholders = append(placeholders, "$"+strconv.Itoa(i+1))
	}
	return map[string]string{
		cFunctionName: functionName,
		cOptionName:   optionName,
		cRowName:      rowStructName,
		cSqlText:      fmt.Sprintf("`insert into %s (%s) values (%s) returning %s;`", fullTableName, strings.Join(inputColumnNames, ", "), strings.Join(placeholders, ", "), strings.Join(columnNames, ", ")),
		cOptions:      strings.Join(queryArgs, ", "),
		cFields:       strings.Join(fieldNames, ", "),
		cImports:      strings.Join(imports, "\n\t"),
	}
}

func createDeleteOne(
	fullTableName, functionName, rowStructName string,
	optionFields, rowFields []*ast.Field,
) map[string]string {
	queryTemplate := "`delete from " + fullTableName + " where {%WhereExpression} returning {%ReturningColumns};`"
	return makeSqlBuilderParametersWithWhereClause(
		functionName,
		rowStructName,
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

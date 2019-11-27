package main

import (
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode"
)

type (
	sqlCompareOperator string
)

const (
	CompareEqual     sqlCompareOperator = "equal"
	CompareNotEqual  sqlCompareOperator = "notEqual"
	CompareLike      sqlCompareOperator = "like"
	CompareNotLike   sqlCompareOperator = "notLike"
	CompareIn        sqlCompareOperator = "in"
	CompareNotIn     sqlCompareOperator = "notIn"
	CompareGreatThan sqlCompareOperator = "great"
	CompareLessThan  sqlCompareOperator = "less"
	CompareNotGreat  sqlCompareOperator = "notGreat"
	CompareNotLess   sqlCompareOperator = "notLess"
	CompareStarts    sqlCompareOperator = "starts"

	TagTypeSQL   = "sql"
	TagTypeUnion = "union"
	TagTypeOp    = "operator"

	tagNoInsert        = "noInsert"
	tagCaseInsensitive = "ci"
	tagEncrypt         = "encrypt"
	tagIdentifier      = "identifier"
)

var (
	compareOperators = []sqlCompareOperator{
		CompareEqual,
		CompareNotEqual,
		CompareLike,
		CompareNotLike,
		CompareIn,
		CompareNotIn,
		CompareGreatThan,
		CompareLessThan,
		CompareNotGreat,
		CompareNotLess,
		CompareStarts,
	}
	multiCompareOperators = []sqlCompareOperator{
		CompareIn,
		CompareNotIn,
	}
)

func (c *sqlCompareOperator) Check() {
	if c == nil || *c == "" {
		*c = CompareEqual
	}
	for _, op := range compareOperators {
		if op == *c {
			return
		}
	}
	panic(fmt.Sprintf("unknown compare operator '%s'", string(*c)))
}

func (c sqlCompareOperator) isMult() bool {
	for _, op := range multiCompareOperators {
		if op == c {
			return true
		}
	}
	return false
}

func (c sqlCompareOperator) getExpression(sLeft, sRight string) string {
	c.Check()
	templates := map[sqlCompareOperator]string{
		CompareEqual:     `%s = %s`,
		CompareNotEqual:  `% != %s`,
		CompareLike:      `%s like %s`,
		CompareNotLike:   `%s not like %s`,
		CompareIn:        `%s in (%s)`,
		CompareNotIn:     `%s not in (%s)`,
		CompareGreatThan: `%s > %s`,
		CompareLessThan:  `%s < %s`,
		CompareNotGreat:  `%s <= %s`,
		CompareNotLess:   `%s >= %s`,
		CompareStarts:    `%s starts with %s`,
	}
	if template, ok := templates[c]; ok {
		return fmt.Sprintf(template, sLeft, sRight)
	}
	panic(fmt.Sprintf("cannot find template for operator '%s'", string(c)))
}

func generateExportedNameFromRef(ref *string) string {
	refSmts := strings.Split(*ref, "/")
	// TODO
	return makeExportedName(strings.Join(refSmts[len(refSmts)-2:], "-"))
}

func makeExportedName(name string) string {
	var (
		reader   io.RuneReader = strings.NewReader(name)
		toUpper                = true
		exported               = make([]rune, 0, len(name))
	)
	for {
		r, _, err := reader.ReadRune()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		if unicode.IsLetter(r) {
			if toUpper {
				exported = append(exported, unicode.ToUpper(r))
				toUpper = false
			} else {
				exported = append(exported, r)
			}
		} else {
			toUpper = true
			if unicode.IsNumber(r) {
				exported = append(exported, r)
			}
		}
	}
	return string(exported)
}

func (c *DomainSchema) describeGO(typeName string) fieldDescriber {
	return goTypeParametersBySqlType(typeName, c)
}

func (c *Column) tags() []string {
	var tags = append([]string{c.Name}, c.Tags...)
	if c.Schema.Value.NotNull {
		tags = append(tags, "required")
	} else {
		tags = append(tags, "omitempty")
	}
	return tags
}

func (c *Column) describeGO() fieldDescriber {
	typeName := ""
	if c.Schema.Ref != nil {
		typeName = generateExportedNameFromRef(c.Schema.Ref)
	} else {
		typeName = makeExportedName(c.Name)
	}
	return c.Schema.Value.describeGO(typeName)
}

func (c *ColumnRef) generateField(w *ast.File, required bool) ast.Field {
	var decorator = func(e ast.Expr) ast.Expr { return e }
	if !required {
		decorator = makeTypeStar
	}
	fieldDescriber := c.Value.describeGO()
	fieldType := fieldDescriber.fieldTypeExpr()
	mergeCodeBase(w, fieldDescriber.getFile())

	return ast.Field{
		Doc: nil,
		Names: []*ast.Ident{
			makeName(makeExportedName(c.Value.Name)),
		},
		Type: decorator(fieldType),
		Tag: makeTagsForField(map[string][]string{
			"sql": c.Value.tags(),
		}),
		Comment: nil,
	}
}

func (c *TableClass) generateFields(w *ast.File) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(c.Columns))
	for _, column := range c.Columns {
		field := column.generateField(w, column.Value.Schema.Value.NotNull)
		fields = append(fields, &field)
	}
	return
}

func (r *TableApi) generateInsertable(table *TableClass, w *ast.File) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(table.Columns))
	for _, column := range table.Columns {
		if !arrayContains(column.Value.Tags, tagNoInsert) {
			field := column.generateField(w, column.Value.Schema.Value.NotNull)
			fields = append(fields, &field)
		}
	}
	return
}

func (r *TableApi) generateIdentifierOption(table *TableClass, w *ast.File) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(table.Columns))
	for _, column := range table.Columns {
		if arrayContains(column.Value.Tags, tagIdentifier) {
			field := column.generateField(w, column.Value.Schema.Value.NotNull)
			fields = append(fields, &field)
		}
	}
	return
}

func (r *TableApi) generateFields(table *TableClass, w *ast.File) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(r.Options))
	// TODO move these case to uplevel
	if len(r.Options) == 0 && r.Type == "insertOne" {
		fields = append(fields, r.generateInsertable(table, w)...)
		return
	}
	if len(r.Options) == 0 && r.Type == "deleteOne" {
		fields = append(fields, r.generateIdentifierOption(table, w)...)
		return
	}
	// </move these case to uplevel> -------------------
	for _, option := range r.Options {
		operator := option.Operator
		operator.Check()
		if option.Column != "" {
			// TODO move to new function
			if len(option.OneOf) > 0 {
				panic("the option must contains 'one_of' or 'field' not both")
			}
			column := table.Columns.find(option.Column)
			field := column.generateField(w, option.Required || operator.isMult())
			if operator.isMult() {
				field.Type = &ast.ArrayType{
					Elt: field.Type,
				}
			}
			if field.Tag != nil {
				if sqlTags, ok := tagToMap(field.Tag.Value)[TagTypeSQL]; ok {
					sqlTags = arrayRemove(sqlTags, "required")
					field.Tag = makeTagsForField(map[string][]string{
						TagTypeSQL: sqlTags,
						TagTypeOp:  {string(operator)},
					})
				}
			}
			fields = append(fields, &field)
			continue
		}
		if len(option.OneOf) > 0 {
			// TODO move to new function
			unionColumns := make([]string, 0, len(option.OneOf))
			for _, oneOf := range option.OneOf {
				unionColumns = append(unionColumns, oneOf.Column)
				if oneOf.Column == "" {
					panic("each of 'one_of' must contains 'column'")
				}
				if len(oneOf.OneOf) > 0 {
					panic("nested 'one_of' does not supported")
				}
			}
			firstColumn := ColumnsContainer(table.Columns).find(option.OneOf[0].Column)
			baseType := firstColumn.generateField(w, true)
			for _, oneOf := range option.OneOf[1:] {
				nextColumn := ColumnsContainer(table.Columns).find(oneOf.Column)
				nextType := nextColumn.generateField(w, true).Type
				if !reflect.DeepEqual(baseType.Type, nextType) {
					panic("each of 'one_of' must have same type of data")
				}
			}
			baseType.Names = []*ast.Ident{
				makeName(makeExportedName("OneOf-" + strings.Join(unionColumns, "-or-"))),
			}
			var (
				ok      bool
				sqlTags = []string{"-", TagTypeUnion}
			)
			if baseType.Tag != nil {
				if sqlTags, ok = tagToMap(baseType.Tag.Value)[TagTypeSQL]; ok {
					sqlTags[0] = "-"
					sqlTags = append(sqlTags, TagTypeUnion)
				}
			}
			baseType.Tag = makeTagsForField(map[string][]string{
				TagTypeSQL:   sqlTags,
				TagTypeUnion: unionColumns,
				TagTypeOp:    {string(operator)},
			})
			fields = append(fields, &baseType)
			continue
		}
		panic("the option must contains 'one_of' or 'column'")
	}
	return
}

type (
	apiBuilder func(*SchemaRef, string, string, []*ast.Field, []*ast.Field) *ast.File
)

func (r *TableApi) getApiBuilder(functionName string) apiBuilder {
	var (
		ok     bool
		tplSet templateApi
	)
	if tplSet, ok = funcTemplates[r.Type]; !ok {
		panic(fmt.Sprintf("cannot find template `%s`", r.Type))
	}
	return func(schema *SchemaRef, tableName, rowStructName string, queryOptionFields, queryOutputFields []*ast.Field) (f *ast.File) {
		var (
			err          error
			templateData = tplSet.TemplateData(
				fmt.Sprintf("%s.%s", schema.Value.Name, tableName),
				functionName,
				rowStructName,
				queryOptionFields,
				queryOutputFields,
			)
		)
		goSampleCode := evalTemplateParameters(tplSet.Template, templateData)
		if f, err = parser.ParseFile(token.NewFileSet(), r.Type, strings.NewReader(goSampleCode), 0); err != nil {
			println(goSampleCode)
			panic(err)
		}
		return
	}
}

func (c *SchemaRef) generateGO(schemaName string, w *ast.File) {
	if len(c.Value.Tables) > 0 {
		for tableName, table := range c.Value.Tables {
			var (
				structName   = makeExportedName(schemaName + "-" + tableName + "-Row")
				resultFields = table.generateFields(w)
			)
			insertNewStructure(w, structName, resultFields, stringToSlice(table.Description))
			if len(table.Api) > 0 {
				for i, api := range table.Api {
					apiName := evalTemplateParameters(
						api.Name,
						map[string]string{
							cNN:      strconv.Itoa(i),
							cSchema:  schemaName,
							cTable:   tableName,
							cApiType: api.Type,
						},
					)
					if apiName == "" {
						panic(fmt.Sprintf("you must specify name for api #%d in '%s' schema '%s' table", i, schemaName, tableName))
					} else {
						apiName = makeExportedName(apiName)
					}
					var (
						optionStructName = apiName + "Option"
						optionFields     = api.generateFields(&table, w)
						builder          = api.getApiBuilder(apiName)
					)
					insertNewStructure(w, optionStructName, optionFields, nil)
					mergeCodeBase(w, builder(c, tableName, structName, optionFields, resultFields))
				}
			}
		}
	}
}

func generateGO(db *Root, schemaName, packageName string, w io.Writer) {
	var file = new(ast.File)
	for _, schema := range db.Schemas {
		if schemaName == "" || schemaName == schema.Value.Name {
			schema.generateGO(schema.Value.Name, file)
		}
	}
	file.Name = makeName(packageName)
	if err := format.Node(w, token.NewFileSet(), file); err != nil {
		panic(err)
	}
}

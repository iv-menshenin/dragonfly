package main

import (
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"reflect"
	"regexp"
	"strings"
	"unicode"
)

type (
	SqlCompareOperator string
)

const (
	CompareEqual     SqlCompareOperator = "equal"
	CompareNotEqual  SqlCompareOperator = "notEqual"
	CompareLike      SqlCompareOperator = "like"
	CompareNotLike   SqlCompareOperator = "notLike"
	CompareIn        SqlCompareOperator = "in"
	CompareNotIn     SqlCompareOperator = "notIn"
	CompareGreatThan SqlCompareOperator = "great"
	CompareLessThan  SqlCompareOperator = "less"
	CompareNotGreat  SqlCompareOperator = "notGreat"
	CompareNotLess   SqlCompareOperator = "notLess"
	CompareStarts    SqlCompareOperator = "starts"

	TagTypeSQL   = "sql"
	TagTypeUnion = "union"
	TagTypeOp    = "operator"

	tagNoInsert        = "noInsert"
	tagCaseInsensitive = "ci"
	tagEncrypt         = "encrypt"
	tagIdentifier      = "identifier"
)

var (
	compareOperators = []SqlCompareOperator{
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
	multiCompareOperators = []SqlCompareOperator{
		CompareIn,
		CompareNotIn,
	}

	knownTypes = map[string]string{
		"smallserial": "int",
		"serial":      "int64",
		"bigserial":   "int64",
		"bigint":      "int64",
		"int4":        "int64",
		"int8":        "int64",
		"int16":       "int64",
		"integer":     "int64",
		"varchar":     "string",
		"character":   "string",
		"char":        "string",
		"bit":         "[]byte",
		"bool":        "bool",
		"boolean":     "bool",
		"date":        "time.Time",
		"timestamp":   "time.Time",
		"timestamptz": "time.Time",
		"timetz":      "time.Time",
		"float":       "float64",
		"float8":      "float64",
		"float16":     "float64",
		"float32":     "float64",
		"smallint":    "int",
		"real":        "float64",
		"numeric":     "float64",
		"decimal":     "float64",
		"json":        "json",
	}
	packagesPath = map[string]string{
		"time": "time",
	}
	typeDescr = regexp.MustCompile("(\\w*)(?:\\s*[([]\\s*((\\d*)\\s*,?\\s*)[)\\]])?")
)

func (c *SqlCompareOperator) Check() {
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

func (c SqlCompareOperator) isMult() bool {
	for _, op := range multiCompareOperators {
		if op == c {
			return true
		}
	}
	return false
}

func (c SqlCompareOperator) getExpression(sLeft, sRight string) string {
	c.Check()
	templates := map[SqlCompareOperator]string{
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

func goTypeParametersBySqlType(t string) (rawGoType, importedPackage string) {
	sub := typeDescr.FindAllStringSubmatch(t, -1)
	if len(sub) > 0 && len(sub[0]) > 1 {
		t = sub[0][1]
	}
	if gotType, ok := knownTypes[strings.ToLower(t)]; ok {
		if sects := strings.Split(gotType, "."); len(sects) > 1 {
			rawGoType = sects[1]
			importedPackage = sects[0]
		} else {
			rawGoType = gotType
		}
	}
	if t == "enum" {
		rawGoType = "string"
	}
	return
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

func (c *DomainSchema) describeGO() ast.Expr {
	goType, importedPackage := goTypeParametersBySqlType(c.Type)
	if importedPackage == "" {
		return makeTypeIdent(goType)
	} else {
		return makeTypeSelector(importedPackage, goType)
	}
}

func (r *Column) tags() []string {
	var tags = append([]string{r.Name}, r.Tags...)
	if r.Schema.Value.NotNull {
		tags = append(tags, "required")
	} else {
		tags = append(tags, "omitempty")
	}
	return tags
}

func (r *Column) describeGO() ast.Expr {
	return r.Schema.Value.describeGO()
}

func (r *ColumnRef) generateField(w *ast.File, required bool) ast.Field {
	var decorator = func(e ast.Expr) ast.Expr { return e }
	if !required {
		decorator = makeTypeStar
	}
	fieldType := r.Value.describeGO()
	// we myst check our imports if type is a selector
	if sel, ok := fieldType.(*ast.SelectorExpr); ok {
		if id, ok := sel.X.(*ast.Ident); ok {
			if neededPackage, ok := packagesPath[id.Name]; ok {
				imp := ast.ImportSpec{
					Path: &ast.BasicLit{Value: fmt.Sprintf("\"%s\"", neededPackage)},
				}
				addImport(w, &imp)
			}
		}
	}
	return ast.Field{
		Doc: nil,
		Names: []*ast.Ident{
			makeName(makeExportedName(r.Value.Name)),
		},
		Type: decorator(fieldType),
		Tag: makeTagsForField(map[string][]string{
			"sql": r.Value.tags(),
		}),
		Comment: nil,
	}
}

func (c *Table) generateFields(w *ast.File) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(c.Columns))
	for _, column := range c.Columns {
		field := column.generateField(w, column.Value.Schema.Value.NotNull)
		fields = append(fields, &field)
	}
	return
}

func (r *TableApi) generateInsertable(table *Table, w *ast.File) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(table.Columns))
	for _, column := range table.Columns {
		if !arrayContains(column.Value.Tags, tagNoInsert) {
			field := column.generateField(w, column.Value.Schema.Value.NotNull)
			fields = append(fields, &field)
		}
	}
	return
}

func (r *TableApi) generateIdentifierOption(table *Table, w *ast.File) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(table.Columns))
	for _, column := range table.Columns {
		if arrayContains(column.Value.Tags, tagIdentifier) {
			field := column.generateField(w, column.Value.Schema.Value.NotNull)
			fields = append(fields, &field)
		}
	}
	return
}

func (r *TableApi) generateFields(table *Table, w *ast.File) (fields []*ast.Field) {
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
		if option.Field != "" {
			// TODO move to new function
			if len(option.OneOf) > 0 {
				panic("the option must contains 'oneOf' or 'field' not both")
			}
			column := TableColumns(table.Columns).find(option.Field)
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
				unionColumns = append(unionColumns, oneOf.Field)
				if oneOf.Field == "" {
					panic("each of 'oneOf' must contains 'fields'")
				}
				if len(oneOf.OneOf) > 0 {
					panic("nested 'oneOf' does not supported")
				}
			}
			firstColumn := TableColumns(table.Columns).find(option.OneOf[0].Field)
			baseType := firstColumn.generateField(w, true)
			for _, oneOf := range option.OneOf[1:] {
				nextColumn := TableColumns(table.Columns).find(oneOf.Field)
				nextType := nextColumn.generateField(w, true).Type
				if !reflect.DeepEqual(baseType.Type, nextType) {
					panic("each of 'oneOf' must have same type of data")
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
		panic("the option must contains 'oneOf' or 'field'")
	}
	return
}

type (
	apiBuilder func(*SchemaRef, string, string, []*ast.Field, []*ast.Field) *ast.File
)

func (r *TableApi) getApiBuilder() apiBuilder {
	var (
		ok     bool
		tplSet templateApi
	)
	if tplSet, ok = funcTemplates[r.Type]; !ok {
		panic(fmt.Sprintf("cannot find template `%s`", r.Type))
	}
	return func(schema *SchemaRef, tableName, rowStructName string, options, row []*ast.Field) (f *ast.File) {
		var (
			err          error
			templateData = tplSet.TemplateData(schema, tableName, rowStructName, r, options, row)
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
			insertNewStructure(w, structName, resultFields, stringRefToSlice(table.Description))
			if len(table.Api) > 0 {
				for _, api := range table.Api {
					var (
						optionStructName = makeExportedName(api.Name + "-Option")
						optionFields     = api.generateFields(&table, w)
						builder          = api.getApiBuilder()
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

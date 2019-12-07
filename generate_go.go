package dragonfly

import (
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode"
)

type (
	sqlCompareOperator string
	ApiDbOperation     int
	ApiInterface       interface {
		String() string
		HasFindOption() bool
		HasInputOption() bool
		Operation() ApiDbOperation
	}
	ApiType string
)

const (
	ApiOperationInsert ApiDbOperation = iota
	ApiOperationUpdate
	ApiOperationSelect
	ApiOperationDelete

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
	tagNoUpdate        = "noUpdate"
	tagAlwaysUpdate    = "alwaysUpdate"
	tagDeletedFlag     = "deletedFlag"
	tagGenerate        = "generate"
	tagCaseInsensitive = "ci"
	tagEncrypt         = "encrypt"
	tagIdentifier      = "identifier"

	generateFunction = "now"

	apiTypeInsertOne ApiType = "insertOne"
	apiTypeUpdateOne ApiType = "updateOne"
	apiTypeDeleteOne ApiType = "deleteOne"
	apiTypeFindOne   ApiType = "findOne"
	apiTypeFindAll   ApiType = "findAll"
	apiTypeLookUp    ApiType = "lookUp"
)

func (c ApiType) String() string {
	return string(c)
}

func (c ApiType) HasFindOption() bool {
	return c == apiTypeUpdateOne || c == apiTypeDeleteOne || c == apiTypeFindOne || c == apiTypeFindAll || c == apiTypeLookUp
}

func (c ApiType) HasInputOption() bool {
	return c == apiTypeUpdateOne || c == apiTypeInsertOne
}

func (c ApiType) Operation() ApiDbOperation {
	switch c {
	case apiTypeInsertOne:
		return ApiOperationInsert
	case apiTypeUpdateOne:
		return ApiOperationUpdate
	case apiTypeDeleteOne:
		return ApiOperationDelete
	}
	return ApiOperationSelect
}

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

func (c sqlCompareOperator) getRawExpression() string {
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
		return template
	}
	panic(fmt.Sprintf("cannot find template for operator '%s'", string(c)))
}

func (c sqlCompareOperator) getExpression(sLeft, sRight string) string {
	return fmt.Sprintf(c.getRawExpression(), sLeft, sRight)
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
		Comment: makeComment([]string{c.Value.Description}),
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

func (c *TableApi) generateInsertable(table *TableClass, w *ast.File) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(table.Columns))
	for _, column := range table.Columns {
		if !arrayContains(column.Value.Tags, tagNoInsert) {
			field := column.generateField(w, column.Value.Schema.Value.NotNull && (column.Value.Schema.Value.Default == nil))
			fields = append(fields, &field)
		}
	}
	return
}

func (c *TableApi) generateMutable(table *TableClass, w *ast.File) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(table.Columns))
	for _, column := range table.Columns {
		if !arrayContains(column.Value.Tags, tagNoUpdate) {
			field := column.generateField(w, column.Value.Schema.Value.NotNull && (column.Value.Schema.Value.Default == nil))
			fields = append(fields, &field)
		}
	}
	return
}

func (c *TableApi) generateIdentifierOption(table *TableClass, w *ast.File) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(table.Columns))
	for _, column := range table.Columns {
		if arrayContains(column.Value.Tags, tagIdentifier) {
			field := column.generateField(w, column.Value.Schema.Value.NotNull)
			fields = append(fields, &field)
		}
	}
	return
}

func (c *TableApi) generateOptions(table *TableClass, w *ast.File) (findBy, mutable []*ast.Field) {
	// TODO split
	findBy = make([]*ast.Field, 0, len(c.FindOptions))
	mutable = make([]*ast.Field, 0, len(c.FindOptions))
	if len(c.ModifyColumns) > 0 {
		for _, columnName := range c.ModifyColumns {
			column := table.Columns.find(columnName)
			required := column.Value.Schema.Value.NotNull && (column.Value.Schema.Value.Default == nil)
			field := column.generateField(w, required)
			mutable = append(mutable, &field)
		}
	} else {
		if c.Type.Operation() == ApiOperationInsert {
			mutable = append(mutable, c.generateInsertable(table, w)...)
		}
		if c.Type.Operation() == ApiOperationUpdate {
			mutable = append(mutable, c.generateMutable(table, w)...)
		}
	}
	if len(c.FindOptions) == 0 && (c.Type == apiTypeDeleteOne || c.Type == apiTypeInsertOne || c.Type == apiTypeUpdateOne) {
		findBy = append(findBy, c.generateIdentifierOption(table, w)...)
		return
	}
	// </move these case to uplevel> -------------------
	for _, option := range c.FindOptions {
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
			findBy = append(findBy, &field)
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
			findBy = append(findBy, &baseType)
			continue
		}
		panic("the option must contains 'one_of' or 'column'")
	}
	return
}

type (
	apiBuilder func(*SchemaRef, string, string, []*ast.Field, []*ast.Field, []*ast.Field) *ast.File
)

func (c *TableApi) getApiBuilder(functionName string) apiBuilder {
	var (
		ok     bool
		tplSet ApiFuncBuilder
	)
	if tplSet, ok = funcTemplates[c.Type.String()]; !ok {
		panic(fmt.Sprintf("cannot find template `%s`", c.Type))
	}
	return func(
		schema *SchemaRef,
		tableName, rowStructName string,
		queryOptionFields, queryInputFields, queryOutputFields []*ast.Field,
	) *ast.File {
		return tplSet(
			fmt.Sprintf("%s.%s", schema.Value.Name, tableName),
			functionName,
			rowStructName,
			queryOptionFields,
			queryInputFields,
			queryOutputFields,
		)
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
							cApiType: api.Type.String(),
						},
					)
					if apiName == "" {
						panic(fmt.Sprintf("you must specify name for api #%d in '%s' schema '%s' table", i, schemaName, tableName))
					} else {
						apiName = makeExportedName(apiName)
					}
					if apiName == "AuthAccountsUpdateOne" {
						// TODO debug
						apiName = "AuthAccountsUpdateOne"
					}
					var (
						optionFields, mutableFields = api.generateOptions(&table, w)
						builder                     = api.getApiBuilder(apiName)
					)
					// insertNewStructure(w, optionStructName, optionFields, nil)
					mergeCodeBase(w, builder(c, tableName, structName, optionFields, mutableFields, resultFields))
				}
			}
		}
	}
}

func GenerateGO(db *Root, schemaName, packageName string, w io.Writer) {
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

func insertTypeSpec(w *ast.File, newType ast.TypeSpec) {
	var genDecls *ast.Decl
	for i, dec := range w.Decls {
		if t, ok := dec.(*ast.GenDecl); ok {
			if t.Tok != token.TYPE {
				continue
			}
			if genDecls == nil {
				genDecls = &w.Decls[i]
			}
			for _, spec := range t.Specs {
				if s, ok := spec.(*ast.TypeSpec); ok {
					if s.Name.Name == newType.Name.Name {
						if !reflect.DeepEqual(s.Type, newType.Type) {
							panic(fmt.Sprintf("%s type is already declared", newType.Name.Name))
						} else {
							return
						}
					}
				}
			}
		}
	}
	if genDecls == nil {
		w.Decls = append(w.Decls, &ast.GenDecl{
			Tok:   token.TYPE,
			Specs: []ast.Spec{&newType},
		})
	} else {
		(*genDecls).(*ast.GenDecl).Specs = append((*genDecls).(*ast.GenDecl).Specs, &newType)
	}
}

func insertNewStructure(w *ast.File, name string, fields []*ast.Field, comments []string) {
	var newType = ast.TypeSpec{
		Doc:  nil,
		Name: makeName(name),
		Type: &ast.StructType{
			Fields:     &ast.FieldList{List: fields},
			Incomplete: false,
		},
		Comment: makeComment(comments),
	}
	insertTypeSpec(w, newType)
}

func isImportSpec(decl ast.Decl, callback func(spec *ast.ImportSpec)) bool {
	if gen, ok := decl.(*ast.GenDecl); ok {
		if gen.Tok == token.IMPORT {
			if callback != nil {
				for _, spec := range gen.Specs {
					if imp, ok := spec.(*ast.ImportSpec); ok {
						callback(imp)
					}
				}
			}
			return true
		}
	}
	return false
}

func addImport(w *ast.File, imp *ast.ImportSpec) {
	isImportPathExists := func(in *ast.GenDecl, what ast.Spec) bool {
		if p, ok := what.(*ast.ImportSpec); ok {
			path := p.Path.Value
			for _, imp := range in.Specs {
				if p, ok := imp.(*ast.ImportSpec); ok {
					if p.Path != nil && p.Path.Value == path {
						return true
					}
				}
			}
		}
		return false
	}
	getIdForImport := func() int {
		for i, decl := range w.Decls {
			if isImportSpec(decl, nil) {
				return i
			}
		}
		return -1
	}
	importInd := getIdForImport()
	if importInd < 0 {
		importInd = 0
		newDecls := make([]ast.Decl, 1, len(w.Decls)+1)
		newDecls[0] = &ast.GenDecl{
			Tok:   token.IMPORT,
			Specs: nil,
		}
		w.Decls = append(newDecls, w.Decls...)
	}
	if gen, ok := w.Decls[importInd].(*ast.GenDecl); ok {
		if !isImportPathExists(gen, imp) {
			gen.Specs = append(gen.Specs, imp)
			w.Decls[importInd] = gen
		}
	}
}

func mergeCodeBase(main, next *ast.File) {
	if next == nil {
		return
	}
	if main == nil {
		main = next
		return
	}
	for _, decl := range next.Decls {
		if isImportSpec(decl, func(imp *ast.ImportSpec) {
			addImport(main, imp)
		}) {
			continue
		}
		main.Decls = append(main.Decls, decl)
	}
}

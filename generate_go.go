package dragonfly

import (
	"errors"
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	"github.com/iv-menshenin/go-ast"
	"go/ast"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

type (
	ApiDbOperation int
	ApiInterface   interface {
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
	ApiOperationUpsert

	// generation tags
	tagNoInsert       = "noInsert"
	tagNoUpdate       = "noUpdate"
	tagNoDefaultValue = "noDefaultValue"
	tagAlwaysUpdate   = "alwaysUpdate"
	tagDeletedFlag    = "deletedFlag"
	tagIdentifier     = "identifier"
	tagIgnore         = "ignore"

	// value tags
	tagCaseInsensitive = "ci"

	apiTypeInsertOne       ApiType = "insertOne"
	apiTypeUpsertOne       ApiType = "upsertOne"
	apiTypeUpdateOne       ApiType = "updateOne"
	apiTypeUpdateAll       ApiType = "updateAll"
	apiTypeDeleteOne       ApiType = "deleteOne"
	apiTypeDeleteAll       ApiType = "deleteAll"
	apiTypeFindOne         ApiType = "findOne"
	apiTypeFindAll         ApiType = "findAll"
	apiTypeFindAllPaginate ApiType = "findAllPaginate"
	apiTypeLookUp          ApiType = "lookUp"
)

func (c ApiType) String() string {
	return string(c)
}

func (c ApiType) HasFindOption() bool {
	op, ok := apiTypeIsOperation[c]
	if !ok {
		return true
	}
	switch op {
	case ApiOperationInsert:
		return false
	default:
		return true
	}
}

func (c ApiType) HasInputOption() bool {
	op, ok := apiTypeIsOperation[c]
	if !ok {
		return true
	}
	switch op {
	case ApiOperationUpdate, ApiOperationInsert, ApiOperationUpsert:
		return true
	default:
		return false
	}
}

var (
	apiTypeIsOperation = map[ApiType]ApiDbOperation{
		apiTypeInsertOne: ApiOperationInsert,
		apiTypeUpsertOne: ApiOperationUpsert,
		apiTypeUpdateOne: ApiOperationUpdate,
		apiTypeDeleteOne: ApiOperationDelete,
		apiTypeDeleteAll: ApiOperationDelete,
	}
)

func (c ApiType) Operation() ApiDbOperation {
	op, ok := apiTypeIsOperation[c]
	if ok {
		return op
	}
	return ApiOperationSelect
}

func generateExportedNameFromRef(ref *string) string {
	refSmts := strings.Split(*ref, "/")
	// TODO
	return makeExportedName(strings.Join(refSmts[len(refSmts)-2:], "-"))
}

func makeExportedName(name string) (result string) {
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

func (c *Column) tags(name string) (tags []string) {
	if name == builders.TagTypeSQL {
		tags = append([]string{c.Name}, c.Tags...)
	} else {
		tagTemplate := regexp.MustCompile(fmt.Sprintf(`^%s\((\w+)\)$`, name))
		fieldName := "-"
		for _, tag := range c.Tags {
			sub := tagTemplate.FindAllStringSubmatch(tag, -1)
			if len(sub) > 0 {
				fieldName = sub[0][1]
			}
		}
		tags = append(tags, fieldName)
	}
	if len(tags) > 0 && tags[0] != "-" {
		if c.Schema.Value.NotNull {
			tags = append(tags, "required")
		} else {
			tags = append(tags, "omitempty")
		}
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

// returning ast.Field and "is custom type" mark
func (c *ColumnRef) generateField(w *AstData, required bool) (ast.Field, bool) {
	var decorator = func(e ast.Expr) ast.Expr { return e }
	if !required && !c.Value.Schema.Value.IsArray {
		decorator = builders.Star
	}
	fieldDescriber := c.Value.describeGO()
	_, isCustomType := fieldDescriber.(customTypeDescriber)
	fieldType := fieldDescriber.fieldTypeExpr()
	if err := mergeCodeBase(w, fieldDescriber.getFile()); err != nil {
		panic(err)
	}

	return ast.Field{
		Names: []*ast.Ident{
			ast.NewIdent(makeExportedName(c.Value.Name)),
		},
		Type: decorator(fieldType),
		Tag: builders.MakeTagsForField(map[string][]string{
			builders.TagTypeSQL:  c.Value.tags(builders.TagTypeSQL),
			builders.TagTypeJSON: c.Value.tags(builders.TagTypeJSON),
		}),
		Comment: builders.CommentGroup(c.Value.Description),
	}, isCustomType
}

func (c *Table) generateFields(w *AstData) (fields []builders.MetaField) {
	fields = make([]builders.MetaField, 0, len(c.Columns))
	for _, column := range c.Columns {
		if utils.ArrayContains(column.Value.Tags, tagIgnore) {
			continue
		}
		field, isCustom := column.generateField(w, column.Value.Schema.Value.NotNull)
		fields = append(fields, makeMetaFieldAsIs(column, field, isCustom))
	}
	return
}

func (c *TableApi) generateFieldsExceptTags(fn fieldConstructor, table *Table, w *AstData, tags ...string) (fields []builders.MetaField) {
	fields = make([]builders.MetaField, 0, len(table.Columns))
	for _, column := range table.Columns {
		if utils.ArrayContains(column.Value.Tags, tagIgnore) {
			continue
		}
		var passToNext = false
		for _, tag := range tags {
			passToNext = passToNext || utils.ArrayContains(column.Value.Tags, tag)
		}
		if passToNext {
			continue
		}
		// we may to allow the absence of a value only if NULL is allowed to column or the field has a default value (in this case, make sure the tagNoDefaultValue tag is missing)
		required := column.Value.Schema.Value.NotNull && (utils.ArrayContains(column.Value.Tags, tagNoDefaultValue) || column.Value.Schema.Value.Default == nil)
		field, isCustom := column.generateField(w, required)
		fields = append(fields, fn(column, field, isCustom))
	}
	return
}

func (c *TableApi) generateInsertable(fn fieldConstructor, table *Table, w *AstData) (fields []builders.MetaField) {
	return c.generateFieldsExceptTags(fn, table, w, tagNoInsert)
}

func (c *TableApi) generateMutable(fn fieldConstructor, table *Table, w *AstData) (fields []builders.MetaField) {
	return c.generateFieldsExceptTags(fn, table, w, tagNoUpdate)
}

func (c *TableApi) generateMutableOrInsertable(fn fieldConstructor, table *Table, w *AstData) (fields []builders.MetaField) {
	return c.generateFieldsExceptTags(fn, table, w, tagNoUpdate, tagNoInsert)
}

func (c *Table) extractColumnsByConstraintName(keyName string) (columns []ColumnRef) {
	if constraint, ok := c.Constraints.tryToFind(keyName); ok {
		columns = make([]ColumnRef, 0, len(constraint.Columns))
		for _, colName := range constraint.Columns {
			column, ok := c.Columns.tryToFind(colName)
			if !ok {
				panic(fmt.Sprintf("unexpected column name `%s` in key `%s`", colName, keyName))
			}
			columns = append(columns, *column)
		}
		return
	} else {
		panic(fmt.Sprintf("cannot find key `%s`", keyName))
	}
}

func (c *Table) extractColumnsByTags(tagName string) (columns []ColumnRef) {
	columns = make([]ColumnRef, 0, len(c.Columns))
	for _, column := range c.Columns {
		if utils.ArrayContains(column.Value.Tags, tagName) {
			columns = append(columns, column)
		}
	}
	return
}

func (c *Table) extractColumnsByUniqueKeyType(keyType ConstraintType) (columns []ColumnRef) {
	for _, column := range c.Columns {
		for _, constraint := range column.Value.Constraints {
			if constraint.Type == keyType {
				// by column allowed only once key describing
				return []ColumnRef{column}
			}
		}
	}
	for _, constraint := range c.Constraints {
		if constraint.Constraint.Type == keyType {
			return c.extractColumnsByConstraintName(constraint.Constraint.Name)
		}
	}
	return
}

func (c *Table) extractPrimaryKeyColumns() (columns []ColumnRef) {
	return c.extractColumnsByUniqueKeyType(ConstraintPrimaryKey)
}

func (c *Table) extractUniqueKeyColumns() (columns []ColumnRef) {
	return c.extractColumnsByUniqueKeyType(ConstraintUniqueKey)
}

func (c *TableApi) generateIdentifierOption(table *Table, w *AstData) (fields []builders.MetaField) {
	var columns []ColumnRef
	if c.Key != "" {
		columns = table.extractColumnsByConstraintName(c.Key)
	} else {
		for {
			if columns = table.extractColumnsByTags(tagIdentifier); len(columns) > 0 {
				break
			}
			if columns = table.extractPrimaryKeyColumns(); len(columns) > 0 {
				break
			}
			if columns = table.extractUniqueKeyColumns(); len(columns) > 0 {
				break
			}
			panic("cannot extract unique columns for " + c.Name)
		}
	}
	fields = make([]builders.MetaField, 0, len(columns))
	for _, column := range columns {
		field, isCustom := column.generateField(w, column.Value.Schema.Value.NotNull)
		fields = append(fields, builders.MetaField{
			Field:           &field,
			SourceSql:       builders.SourceSqlColumn{ColumnName: column.Value.Name},
			CaseInsensitive: utils.ArrayContains(column.Value.Tags, tagCaseInsensitive),
			IsMaybeType:     false,
			IsCustomType:    isCustom,
			CompareOperator: builders.CompareEqual,
			Constant:        "", // not allowed here
		})
	}
	return
}

func checkOption(table *Table, operator builders.SQLDataCompareOperator, option ApiFindOption, w *AstData) {
	if option.Column != "" {
		if len(option.OneOf) > 0 {
			panic("the option must contains 'one_of' or 'field' not both")
		}
		column := table.Columns.getColumn(option.Column)
		if operator == builders.CompareIsNull && column.Value.Schema.Value.NotNull {
			panic(fmt.Sprintf("cannot apply operator `isNull` to not_null column `%s`", column.Value.Name))
		}
	} else if len(option.OneOf) > 0 {
		var basicType ast.Expr = nil
		for _, oneOf := range option.OneOf {
			column := table.Columns.getColumn(oneOf)
			colType, _ := column.generateField(w, option.Required)
			if basicType == nil {
				basicType = colType.Type
			} else if !reflect.DeepEqual(basicType, colType.Type) {
				panic(fmt.Sprintf("each of 'one_of' must have same go-type of data: %v", option.OneOf))
			}
		}
	} else {
		panic("the option must contains 'one_of' or 'column'")
	}
}

func (option ApiFindOption) makeMetaFieldByColumn(table *Table, w *AstData) builders.MetaField {
	column := table.Columns.getColumn(option.Column)
	if option.Operator == builders.CompareIsNull {
		column = ColumnRef{
			Value: Column{
				Name: column.Value.Name,
				Schema: ColumnSchemaRef{
					Value: DomainSchema{
						TypeBase: TypeBase{
							Type: "isnull", // TODO hack?
						},
						NotNull: false,
					},
				},
				Tags: column.Value.Tags,
			},
		}
	}
	field, isCustom := column.generateField(w, option.Required || option.Operator.IsMult())
	if option.Operator.IsMult() {
		field.Type = &ast.ArrayType{Elt: field.Type}
	}
	if field.Tag != nil { // TODO is this necessary?
		if sqlTags, ok := utils.FieldTagToMap(field.Tag.Value)[builders.TagTypeSQL]; ok {
			field.Tag = builders.MakeTagsForField(map[string][]string{
				builders.TagTypeSQL: utils.ArrayRemove(sqlTags, "required"),
			})
		}
	}
	return builders.MetaField{
		Field:           &field,
		SourceSql:       builders.SourceSqlColumn{ColumnName: option.Column},
		CaseInsensitive: utils.ArrayContains(column.Value.Tags, tagCaseInsensitive),
		IsMaybeType:     false,
		IsCustomType:    isCustom,
		CompareOperator: option.Operator,
		Constant:        option.Constant,
	}
}

// TODO split
func (c *TableApi) generateFindFields(table *Table, w *AstData) (findBy []builders.MetaField) {
	findBy = make([]builders.MetaField, 0, len(c.FindOptions))
	for _, option := range c.FindOptions {
		operator := option.Operator
		operator.Check()
		checkOption(table, operator, option, w)
		if option.Column != "" {
			findBy = append(findBy, option.makeMetaFieldByColumn(table, w))
			continue
		}
		if len(option.OneOf) > 0 {
			// TODO move to new function
			unionColumns := make([]string, 0, len(option.OneOf))
			for _, oneOf := range option.OneOf {
				unionColumns = append(unionColumns, oneOf)
			}
			firstColumn := table.Columns.getColumn(option.OneOf[0])
			fieldType, isCustom := firstColumn.generateField(w, option.Required)
			fieldType.Names = []*ast.Ident{
				ast.NewIdent(makeExportedName("OneOf-" + strings.Join(unionColumns, "-or-"))),
			}
			var sqlTags = []string{"-", builders.TagTypeUnion}
			if fieldType.Tag != nil {
				if baseSqlTags, ok := utils.FieldTagToMap(fieldType.Tag.Value)[builders.TagTypeSQL]; ok {
					for _, tag := range baseSqlTags[1:] {
						if tag != "required" {
							sqlTags = append(sqlTags, tag)
						}
					}
				}
			}
			if option.Required {
				sqlTags = append(sqlTags, "required")
			}
			fieldType.Tag = builders.MakeTagsForField(map[string][]string{
				builders.TagTypeSQL:   sqlTags,
				builders.TagTypeUnion: unionColumns,
			})
			findBy = append(findBy, builders.MetaField{
				Field:           &fieldType,
				SourceSql:       builders.SourceSqlSomeColumns{ColumnNames: unionColumns},
				CaseInsensitive: utils.ArrayContains(sqlTags, tagCaseInsensitive),
				IsMaybeType:     false,
				IsCustomType:    isCustom,
				CompareOperator: operator,
				Constant:        "", // not allowed here
			})
			continue
		}
	}
	return
}

var (
	canonTypesToMaybe = map[string]string{
		"time.Time": "MaybeTime",
		"string":    "MaybeString",
		"bool":      "MaybeBool",
		"int":       "MaybeInt",
		"int8":      "MaybeInt8",
		"int16":     "MaybeInt16",
		"int32":     "MaybeInt32",
		"int64":     "MaybeInt64",
		"uint":      "MaybeUInt",
		"uint8":     "MaybeUInt8",
		"uint16":    "MaybeUInt16",
		"uint32":    "MaybeUInt32",
		"uint64":    "MaybeUInt64",
		"float32":   "MaybeFloat32",
		"float64":   "MaybeFloat64",
	}
)

func tryMakeMaybeType(rawTypeName string) ast.Expr {
	if newType, ok := canonTypesToMaybe[rawTypeName]; ok {
		return ast.NewIdent(newType)
	}
	return nil
}

type fieldConstructor func(column ColumnRef, field ast.Field, isCustom bool) builders.MetaField

func makeMetaFieldAsIs(column ColumnRef, field ast.Field, isCustom bool) builders.MetaField {
	return builders.MetaField{
		Field:           &field,
		SourceSql:       builders.SourceSqlColumn{ColumnName: column.Value.Name},
		CaseInsensitive: utils.ArrayContains(column.Value.Tags, tagCaseInsensitive),
		IsCustomType:    isCustom,
	}
}

func makeMetaFieldMaybeType(column ColumnRef, field ast.Field, _ bool) builders.MetaField {
	var rawTypeName = fmt.Sprintf("%s", field.Type)
	if star, ok := field.Type.(*ast.StarExpr); ok {
		if t, ok := star.X.(*ast.Ident); ok {
			rawTypeName = t.String()
		} else if t, ok := star.X.(*ast.SelectorExpr); ok {
			rawTypeName = fmt.Sprintf("%s.%s", t.X, t.Sel)
		}
	} else if t, ok := field.Type.(*ast.Ident); ok {
		rawTypeName = t.String()
	} else if t, ok := field.Type.(*ast.SelectorExpr); ok {
		rawTypeName = fmt.Sprintf("%s.%s", t.X, t.Sel)
	}
	if newType := tryMakeMaybeType(rawTypeName); newType != nil {
		field := ast.Field{
			Names:   field.Names,
			Type:    newType,
			Tag:     field.Tag,
			Comment: field.Comment,
		}
		return builders.MetaField{
			Field:           &field,
			SourceSql:       builders.SourceSqlColumn{ColumnName: column.Value.Name},
			CaseInsensitive: utils.ArrayContains(column.Value.Tags, tagCaseInsensitive),
			IsMaybeType:     true,
		}
	} else {
		if _, ok := field.Type.(*ast.StarExpr); !ok {
			// make custom type as ref, if isn`t yet
			field.Type = &ast.StarExpr{X: field.Type}
		}
		return builders.MetaField{
			Field:           &field,
			SourceSql:       builders.SourceSqlColumn{ColumnName: column.Value.Name},
			CaseInsensitive: utils.ArrayContains(column.Value.Tags, tagCaseInsensitive),
		}
	}
}

func (c *TableApi) generateOptions(table *Table, w *AstData) (findBy, mutable []builders.MetaField) {
	if c.Type.HasFindOption() {
		if len(c.FindOptions) > 0 {
			findBy = c.generateFindFields(table, w)
		} else {
			findBy = c.generateIdentifierOption(table, w)
		}
	} else {
		if len(c.FindOptions) > 0 {
			println(fmt.Sprintf("api type `%s` cannot contains `find_by` options", c.Type))
		}
	}
	if c.Type.HasInputOption() {
		var decorator fieldConstructor = makeMetaFieldAsIs
		if apiTypeIsOperation[c.Type] == ApiOperationUpdate {
			// NOT `apiTypeIsOperation[c.Type] == ApiOperationUpsert`
			// because it must be a fully insertable row
			decorator = makeMetaFieldMaybeType
		}
		if len(c.ModifyColumns) > 0 {
			mutable = make([]builders.MetaField, 0, len(c.ModifyColumns))
			for _, columnName := range c.ModifyColumns {
				column := table.Columns.getColumn(columnName)
				field, isCustom := column.generateField(w, column.Value.Schema.Value.NotNull && (column.Value.Schema.Value.Default == nil))
				mutable = append(mutable, decorator(column, field, isCustom))
			}
		} else {
			if c.Type.Operation() == ApiOperationInsert {
				mutable = c.generateInsertable(decorator, table, w)
			}
			if c.Type.Operation() == ApiOperationUpdate {
				mutable = c.generateMutable(decorator, table, w)
			}
			if c.Type.Operation() == ApiOperationUpsert {
				mutable = c.generateMutableOrInsertable(decorator, table, w)
			}
		}
	} else {
		if len(c.ModifyColumns) > 0 {
			println(fmt.Sprintf("api type `%s` cannot contains `modify` options", c.Type))
		}
	}
	return
}

type (
	apiBuilder func(*SchemaRef, string, string, []builders.MetaField, []builders.MetaField, []builders.MetaField) AstDataChain
)

func (c *TableApi) getApiBuilder(functionName string) apiBuilder {
	var (
		ok     bool
		tplSet ApiFuncBuilder
	)
	if tplSet, ok = funcTemplates[c.Type]; !ok {
		panic(fmt.Sprintf("cannot find template `%s`", c.Type))
	}
	return func(
		schema *SchemaRef,
		tableName, rowStructName string,
		queryOptionFields, queryInputFields, queryOutputFields []builders.MetaField,
	) AstDataChain {
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

func registerStructType(typeName, comment string, fields []builders.MetaField, w *AstData) {
	var extractedFields = make([]*ast.Field, 0, len(fields))
	for _, f := range fields {
		extractedFields = append(extractedFields, f.Field)
	}
	if err := mergeCodeBase(w, []AstDataChain{
		{
			Types: map[string]*ast.TypeSpec{
				typeName: {
					Name: ast.NewIdent(typeName),
					Type: &ast.StructType{
						Fields: &ast.FieldList{List: extractedFields},
					},
					Comment: builders.CommentGroup(comment),
				},
			},
			Constants:       nil,
			Implementations: nil,
		},
	}); err != nil {
		panic(err)
	}
}

func (c TableApi) buildExtendedFields(tableRowStructName string, w *AstData) (apiResultStructName string, additionFields []builders.MetaField) {
	additionFields = make([]builders.MetaField, 0, len(c.Extended)+1)
	apiResultStructName = makeExportedName(c.Name + "ExRow")
	var isCustom = false
	for _, column := range c.Extended {
		var columnRef = ColumnRef{
			Value: Column{
				Name:   column.Name,
				Schema: column.Schema,
			},
		}
		var field ast.Field
		field, isCustom = columnRef.generateField(w, column.Schema.Value.NotNull)
		field.Tag = builders.MakeTagsForField(map[string][]string{
			builders.TagTypeSQL: {column.SQL},
		})
		additionFields = append(additionFields, builders.MetaField{
			Field:     &field,
			SourceSql: builders.SourceSqlExpression{Expression: column.SQL},
		})
	}
	var mainStruct = builders.MetaField{
		Field: &ast.Field{
			Type:    ast.NewIdent(tableRowStructName),
			Comment: builders.CommentGroup("implemented main structure"),
		},
		IsCustomType: isCustom,
	}
	registerStructType(apiResultStructName, c.Name, append([]builders.MetaField{mainStruct}, additionFields...), w)
	return
}

func (c *SchemaRef) generateGO(schemaName string, w *AstData) {
	for _, typeName := range c.Value.Types.getNames() {
		typeSchema := c.Value.Types[typeName]
		typeName = c.Value.Name + "." + typeName
		if err := mergeCodeBase(w, typeSchema.generateType(schemaName, typeName)); err != nil {
			panic(err)
		}
	}
	for _, tableName := range c.Value.Tables.getNames() {
		table := c.Value.Tables[tableName]
		var (
			tableRowStructName = makeExportedName(schemaName + "-" + tableName + "-Row")
			resultFields       = table.generateFields(w)
		)
		registerStructType(tableRowStructName, table.Description, resultFields, w)
		if len(table.Api) > 0 {
			for i, api := range table.Api {
				var additionFields []builders.MetaField
				apiName := utils.EvalTemplateParameters(
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
				var apiResultStructName = tableRowStructName
				if len(api.Extended) > 0 {
					apiResultStructName, additionFields = api.buildExtendedFields(tableRowStructName, w)
				}
				var (
					optionFields, mutableFields = api.generateOptions(&table, w)
					builder                     = api.getApiBuilder(apiName)
				)
				if err := mergeCodeBase(w, []AstDataChain{
					builder(c, tableName, apiResultStructName, optionFields, mutableFields, append(resultFields, additionFields...)),
				}); err != nil {
					panic(err)
				}
			}
		}
	}
}

func mergeCodeBase(main *AstData, chains []AstDataChain) error {
	for _, next := range chains {
		for name, spec := range next.Types {
			for _, chain := range main.Chains {
				if spec2, ok := chain.Types[name]; ok {
					if !reflect.DeepEqual(spec2, spec) {
						return errors.New(fmt.Sprintf("type `%s` repeated with different contents", name))
					} else {
						// TODO WARNING
						delete(next.Types, name)
					}
				}
			}
		}
		for name, cnst := range next.Constants {
			for _, chain := range main.Chains {
				if cnst2, ok := chain.Constants[name]; ok {
					if !reflect.DeepEqual(cnst2, cnst) {
						panic(fmt.Sprintf("constant `%s` repeated with different contents", name))
					} else {
						// TODO WARNING
						delete(next.Constants, name)
					}
				}
			}
		}
		for name, impl := range next.Implementations {
			for _, chain := range main.Chains {
				if impl2, ok := chain.Implementations[name]; ok {
					if !reflect.DeepEqual(impl2, impl) {
						panic(fmt.Sprintf("constant `%s` repeated with different contents", name))
					} else {
						// TODO WARNING
						delete(next.Implementations, name)
					}
				}
			}
		}
		if len(next.Types)+len(next.Implementations)+len(next.Constants) > 0 {
			main.Chains = append(main.Chains, next)
		}
	}
	return nil
}

package dragonfly

import (
	"errors"
	"fmt"
	"github.com/iv-menshenin/dragonfly/code_builders"
	"github.com/iv-menshenin/dragonfly/utils"
	"go/ast"
	"go/printer"
	"io"
	"reflect"
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

	tagNoInsert     = "noInsert"
	tagNoUpdate     = "noUpdate"
	tagAlwaysUpdate = "alwaysUpdate"
	tagDeletedFlag  = "deletedFlag"
	tagIdentifier   = "identifier"

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
	return c != apiTypeInsertOne
}

func (c ApiType) HasInputOption() bool {
	return c != apiTypeDeleteOne && c != apiTypeFindOne && c != apiTypeFindAll && c != apiTypeLookUp
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

func (c *ColumnRef) generateField(w *AstData, required bool) ast.Field {
	var decorator = func(e ast.Expr) ast.Expr { return e }
	if !required {
		decorator = builders.MakeStarExpression
	}
	fieldDescriber := c.Value.describeGO()
	fieldType := fieldDescriber.fieldTypeExpr()
	if err := mergeCodeBase(w, fieldDescriber.getFile()); err != nil {
		panic(err)
	}

	return ast.Field{
		Doc: nil,
		Names: []*ast.Ident{
			ast.NewIdent(makeExportedName(c.Value.Name)),
		},
		Type: decorator(fieldType),
		Tag: builders.MakeTagsForField(map[string][]string{
			"sql": c.Value.tags(),
		}),
		Comment: builders.MakeComment([]string{c.Value.Description}),
	}
}

func (c *Table) generateFields(w *AstData) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(c.Columns))
	for _, column := range c.Columns {
		field := column.generateField(w, column.Value.Schema.Value.NotNull)
		fields = append(fields, &field)
	}
	return
}

func (c *TableApi) generateInsertable(table *Table, w *AstData) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(table.Columns))
	for _, column := range table.Columns {
		if !utils.ArrayContains(column.Value.Tags, tagNoInsert) {
			field := column.generateField(w, column.Value.Schema.Value.NotNull && (column.Value.Schema.Value.Default == nil))
			fields = append(fields, &field)
		}
	}
	return
}

func (c *TableApi) generateMutable(table *Table, w *AstData) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(table.Columns))
	for _, column := range table.Columns {
		if !utils.ArrayContains(column.Value.Tags, tagNoUpdate) {
			field := column.generateField(w, column.Value.Schema.Value.NotNull && (column.Value.Schema.Value.Default == nil))
			fields = append(fields, &field)
		}
	}
	return
}

func (c *TableApi) generateIdentifierOption(table *Table, w *AstData) (fields []*ast.Field) {
	fields = make([]*ast.Field, 0, len(table.Columns))
	for _, column := range table.Columns {
		if utils.ArrayContains(column.Value.Tags, tagIdentifier) {
			field := column.generateField(w, column.Value.Schema.Value.NotNull)
			fields = append(fields, &field)
		}
	}
	if len(fields) > 0 {
		return
	}
	for _, constraint := range table.Constraints {
		if constraint.Constraint.Type == ConstraintPrimaryKey {
			for _, columnName := range constraint.Columns {
				column := table.Columns.getColumn(columnName)
				field := column.generateField(w, column.Value.Schema.Value.NotNull)
				fields = append(fields, &field)
			}
		}
	}
	if len(fields) > 0 {
		return
	}
	for _, constraint := range table.Constraints {
		if constraint.Constraint.Type == ConstraintUniqueKey {
			for _, columnName := range constraint.Columns {
				column := table.Columns.getColumn(columnName)
				field := column.generateField(w, column.Value.Schema.Value.NotNull)
				fields = append(fields, &field)
			}
		}
	}
	return
}

func (c *ApiFindOptions) generateFindFields(table *Table, w *AstData) (findBy []*ast.Field) {
	if c == nil {
		return
	}
	findBy = make([]*ast.Field, 0, len(*c))
	for _, option := range *c {
		operator := option.Operator
		operator.Check()
		if option.Column != "" {
			// TODO move to new function
			if len(option.OneOf) > 0 {
				panic("the option must contains 'one_of' or 'field' not both")
			}
			column := table.Columns.getColumn(option.Column)
			field := column.generateField(w, option.Required || operator.IsMult())
			if operator.IsMult() {
				field.Type = &ast.ArrayType{
					Elt: field.Type,
				}
			}
			if field.Tag != nil {
				if sqlTags, ok := utils.FieldTagToMap(field.Tag.Value)[builders.TagTypeSQL]; ok {
					sqlTags = utils.ArrayRemove(sqlTags, "required")
					field.Tag = builders.MakeTagsForField(map[string][]string{
						builders.TagTypeSQL: sqlTags,
						builders.TagTypeOp:  {string(operator)},
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
			firstColumn := table.Columns.getColumn(option.OneOf[0].Column)
			baseType := firstColumn.generateField(w, true)
			for _, oneOf := range option.OneOf[1:] {
				nextColumn := table.Columns.getColumn(oneOf.Column)
				nextType := nextColumn.generateField(w, true).Type
				if !reflect.DeepEqual(baseType.Type, nextType) {
					panic("each of 'one_of' must have same type of data")
				}
			}
			baseType.Names = []*ast.Ident{
				ast.NewIdent(makeExportedName("OneOf-" + strings.Join(unionColumns, "-or-"))),
			}
			var (
				ok      bool
				sqlTags = []string{"-", builders.TagTypeUnion}
			)
			if baseType.Tag != nil {
				if sqlTags, ok = utils.FieldTagToMap(baseType.Tag.Value)[builders.TagTypeSQL]; ok {
					sqlTags[0] = "-"
					sqlTags = append(sqlTags, builders.TagTypeUnion)
				}
			}
			baseType.Tag = builders.MakeTagsForField(map[string][]string{
				builders.TagTypeSQL:   sqlTags,
				builders.TagTypeUnion: unionColumns,
				builders.TagTypeOp:    {string(operator)},
			})
			findBy = append(findBy, &baseType)
			continue
		}
		panic("the option must contains 'one_of' or 'column'")
	}
	return
}

func (c *TableApi) generateOptions(table *Table, w *AstData) (findBy, mutable []*ast.Field) {
	if c.Type.HasFindOption() {
		if len(c.FindOptions) > 0 {
			findBy = c.FindOptions.generateFindFields(table, w)
		} else {
			findBy = c.generateIdentifierOption(table, w)
		}
	} else {
		if len(c.FindOptions) > 0 {
			println(fmt.Sprintf("api type `%s` cannot contains `find_by` options", c.Type))
		}
	}
	if c.Type.HasInputOption() {
		if len(c.ModifyColumns) > 0 {
			mutable = make([]*ast.Field, 0, len(c.ModifyColumns))
			for _, columnName := range c.ModifyColumns {
				column := table.Columns.getColumn(columnName)
				field := column.generateField(w, column.Value.Schema.Value.NotNull && (column.Value.Schema.Value.Default == nil))
				mutable = append(mutable, &field)
			}
		} else {
			if c.Type.Operation() == ApiOperationInsert {
				mutable = c.generateInsertable(table, w)
			}
			if c.Type.Operation() == ApiOperationUpdate {
				mutable = c.generateMutable(table, w)
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
	apiBuilder func(*SchemaRef, string, string, []*ast.Field, []*ast.Field, []*ast.Field) AstDataChain
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

func (c *SchemaRef) generateGO(schemaName string, w *AstData) {
	for typeName, typeSchema := range c.Value.Types {
		typeName = c.Value.Name + "." + typeName
		if err := mergeCodeBase(w, typeSchema.generateType(schemaName, typeName)); err != nil {
			panic(err)
		}
	}
	for tableName, table := range c.Value.Tables {
		var (
			structName   = makeExportedName(schemaName + "-" + tableName + "-Row")
			resultFields = table.generateFields(w)
		)
		if err := mergeCodeBase(w, []AstDataChain{
			{
				Types: map[string]*ast.TypeSpec{
					structName: {
						Name: ast.NewIdent(structName),
						Type: &ast.StructType{
							Fields: &ast.FieldList{List: resultFields},
						},
						Comment: builders.MakeComment(utils.StringToSlice(table.Description)),
					},
				},
				Constants:       nil,
				Implementations: nil,
			},
		}); err != nil {
			panic(err)
		}
		if len(table.Api) > 0 {
			for i, api := range table.Api {
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
				var (
					optionFields, mutableFields = api.generateOptions(&table, w)
					builder                     = api.getApiBuilder(apiName)
				)
				if err := mergeCodeBase(w, []AstDataChain{
					builder(c, tableName, structName, optionFields, mutableFields, resultFields),
				}); err != nil {
					panic(err)
				}
			}
		}
	}
}

func GenerateGO(db *Root, schemaName, packageName string, w io.Writer) {
	// we must allow to use type `schema.domain` as known type
	for _, schema := range db.Schemas {
		for domainName, domain := range schema.Value.Domains {
			if domainType, ok := knownTypes[domain.Type]; ok {
				knownTypes[fmt.Sprintf("%s.%s", schema.Value.Name, domainName)] = domainType
			}
		}
	}
	var astData AstData
	for _, schema := range db.Schemas {
		if schemaName == "" || schemaName == schema.Value.Name {
			schema.generateGO(schema.Value.Name, &astData)
		}
	}
	file, fset := astData.makeAstFile(packageName)
	filePrinter := printer.Config{
		Mode:     printer.UseSpaces | printer.TabIndent,
		Tabwidth: 8,
	}
	if err := filePrinter.Fprint(w, fset, file); err != nil {
		panic(err)
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

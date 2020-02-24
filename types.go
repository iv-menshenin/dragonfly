package dragonfly

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iv-menshenin/dragonfly/code_builders"
	"github.com/iv-menshenin/dragonfly/utils"
	"os"
	"reflect"
	"strconv"
	"strings"
)

const (
	// root elements
	components = "components"
	schemas    = "schemas"

	// schemas
	types   = "types"
	domains = "domains"
	tables  = "tables"

	// components
	columns     = "columns"
	classes     = "classes"
	constraints = "constraints"

	pathToDomainTemplate = "#/schemas/%s/domains/%s"
)

var (
	elements = []string{components, schemas, types, domains, tables, columns, classes}
)

type (
	TypeBase struct {
		Type      string `yaml:"type" json:"type"`
		Length    *int   `yaml:"length,omitempty" json:"length,omitempty"`
		Precision *int   `yaml:"precision,omitempty" json:"precision,omitempty"`
	}
	TypeSchema struct {
		TypeBase `yaml:"-,inline" json:"-,inline"`
		// for type `enum` only
		Enum []EnumEntity `yaml:"enum,omitempty" json:"enum,omitempty"`
		// for types `record` and `json`
		Fields ColumnsContainer `yaml:"fields,omitempty" json:"fields,omitempty"`
		// for type `map`
		KeyType   *ColumnSchemaRef `yaml:"key_type,omitempty" json:"key_type,omitempty"`
		ValueType *ColumnSchemaRef `yaml:"value_type,omitempty" json:"value_type,omitempty"`
		used      *bool
	}
	DomainSchema struct { // TODO DOMAIN CONSTRAINTS NAME (CHECK/NOT NULL)
		TypeBase `yaml:"-,inline" json:"-,inline"`
		NotNull  bool    `yaml:"not_null,omitempty" json:"not_null,omitempty"`
		Default  *string `yaml:"default,omitempty" json:"default,omitempty"`
		Check    *string `yaml:"check,omitempty" json:"check,omitempty"`
		used     *bool
	}
	EnumEntity struct {
		Value       string `yaml:"value" json:"value"`
		Description string `yaml:"description,omitempty" json:"description,omitempty"`
	}
	ColumnSchemaRef struct {
		Value DomainSchema `yaml:"value,inline" json:"value,inline"`
		Ref   *string      `yaml:"$ref,omitempty" json:"$ref,omitempty"`
	}
	Column struct {
		Name        string          `yaml:"name" json:"name"`
		Schema      ColumnSchemaRef `yaml:"schema" json:"schema"`
		Constraints []Constraint    `yaml:"constraints,omitempty" json:"constraints,omitempty"`
		Tags        []string        `yaml:"tags,omitempty" json:"tags,omitempty"`
		Description string          `yaml:"description,omitempty" json:"description,omitempty"`
	}
	ColumnRef struct {
		Value Column  `yaml:"value,inline" json:"value,inline"`
		Ref   *string `yaml:"$ref,omitempty" json:"$ref,omitempty"`
		used  *bool
	}
	// constraint parameters
	// TODO interface IConstraintParameter
	ForeignKey struct {
		ToTable  string  `yaml:"table" json:"table"`
		ToColumn string  `yaml:"column" json:"column"`
		OnUpdate *string `yaml:"on_update,omitempty" json:"on_update,omitempty"`
		OnDelete *string `yaml:"on_delete,omitempty" json:"on_delete,omitempty"`
	}
	Check struct {
		Expression string `yaml:"expression" json:"expression"`
	}
	// deprecated
	Where struct {
		Where string `yaml:"where" json:"where"`
	}
	// ForeignKey, Check, Where
	ConstraintParameters struct {
		Parameter interface{} `yaml:"value,inline" json:"value,inline"`
	}
	ConstraintType int
	Constraint     struct {
		Name       string               `yaml:"name" json:"name"`
		Type       ConstraintType       `yaml:"type" json:"type"`
		Parameters ConstraintParameters `yaml:"parameters,omitempty" json:"parameters,omitempty"`
		used       *bool
	}
	ConstraintSchema struct {
		Columns    []string   `yaml:"columns" json:"columns"`
		Constraint Constraint `yaml:"constraint" json:"constraint"`
	}
	IndexType   int
	IndexColumn struct {
		Name     string `yaml:"name" json:"name"`
		Type     string `yaml:"type,omitempty" json:"type,omitempty"`
		Function string `yaml:"function,omitempty" function:"type,omitempty"`
	}
	Index struct {
		Name      string        `yaml:"name" json:"name"`
		IndexType IndexType     `yaml:"type" json:"type"`
		Columns   []IndexColumn `yaml:"columns" json:"columns"`
		Where     string        `yaml:"where" json:"where"`
	}
	ApiFindOption struct {
		Column   string                          `yaml:"column,omitempty" json:"column,omitempty"`
		Required bool                            `yaml:"required,omitempty" json:"required,omitempty"`
		OneOf    []ApiFindOption                 `yaml:"one_of,omitempty" json:"one_of,omitempty"`
		Operator builders.SQLDataCompareOperator `yaml:"operator,omitempty" json:"operator,omitempty"`
	}
	ApiFindOptions []ApiFindOption
	TableApi       struct {
		Type          ApiType        `yaml:"type" json:"type"`
		Name          string         `yaml:"name" json:"name"`
		FindOptions   ApiFindOptions `yaml:"find_by,omitempty" json:"find_by,omitempty"`
		ModifyColumns []string       `yaml:"modify,omitempty" json:"modify,omitempty"`
	}
	ColumnsContainer []ColumnRef
	IndicesContainer []Index
	ApiContainer     []TableApi
	TableConstraints []ConstraintSchema
	Table            struct {
		Inherits    []string         `yaml:"inherits,omitempty" json:"inherits,omitempty"`
		Columns     ColumnsContainer `yaml:"columns" json:"columns"`
		Constraints TableConstraints `yaml:"constraints,omitempty" json:"constraints,omitempty"`
		Indices     IndicesContainer `yaml:"indices,omitempty" json:"indices,omitempty"`
		Description string           `yaml:"description,omitempty" json:"description,omitempty"`
		Api         ApiContainer     `yaml:"api,omitempty" json:"api,omitempty"`
		used        *bool
	}
	TableClass struct {
		Columns     ColumnsContainer `yaml:"columns" json:"columns"`
		Constraints TableConstraints `yaml:"constraints,omitempty" json:"constraints,omitempty"`
		Api         ApiContainer     `yaml:"api,omitempty" json:"api,omitempty"`
	}
	DomainsContainer map[string]DomainSchema
	TypesContainer   map[string]TypeSchema
	TablesContainer  map[string]Table
	Schema           struct {
		Name    string           `yaml:"name" json:"name"`
		Types   TypesContainer   `yaml:"types,omitempty" json:"types,omitempty"`
		Domains DomainsContainer `yaml:"domains,omitempty" json:"domains,omitempty"`
		Tables  TablesContainer  `yaml:"tables,omitempty" json:"tables,omitempty"`
	}
	SchemaRef struct {
		Value Schema  `yaml:"value,inline" json:"value,inline"`
		Ref   *string `yaml:"$ref,omitempty" json:"$ref,omitempty"`
	}
	Components struct {
		Columns map[string]Column     `yaml:"columns" json:"columns"`
		Classes map[string]TableClass `yaml:"classes" json:"classes"`
	}
	Schemas []SchemaRef
	Root    struct {
		Schemas Schemas `yaml:"schemas" json:"schemas"`
		// important: avoid getting any components directly, they are not normalized
		Components Components `yaml:"components" json:"components"`
	}
)

const (
	IndexTypeIndex IndexType = iota + 1
	IndexTypeUnique
)

var (
	indexTypes = map[string]IndexType{
		"index":  IndexTypeIndex,
		"unique": IndexTypeUnique,
	}
)

func (c *IndexType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	if t, ok := indexTypes[strings.ToLower(s)]; ok {
		*c = t
		return nil
	}
	return errors.New("cannot resolve index type '" + s + "'")
}

func (c *IndexType) UnmarshalJSON(data []byte) error {
	var s = string(data)
	if t, ok := indexTypes[strings.ToLower(s)]; ok {
		*c = t
	}
	return errors.New("cannot resolve index type '" + s + "'")
}

func (c *TypeSchema) generateType(schema, typeName string) []AstDataChain {
	typeName = makeExportedName(typeName)
	var describer fieldDescriber
	switch c.Type {
	case "record":
		describer = makeRecordDescriberDirectly(typeName, c)
	case "json":
		describer = makeJsonDescriberDirectly(typeName, c)
	case "map":
		describer = makeMapDescriberDirectly(typeName, c)
	case "enum":
		describer = makeEnumDescriberDirectly(typeName, c)
	default:
		panic(fmt.Sprintf("unknown type '%s' for '%s'", c.Type, typeName))
	}
	return describer.getFile()
}

const (
	ConstraintPrimaryKey ConstraintType = iota + 1
	ConstraintForeignKey
	ConstraintUniqueKey
	ConstraintCheck
)

var (
	constraintReference = map[string]ConstraintType{
		"primary key": ConstraintPrimaryKey,
		"foreign key": ConstraintForeignKey,
		"unique key":  ConstraintUniqueKey,
		"primary":     ConstraintPrimaryKey,
		"foreign":     ConstraintForeignKey,
		"unique":      ConstraintUniqueKey,
		"check":       ConstraintCheck,
	}
)

func splitPath(path string) (result map[string]string) {
	pathSmt := strings.Split(path, "/")
	result = make(map[string]string, len(pathSmt))
	var pass = false
	for i, s := range pathSmt {
		if pass {
			pass = false
			continue
		}
		if utils.ArrayContainsCI(elements, s) {
			if i+1 < len(pathSmt) {
				result[s] = pathSmt[i+1]
			}
			pass = true
			continue
		}
	}
	return
}

func (c ColumnSchemaRef) makeCustomType() (string, string, bool) {
	if c.Ref == nil {
		return "", "", false
	}
	pathSmt := splitPath(*c.Ref)
	schema, okSchema := pathSmt[schemas]
	if okSchema {
		if customType, isCustom := pathSmt[domains]; isCustom {
			return schema, customType, isCustom
		} else if customType, isCustom := pathSmt[types]; isCustom {
			return schema, customType, isCustom
		}
	}
	return "", "", false
}

func (c Schemas) tryToFind(name string) (*SchemaRef, bool) {
	for i, schema := range c {
		if strings.EqualFold(schema.Value.Name, name) {
			return &c[i], true
		}
	}
	return nil, false
}

func (c *Root) getComponentColumn(name string) (*Column, bool) {
	column, ok := c.Components.Columns[name]
	if !ok {
		return nil, false
	}
	if column.Schema.Ref != nil {
		processRef(c, *column.Schema.Ref, &column.Schema.Value)
	}
	return &column, true
}

func (c *Root) getComponentClass(schema *SchemaRef, tableName string, name string) (*TableClass, bool) {
	class, ok := c.Components.Classes[name]
	if !ok {
		return nil, false
	}
	return &class, true
}

func (c *ConstraintParameters) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var foreign ForeignKey
	if unmarshal(&foreign) == nil {
		c.Parameter = foreign
		return nil
	}
	var check Check
	if unmarshal(&check) == nil {
		c.Parameter = check
		return nil
	}
	var where Where
	if unmarshal(&where) == nil {
		c.Parameter = where
		return nil
	}
	return errors.New("cannot resolve parameter type")
}

func (c *ConstraintParameters) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var foreign ForeignKey
	if decoder.Decode(&foreign) == nil {
		c.Parameter = foreign
		return nil
	}
	var check Check
	if decoder.Decode(&check) == nil {
		c.Parameter = check
		return nil
	}
	var where Where
	if decoder.Decode(&where) == nil {
		c.Parameter = where
		return nil
	}
	return errors.New("cannot resolve parameter type")
}

func (c *ConstraintType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var dataStr string
	if err := unmarshal(&dataStr); err != nil {
		return err
	}
	if constraintType, ok := constraintReference[strings.ToLower(dataStr)]; !ok {
		return errors.New(fmt.Sprintf("cannot resolve constraint type %s", dataStr))
	} else {
		*c = constraintType
		return nil
	}
}

func (c *ConstraintType) UnmarshalJSON(data []byte) error {
	dataStr := strings.ToLower(string(data))
	if constraintType, ok := constraintReference[dataStr]; !ok {
		return errors.New(fmt.Sprintf("cannot resolve constraint type %s", dataStr))
	} else {
		*c = constraintType
		return nil
	}
}

func (c ConstraintType) String() string {
	for key, value := range constraintReference {
		if value == c {
			return key
		}
	}
	return "unknown"
}

func processRef(db *Root, ref string, i interface{}) {
	if ref == "" {
		panic(errors.New("cannot resolve empty $ref"))
	}
	if chains := strings.Split(strings.TrimSpace(ref), " "); len(chains) > 1 {
		if chains[0] == "!include" {
			fileName := strings.TrimSpace(strings.Join(chains[1:], " "))
			readAndParseFile(fileName, i)
			return
		}
	}
	if ref[0] == '#' {
		chains := strings.Split(ref, "/")[1:]
		if !db.follow(db, chains, i) {
			panic(errors.New("cannot resolve $ref: '" + ref + "'"))
		}
	}
}

func copyFromTo(from, to interface{}) {
	fromV, toV := reflect.ValueOf(from), reflect.ValueOf(to)
	// first we must get 'to' variable from link
	toV = toV.Elem()
	if !toV.CanSet() {
		if toV.CanAddr() {
			toV = toV.Addr()
		}
	}
	if fromV.Type().Kind() == reflect.Ptr {
		fromV = fromV.Elem()
	}
	toV.Set(fromV)
}

func (c *Column) follow(db *Root, path []string, i interface{}) bool {
	if len(path) == 0 {
		// can panic
		copyFromTo(c, i)
		return true
	}
	return false
}

const (
	cNN            = "Num"
	cIndex         = "Index"
	cColumnIndex   = "ColumnIndex"
	cColumn        = "Column"
	cTable         = "Table"
	cSchema        = "Schema"
	cForeignTable  = "ForeignTable"
	cForeignColumn = "ForeignColumn"
	cApiType       = "ApiType"
)

func (c *ColumnRef) normalize(schema *SchemaRef, relationName string, columnIndex int, db *Root) {
	c.used = utils.RefBool(false)
	if c.Ref != nil {
		processRef(db, *c.Ref, &c.Value)
	}
	if c.Value.Name == "" {
		panic(fmt.Sprintf("undefined name for table '%s' column #%d", relationName, columnIndex+1))
	}
	constraints := make([]Constraint, len(c.Value.Constraints))
	reflect.Copy(reflect.ValueOf(constraints), reflect.ValueOf(c.Value.Constraints))
	for i, constraint := range constraints {
		constraint.normalize(schema, relationName, i, db)
		constraints[i] = constraint
	}
	if !reflect.DeepEqual(constraints, c.Value.Constraints) {
		c.Value.Constraints = constraints
	}
	c.Value.Schema.normalize(relationName, columnIndex, db)
}

func (c *Constraint) normalize(schema *SchemaRef, tableName string, constraintIndex int, db *Root) {
	c.used = utils.RefBool(false)
	constraintNameDefault := map[ConstraintType]string{
		ConstraintPrimaryKey: fmt.Sprintf("pk_{%%%s}_{%%%s}", cSchema, cTable),
		ConstraintForeignKey: fmt.Sprintf("fk_{%%%s}_{%%%s}_{%%%s}", cSchema, cTable, cForeignTable),
		ConstraintUniqueKey:  fmt.Sprintf("ux_{%%%s}_{%%%s}_{%%%s}", cSchema, cTable, cNN),
		ConstraintCheck:      fmt.Sprintf("ch_{%%%s}_{%%%s}_{%%%s}", cSchema, cTable, cNN),
	}
	if c.Name == "" {
		var ok bool
		if c.Name, ok = constraintNameDefault[c.Type]; !ok {
			panic(fmt.Sprintf("cannot resolve constraint #%d type for table %s", constraintIndex, tableName))
		}
	}
	foreignTable := ""
	if fk, ok := c.Parameters.Parameter.(ForeignKey); ok {
		foreignTable = fk.ToTable
	}
	c.Name = utils.EvalTemplateParameters(c.Name, map[string]string{
		cTable:        tableName,
		cSchema:       schema.Value.Name,
		cForeignTable: strings.Replace(foreignTable, ".", "_", -1),
		cIndex:        strconv.Itoa(constraintIndex),
		cNN:           strconv.Itoa(constraintIndex),
	})
}

func (c *ConstraintSchema) normalize(schema *SchemaRef, tableName string, constraintIndex int, db *Root) {
	c.Constraint.normalize(schema, tableName, constraintIndex, db)
}

func (c *ColumnSchemaRef) normalize(tableName string, columnIndex int, db *Root) {
	c.Value.used = utils.RefBool(false)
	if c.Ref != nil {
		processRef(db, *c.Ref, &c.Value)
	}
	if c.Value.Type == "" {
		panic(fmt.Sprintf("undefined data type for table '%s' column #%d", tableName, columnIndex+1))
	}
}

func (c ApiContainer) exists(name string) bool {
	for _, api := range c {
		if api.Name == name {
			return true
		}
	}
	return false
}

func (c ApiContainer) normalize(schema *SchemaRef, tableName string, db *Root) {
	var names = make(map[string]bool, len(c))
	for i, api := range c {
		api.normalize(schema, tableName, i, db)
		if _, ok := names[api.Name]; ok {
			panic(fmt.Sprintf("duplicated api name `%s` in table `%s`", api.Name, tableName))
		}
		names[api.Name] = true
		c[i] = api
	}
}

func (c TableConstraints) exists(name string) bool {
	for _, constraint := range c {
		if strings.EqualFold(constraint.Constraint.Name, name) {
			return true
		}
	}
	return false
}

func (c TableConstraints) tryToFind(name string) (*ConstraintSchema, bool) {
	for i, constraint := range c {
		if strings.EqualFold(constraint.Constraint.Name, name) {
			return &c[i], true
		}
	}
	return nil, false
}

func (c TableConstraints) normalize(schema *SchemaRef, tableName string, db *Root) {
	var names = make(map[string]bool, len(c))
	for i, constraint := range c {
		constraint.normalize(schema, tableName, i, db)
		if _, ok := names[constraint.Constraint.Name]; ok {
			panic(fmt.Sprintf("duplicated constraint name `%s` in table `%s`", constraint.Constraint.Name, tableName))
		}
		names[constraint.Constraint.Name] = true
		c[i] = constraint
	}
}

func (c TablesContainer) tryToFind(name string) (*Table, bool) {
	for tableName, table := range c {
		if strings.EqualFold(name, tableName) {
			return &table, true
		}
	}
	return nil, false
}

func (c ColumnsContainer) exists(name string) bool {
	_, found := c.tryToFind(name)
	return found
}

func (c ColumnsContainer) tryToFind(name string) (*ColumnRef, bool) {
	for i, column := range []ColumnRef(c) {
		if strings.EqualFold(column.Value.Name, name) {
			return &[]ColumnRef(c)[i], true
		}
	}
	return nil, false
}

func (c ColumnsContainer) normalize(schema *SchemaRef, tableName string, db *Root) {
	var names = make(map[string]bool, len(c))
	for i, column := range c {
		column.normalize(schema, tableName, i, db)
		if _, ok := names[column.Value.Name]; ok {
			panic(fmt.Sprintf("duplicated column name `%s` in table `%s`", column.Value.Name, tableName))
		}
		names[column.Value.Name] = true
		c[i] = column
	}
}

func (c ColumnsContainer) getColumn(name string) ColumnRef {
	if columnRef, found := c.tryToFind(name); found {
		return *columnRef
	}
	panic(fmt.Sprintf("cannot find column '%s'", name))
}

func (c ColumnsContainer) CopyFlatColumns() ColumnsContainer {
	result := make([]ColumnRef, 0, len(c))
	for _, column := range c {
		result = append(result, ColumnRef{
			Value: Column{
				Name: column.Value.Name,
				Schema: ColumnSchemaRef{
					Value: DomainSchema{
						TypeBase: TypeBase{
							Type:      column.Value.Schema.Value.Type,
							Length:    column.Value.Schema.Value.Length,
							Precision: column.Value.Schema.Value.Precision,
						},
						NotNull: column.Value.Schema.Value.NotNull,
						Default: column.Value.Schema.Value.Default,
					},
					Ref: column.Value.Schema.Ref,
				},
				Constraints: nil,
				Tags:        nil,
				Description: "copied from original",
			},
			Ref: column.Ref,
		})
	}
	return result
}

func (c *TableClass) follow(db *Root, path []string, i interface{}) bool {
	return c.makeTable().follow(db, path, i)
}

func (c *TableClass) makeTable() *Table {
	return &Table{
		Columns:     c.Columns,
		Constraints: c.Constraints,
		Api:         c.Api,
	}
}

func (c *Table) follow(db *Root, path []string, i interface{}) bool {
	if len(path) == 0 {
		// can panic
		copyFromTo(c, i)
		return true
	}
	if len(path) > 1 {
		if path[0] == columns {
			column := c.Columns.getColumn(path[1])
			copyFromTo(column.Value, i)
			return true
		}
		if path[0] == constraints {
			constraint, ok := c.Constraints.tryToFind(path[1])
			if ok {
				copyFromTo(constraint, i)
				return true
			}
			return false
		}
	}
	return false
}

func (c *Table) normalize(schema *SchemaRef, tableName string, db *Root) {
	c.used = utils.RefBool(false)
	inheritColumns := make(ColumnsContainer, 0, 10)
	inheritConstraints := make(TableConstraints, 0, 10)
	inheritApis := make(ApiContainer, 0, 10)
	for _, class := range c.Inherits {
		classSchema, ok := db.getComponentClass(schema, tableName, class)
		if !ok {
			panic(fmt.Sprintf("the class component '%s' is not exists", class))
		}
		inheritColumns = append(inheritColumns, classSchema.Columns...)
		inheritConstraints = append(inheritConstraints, classSchema.Constraints...)
		inheritApis = append(inheritApis, classSchema.Api...)
	}
	/* merging */
	c.Columns = append(make(ColumnsContainer, len(inheritColumns), len(inheritColumns)+len(c.Columns)), c.Columns...)
	reflect.Copy(reflect.ValueOf(c.Columns[:len(inheritColumns)]), reflect.ValueOf(inheritColumns))
	c.Constraints = append(make(TableConstraints, len(inheritConstraints), len(inheritConstraints)+len(c.Constraints)), c.Constraints...)
	reflect.Copy(reflect.ValueOf(c.Constraints[:len(inheritConstraints)]), reflect.ValueOf(inheritConstraints))
	c.Api = append(make(ApiContainer, len(inheritApis), len(inheritApis)+len(c.Api)), c.Api...)
	reflect.Copy(reflect.ValueOf(c.Api[:len(inheritApis)]), reflect.ValueOf(inheritApis))
	/* normalization */
	c.Columns.normalize(schema, tableName, db)
	c.Constraints.normalize(schema, tableName, db)
	c.Api.normalize(schema, tableName, db)
}

func (c *TableApi) normalize(schema *SchemaRef, tableName string, apiIndex int, db *Root) {
	if c.Name == "" {
		c.Name = fmt.Sprintf("{%%%s}_{%%%s}_{%%%s}", cSchema, cTable, cApiType)
	}
	// TODO test
	c.Name = utils.EvalTemplateParameters(c.Name, map[string]string{
		cApiType: c.Type.String(),
		cTable:   tableName,
		cSchema:  schema.Value.Name,
		cIndex:   strconv.Itoa(apiIndex),
		cNN:      strconv.Itoa(apiIndex),
	})
}

func (c *SchemaRef) normalize(db *Root) {
	for typeName, customType := range c.Value.Types {
		customType.normalize(c, typeName, db)
		c.Value.Types[typeName] = customType
	}
	for i, domain := range c.Value.Domains {
		domain.used = utils.RefBool(false)
		c.Value.Domains[i] = domain
	}
	for tableName, table := range c.Value.Tables {
		table.normalize(c, tableName, db)
		c.Value.Tables[tableName] = table
	}
}

func (c *TypeSchema) normalize(schema *SchemaRef, typeName string, db *Root) {
	c.used = utils.RefBool(false)
	for i, f := range c.Fields {
		f.normalize(schema, typeName, i, db)
		f.used = utils.RefBool(false)
		c.Fields[i] = f
	}
}

func (c *SchemaRef) follow(db *Root, path []string, i interface{}) (ok bool) {
	defer func() {
		err := recover()
		if err != nil {
			ok = false
			if _, e := fmt.Fprintf(os.Stderr, "<%T>: %s", err, err); e != nil {
				panic(e)
			}
		}
	}()
	if len(path) == 0 {
		// can panic
		copyFromTo(c, i)
		return true
	}
	if len(path) < 2 {
		return false
	}
	switch path[0] {
	case types:
		if vType, ok := c.Value.Types[path[1]]; ok {
			return vType.follow(db, path[2:], i)
		}
	case domains:
		if domain, ok := c.Value.Domains[path[1]]; ok {
			return domain.follow(db, path[2:], i)
		}
	case tables:
		if table, ok := c.Value.Tables[path[1]]; ok {
			return table.follow(db, path[2:], i)
		}
	}
	return false
}

func (c *DomainSchema) follow(db *Root, path []string, i interface{}) (ok bool) {
	defer func() {
		err := recover()
		if err != nil {
			ok = false
			if _, e := fmt.Fprintf(os.Stderr, "<%T>: %s", err, err); e != nil {
				panic(e)
			}
		}
	}()
	if len(path) == 0 {
		// can panic
		copyFromTo(c, i)
		return true
	}
	return false
}

func (c *TypeSchema) follow(db *Root, path []string, i interface{}) (ok bool) {
	defer func() {
		err := recover()
		if err != nil {
			ok = false
			if _, e := fmt.Fprintf(os.Stderr, "<%T>: %s", err, err); e != nil {
				panic(e)
			}
		}
	}()
	if len(path) == 0 {
		// can panic
		copyFromTo(c, i)
		return true
	}
	return false
}

func (c *Root) follow(db *Root, path []string, i interface{}) (ok bool) {
	defer func() {
		err := recover()
		if err != nil {
			ok = false
			if _, e := fmt.Fprintf(os.Stderr, "<%T>: %s", err, err); e != nil {
				panic(e)
			}
		}
	}()
	if len(path) < 2 {
		return false
	}
	switch path[0] {
	case schemas:
		for _, schema := range c.Schemas {
			if path[1] == schema.Value.Name {
				return schema.follow(db, path[2:], i)
			}
		}
	case components:
		if len(path) < 3 {
			return false
		}
		switch path[1] {
		case columns:
			if column, ok := c.getComponentColumn(path[2]); ok {
				return column.follow(db, path[3:], i)
			}
		case classes:
			if class, ok := c.getComponentClass(nil, "", path[2]); ok {
				return class.follow(db, path[3:], i)
			}
		}
	}
	return false
}

func (c *Root) normalize() {
	// avoid of breaking links to types
	for i, schemaRef := range c.Schemas {
		if schemaRef.Ref != nil {
			processRef(c, *schemaRef.Ref, &schemaRef.Value)
			c.Schemas[i] = schemaRef
		}
	}
	for i, schemaRef := range c.Schemas {
		schemaRef.normalize(c)
		c.Schemas[i] = schemaRef
	}
	// do not normalize components: it contains supporting data for the project file itself,
	// but not for the database schema
}

package dragonfly

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
	columns = "columns"
	classes = "classes"
)

type (
	DomainSchema struct { // TODO DOMAIN CONSTRAINTS NAME (CHECK/NOT NULL)
		Type      string  `yaml:"type" json:"type"`
		Length    *int    `yaml:"length,omitempty" json:"length,omitempty"`
		Precision *int    `yaml:"precision,omitempty" json:"precision,omitempty"`
		NotNull   bool    `yaml:"not_null,omitempty" json:"not_null,omitempty"`
		Default   *string `yaml:"default,omitempty" json:"default,omitempty"`
		Check     *string `yaml:"check,omitempty" json:"check,omitempty"`
		// for type `enum` only
		Enum []EnumEntity `yaml:"enum,omitempty" json:"enum,omitempty"`
		// for types `record` and `json`
		Fields []Column `yaml:"fields,omitempty" json:"fields,omitempty"`
		// for type `map`
		KeyType   *ColumnSchemaRef `yaml:"key_type,omitempty" json:"key_type,omitempty"`
		ValueType *ColumnSchemaRef `yaml:"value_type,omitempty" json:"value_type,omitempty"`
	}
	EnumEntity struct {
		Value       string `yaml:"value" json:"value"`
		Description string `yaml:"description,omitempty" json:"description,omitempty"`
	}
	ColumnSchemaRef struct {
		Value DomainSchema `yaml:"-,inline" json:"-,inline"`
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
		Value Column  `yaml:"-,inline" json:"-,inline"`
		Ref   *string `yaml:"$ref,omitempty" json:"$ref,omitempty"`
	}
	ForeignKey struct {
		ToTable  string  `yaml:"table" json:"table"`
		ToColumn string  `yaml:"column" json:"column"`
		OnUpdate *string `yaml:"on_update,omitempty" json:"on_update,omitempty"`
		OnDelete *string `yaml:"on_delete,omitempty" json:"on_delete,omitempty"`
	}
	Check struct {
		Expression string `yaml:"expression" json:"expression"`
	}
	Where struct {
		Where string `yaml:"where" json:"where"`
	}
	// ForeignKey, Check
	ConstraintParameters struct {
		Parameter interface{} `yaml:"-,inline" json:"-,inline"`
	}
	Constraint struct {
		Name       string               `yaml:"name" json:"name"`
		Type       string               `yaml:"type" json:"type"`
		Parameters ConstraintParameters `yaml:"parameters,omitempty" json:"parameters,omitempty"`
	}
	ConstraintSchema struct {
		Columns    []string   `yaml:"columns" json:"columns"`
		Constraint Constraint `yaml:"constraint" json:"constraint"`
	}
	ApiFindOption struct {
		Column   string             `yaml:"column,omitempty" json:"column,omitempty"`
		Required bool               `yaml:"required,omitempty" json:"required,omitempty"`
		OneOf    []ApiFindOption    `yaml:"one_of,omitempty" json:"one_of,omitempty"`
		Operator sqlCompareOperator `yaml:"operator,omitempty" json:"operator,omitempty"`
	}
	ApiFindOptions []ApiFindOption
	TableApi       struct {
		Type          ApiType        `yaml:"type" json:"type"`
		Name          string         `yaml:"name" json:"name"`
		FindOptions   ApiFindOptions `yaml:"find_by,omitempty" json:"find_by,omitempty"`
		ModifyColumns []string       `yaml:"modify,omitempty" json:"modify,omitempty"`
	}
	ColumnsContainer []ColumnRef
	ApiContainer     []TableApi
	TableConstraints []ConstraintSchema
	TableClass       struct {
		Inherits    []string         `yaml:"inherits,omitempty" json:"inherits,omitempty"`
		Columns     ColumnsContainer `yaml:"columns" json:"columns"`
		Constraints TableConstraints `yaml:"constraints,omitempty" json:"constraints,omitempty"`
		Description string           `yaml:"description,omitempty" json:"description,omitempty"`
		Api         ApiContainer     `yaml:"api,omitempty" json:"api,omitempty"`
	}
	Schema struct {
		Name    string                  `yaml:"name" json:"name"`
		Types   map[string]DomainSchema `yaml:"types,omitempty" json:"types,omitempty"`
		Domains map[string]DomainSchema `yaml:"domains,omitempty" json:"domains,omitempty"`
		Tables  map[string]TableClass   `yaml:"tables,omitempty" json:"tables,omitempty"`
	}
	SchemaRef struct {
		Value Schema  `yaml:"-,inline" json:"-,inline"`
		Ref   *string `yaml:"$ref,omitempty" json:"$ref,omitempty"`
	}
	Components struct {
		Columns map[string]Column     `yaml:"columns" json:"columns"`
		Classes map[string]TableClass `yaml:"classes" json:"classes"`
	}
	Root struct {
		Schemas []SchemaRef `yaml:"schemas" json:"schemas"`
		// important: avoid getting any components directly, they are not normalized
		Components Components `yaml:"components" json:"components"`
	}
)

func (c ColumnSchemaRef) makeDomainName() (string, bool) {
	if c.Ref == nil {
		return "", false
	}
	pathSmt := strings.Split(*c.Ref, "/")
	return pathSmt[len(pathSmt)-1], pathSmt[len(pathSmt)-1] != ""
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
	if name == "stdRef" {
		name = "stdRef"
	}
	class, ok := c.Components.Classes[name]
	if !ok {
		return nil, false
	}
	if class.Inherits != nil {
		panic(fmt.Sprintf("the class '%s' cannot have 'inherits', multi-level inheritance is not supported", name))
	}
	if schema != nil {
		class.normalize(schema, tableName, c)
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

func (c *ColumnRef) normalize(schema *SchemaRef, tableName string, columnIndex int, db *Root) {
	if c.Ref != nil {
		processRef(db, *c.Ref, &c.Value)
	}
	if c.Value.Name == "" {
		panic(fmt.Sprintf("undefined name for table '%s' column #%d", tableName, columnIndex+1))
	}
	c.Value.Schema.normalize(schema, tableName, columnIndex, db)
}

const (
	cNN            = "Num"
	cTable         = "Table"
	cSchema        = "Schema"
	cForeignTable  = "ForeignTable"
	cForeignColumn = "ForeignColumn"
	cApiType       = "ApiType"
)

func (c *ConstraintSchema) normalize(schema *SchemaRef, tableName string, constraintIndex int, db *Root) {
	constraintNameDefault := map[string]string{
		"primary key": "pk_{%Schema}_{%Table}",
		"foreign key": "fk_{%Schema}_{%Table}_{%ForeignTable}",
		"unique":      "ux_{%Schema}_{%Table}_{%Num}",
		"default":     "ct_{%Schema}_{%Table}_{%Num}",
	}
	if c.Constraint.Name == "" {
		var ok bool
		if c.Constraint.Name, ok = constraintNameDefault[strings.ToLower(c.Constraint.Type)]; !ok {
			c.Constraint.Name = constraintNameDefault["default"]
		}
	}
}

func (c *ColumnSchemaRef) normalize(schema *SchemaRef, tableName string, columnIndex int, db *Root) {
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

func (c TableConstraints) exists(name string) bool {
	for _, constraint := range c {
		if constraint.Constraint.Name == name {
			return true
		}
	}
	return false
}

func (c ColumnsContainer) exists(name string) bool {
	for _, column := range c {
		if column.Value.Name == name {
			return true
		}
	}
	return false
}

func (c ColumnsContainer) find(name string) ColumnRef {
	for i, column := range []ColumnRef(c) {
		if column.Value.Name == name {
			return []ColumnRef(c)[i]
		}
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
						Type:      column.Value.Schema.Value.Type,
						Length:    column.Value.Schema.Value.Length,
						Precision: column.Value.Schema.Value.Precision,
						NotNull:   column.Value.Schema.Value.NotNull,
						Default:   column.Value.Schema.Value.Default,
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
	if len(path) == 0 {
		// can panic
		copyFromTo(c, i)
		return true
	}
	if len(path) > 1 {
		if path[0] == columns {
			column := c.Columns.find(path[1])
			copyFromTo(column.Value, i)
		}
		// TODO ??
	}
	return false
}

func (c *TableClass) normalize(schema *SchemaRef, tableName string, db *Root) {
	for i, column := range c.Columns {
		column.normalize(schema, tableName, i, db)
		c.Columns[i] = column
	}
	for i, constraint := range c.Constraints {
		constraint.normalize(schema, tableName, i, db)
		c.Constraints[i] = constraint
	}
	for i, api := range c.Api {
		api.normalize(schema, tableName, i, db)
		c.Api[i] = api
	}
	for _, class := range c.Inherits {
		classSchema, ok := db.getComponentClass(schema, tableName, class)
		if !ok {
			panic(fmt.Sprintf("the class component '%s' is not exists", class))
		}
		for _, column := range classSchema.Columns {
			if c.Columns.exists(column.Value.Name) {
				panic(fmt.Sprintf("cannot inherit '%s' class. column '%s' already exists in table '%s'", class, column.Value.Name, tableName))
			}
			c.Columns = append(c.Columns, column)
		}
		for _, api := range classSchema.Api {
			c.Api = append(c.Api, api)
		}
		for _, constraint := range classSchema.Constraints {
			c.Constraints = append(c.Constraints, constraint)
		}
	}
}

func (c *TableApi) normalize(schema *SchemaRef, tableName string, apiIndex int, db *Root) {
	if c.Name == "" {
		c.Name = "{%Schema}_{%Table}_{%ApiType}"
	}
}

func (c *SchemaRef) normalize(db *Root) {
	for tableName, table := range c.Value.Tables {
		table.normalize(c, tableName, db)
		c.Value.Tables[tableName] = table
	}
}

func (c *SchemaRef) follow(db *Root, path []string, i interface{}) bool {
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

func (c *DomainSchema) follow(db *Root, path []string, i interface{}) bool {
	if len(path) == 0 {
		// can panic
		copyFromTo(c, i)
		return true
	}
	return false
}

func (c *Root) follow(db *Root, path []string, i interface{}) bool {
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
}

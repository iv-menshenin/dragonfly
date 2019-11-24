package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const (
	// root elements
	components = "components"
	schemas    = "schemas"

	// schemas
	domains = "domains"
	tables  = "tables"

	// components
	columns = "columns"
	classes = "classes"
)

type (
	DomainSchema struct {
		Type      string  `yaml:"type" json:"type"`
		Length    *int    `yaml:"length,omitempty" json:"length,omitempty"`
		Precision *int    `yaml:"precision,omitempty" json:"precision,omitempty"`
		NotNull   bool    `yaml:"not_null,omitempty" json:"not_null,omitempty"`
		Default   *string `yaml:"default,omitempty" json:"default,omitempty"`
		Check     *string `yaml:"check,omitempty" json:"check,omitempty"`
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
	ApiOption struct {
		Field    string             `yaml:"field,omitempty" json:"field,omitempty"`
		Required bool               `yaml:"required,omitempty" json:"required,omitempty"`
		OneOf    []ApiOption        `yaml:"oneOf,omitempty" json:"oneOf,omitempty"`
		Operator SqlCompareOperator `yaml:"operator,omitempty" json:"operator,omitempty"`
	}
	TableApi struct {
		Type    string      `yaml:"type" json:"type"`
		Name    string      `yaml:"name" json:"name"`
		Options []ApiOption `yaml:"options,omitempty" json:"options,omitempty"`
	}
	Table struct {
		Columns     []ColumnRef        `yaml:"columns" json:"columns"`
		Constraints []ConstraintSchema `yaml:"constraints,omitempty" json:"constraints,omitempty"`
		Description *string            `yaml:"description,omitempty" json:"description,omitempty"`
		Api         []TableApi         `yaml:"api,omitempty" json:"api,omitempty"`
	}
	Schema struct {
		Name    string                  `yaml:"name" json:"name"`
		Domains map[string]DomainSchema `yaml:"domains,omitempty" json:"domains,omitempty"`
		Tables  map[string]Table        `yaml:"tables,omitempty" json:"tables,omitempty"`
	}
	SchemaRef struct {
		Value Schema  `yaml:"-,inline" json:"-,inline"`
		Ref   *string `yaml:"$ref,omitempty" json:"$ref,omitempty"`
	}
	Components struct {
		Columns map[string]Column `yaml:"columns" json:"columns"`
		Classes map[string]Table  `yaml:"classes" json:"classes"`
	}
	Root struct {
		Schemas    []SchemaRef `yaml:"schemas" json:"schemas"`
		Components Components  `yaml:"components" json:"components"`
	}
)

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
		raise(errors.New("cannot resolve empty $ref"))
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
			raise(errors.New("cannot resolve $ref: '" + ref + "'"))
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

func (r *Column) follow(db *Root, path []string, i interface{}) bool {
	if len(path) == 0 {
		// can panic
		copyFromTo(r, i)
		return true
	}
	return false
}

func (r *ColumnRef) normalize(schema *SchemaRef, tableName string, columnIndex int, db *Root) {
	if r.Ref != nil {
		processRef(db, *r.Ref, &r.Value)
	}
	r.Value.Schema.normalize(schema, tableName, columnIndex, db)
}

const (
	cNN     = "Num"
	cTable  = "Table"
	cSchema = "Schema"
)

func (c *ConstraintSchema) normalize(schema *SchemaRef, tableName string, constraintIndex int, db *Root) {
	c.Constraint.Name = evalTemplateParameters(c.Constraint.Name, map[string]string{
		cNN:     strconv.Itoa(constraintIndex),
		cTable:  tableName,
		cSchema: schema.Value.Name,
	})
}

func (c *ColumnSchemaRef) normalize(schema *SchemaRef, tableName string, columnIndex int, db *Root) {
	if c.Ref != nil {
		processRef(db, *c.Ref, &c.Value)
	}
}

type (
	TableColumns []ColumnRef
)

func (c TableColumns) find(name string) ColumnRef {
	for i, column := range []ColumnRef(c) {
		if column.Value.Name == name {
			return []ColumnRef(c)[i]
		}
	}
	panic(fmt.Sprintf("cannot find column '%s'", name))
}

func (c *Table) follow(db *Root, path []string, i interface{}) bool {
	if len(path) == 0 {
		// can panic
		copyFromTo(c, i)
		return true
	}
	if len(path) > 1 {
		if path[0] == columns {
			column := TableColumns(c.Columns).find(path[1])
			copyFromTo(column.Value, i)
		}
		// TODO ??
	}
	return false
}

func (c *Table) normalize(schema *SchemaRef, tableName string, db *Root) {
	for i, column := range c.Columns {
		column.normalize(schema, tableName, i, db)
		c.Columns[i] = column
	}
	for i, constraint := range c.Constraints {
		constraint.normalize(schema, tableName, i, db)
		c.Constraints[i] = constraint
	}
}

func (c *SchemaRef) normalize(db *Root) {
	if c.Ref != nil {
		processRef(db, *c.Ref, &c.Value)
	}
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
			if column, ok := c.Components.Columns[path[2]]; ok {
				return column.follow(db, path[3:], i)
			}
		case classes:
			if class, ok := c.Components.Classes[path[2]]; ok {
				return class.follow(db, path[3:], i)
			}
		}
	}
	return false
}

func (c *Root) normalize() {
	for i, schemaRef := range c.Schemas {
		schemaRef.normalize(c)
		c.Schemas[i] = schemaRef
	}
}

package dragonfly

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"io"
)

const(
	sqlGetSchemaStruct = `
select table_name, ordinal_position, column_name, data_type, character_maximum_length, column_default, is_nullable != 'NO', numeric_precision, numeric_precision_radix, numeric_scale, domain_schema, domain_name, udt_name
from information_schema.columns where table_schema=$1`

)

type(
	columnStruct struct {
		Table string
		Ord int
		Column string
		Type string
		Max *int
		Default *string
		Nullable bool
		Precision *int
		Radix *int
		Scale *int
		DomainSchema *string
		Domain *string
		UdtName string
	}
	TableStruct []columnStruct
)

func (c columnStruct) checkDiffs(column ColumnRef, schema *SchemaRef, tableName string, root *Root, w io.Writer) {
	if c.Domain != nil {
		domain, isDomain := column.Value.Schema.makeDomainName()
		if !isDomain || domain != *c.Domain {
			writer(w, "\n-- alter column: %s.%s.%s", schema.Value.Name, tableName, column.Value.Name)
			writer(w, "\n-- type: %s, domain: %s", c.Type, *c.Domain)
			writer(w, "\nalter table %s.%s alter\n", schema.Value.Name, tableName)
			column.generateSQL(schema.Value.Name, tableName, root, w)
			writer(w, ";\n")
			return
		}
	}
}

func (c TableStruct) checkDiffs(schema *SchemaRef, tableName string, root *Root, w io.Writer) {
	table, ok := schema.Value.Tables[tableName]
	if !ok {
		panic(fmt.Sprintf("cannot found table `%s` in schema `%s`", tableName, schema.Value.Name))
	}
	for _, column := range table.Columns {
		existing, found := c.filterTableColumn(column.Value.Name)
		if !found {
			writer(w, "\n-- new column: %s.%s.%s", schema.Value.Name, tableName, column.Value.Name)
			writer(w, "\nalter table %s.%s add\n", schema.Value.Name, tableName)
			column.generateSQL(schema.Value.Name, tableName, root, w)
			writer(w, ";\n")
		} else {
			existing.checkDiffs(column, schema, tableName, root, w)
		}
	}
}

func (c TableStruct) filterTableColumns(tableName string) TableStruct {
	columns := make(TableStruct, 0, 20)
	for _, column := range c {
		if column.Table == tableName {
			columns = append(columns, column)
		}
	}
	return columns
}

func (c TableStruct) filterTableColumn(columnName string) (*columnStruct, bool) {
	for _, column := range c {
		if column.Column == columnName {
			return &column, true
		}
	}
	return nil, false
}

func (c *SchemaRef) checkDiff(db *sql.DB, schema string, root *Root, w io.Writer) {
	var columns TableStruct
	if q, err := db.Query(sqlGetSchemaStruct, schema); err != nil {
		panic(err)
	} else {
		columns = make([]columnStruct, 0, 100)
		var column columnStruct
		for q.Next() {
			if err := q.Scan(
				&column.Table,
				&column.Ord,
				&column.Column,
				&column.Type,
				&column.Max,
				&column.Default,
				&column.Nullable,
				&column.Precision,
				&column.Radix,
				&column.Scale,
				&column.DomainSchema,
				&column.Domain,
				&column.UdtName,
			); err != nil {
				panic(err)
			} else {
				columns = append(columns, column)
			}
		}
	}
	for tableName, _ := range c.Value.Tables {
		tableColumns := columns.filterTableColumns(tableName)
		tableColumns.checkDiffs(c, tableName, root, w)
	}
}

func DatabaseDiff(root *Root, schemaName, dbConnectionString string, w io.Writer) {
	var(
		db, err = sql.Open("postgres", dbConnectionString)
	)
	if err != nil {
		panic(err)
	}
	for _, schema := range root.Schemas {
		if schemaName == "" || schemaName == schema.Value.Name {
			schema.checkDiff(db, schema.Value.Name, root, w)
		}
	}
}

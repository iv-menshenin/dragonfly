package dragonfly

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"math/rand"
	"strings"
	"time"
)

const (
	sqlGetSchemaStruct = `
select table_name, ordinal_position, column_name, data_type, character_maximum_length, column_default, is_nullable != 'NO', numeric_precision, numeric_precision_radix, numeric_scale, domain_schema, domain_name, udt_name
from information_schema.columns where table_schema=$1;`

	sqlGetForeignKey = `
select tc.table_schema,
       tc.constraint_name,
       tc.table_name,
       kcu.column_name,
       ccu.table_schema as foreign_table_schema,
       ccu.table_name as foreign_table_name,
       ccu.column_name as foreign_column_name
  from information_schema.table_constraints as tc
  join information_schema.key_column_usage as kcu
    on tc.constraint_name = kcu.constraint_name
   and tc.table_schema = kcu.table_schema
  join information_schema.constraint_column_usage as ccu
    on ccu.constraint_name = tc.constraint_name
   and ccu.table_schema = tc.table_schema
 where tc.constraint_type='FOREIGN KEY'
   and tc.table_schema=$1
   and tc.table_name=$2
   and ccu.table_schema=$3
   and ccu.table_name=$4;`
)

type (
	columnStruct struct {
		Table        string
		Ord          int
		Column       string
		Type         string
		Max          *int
		Default      *string
		Nullable     bool
		Precision    *int
		Radix        *int
		Scale        *int
		DomainSchema *string
		Domain       *string
		UdtName      string
	}
	TableStruct []columnStruct

	ColumnMutationCase interface {
		GetNewColumnValue(*sql.DB) string
		GetOldColumnValue(*sql.DB) string
	}
	ColumnMutationCases []ColumnMutationCase
	ColumnCase          struct {
		TableSchema string
		TableName   string
		ColumnName  string
	}
	ColumnAdding struct {
		ColumnCase
		NewType ColumnRef
	}
	ColumnChange struct {
		ColumnCase
		OldType columnStruct
		NewType ColumnRef
	}
	ColumnDelete struct {
		ColumnCase
	}
	ColumnRename struct {
		ColumnCase
		OldColumnName string
	}
)

func (c *ColumnCase) GetNewColumnValue(*sql.DB) string {
	return c.ColumnName
}

func (c *ColumnCase) GetOldColumnValue(*sql.DB) string {
	return c.ColumnName
}

type (
	TableColumn struct {
		SchemaName string
		TableName  string
		ColumnName string
	}
	ForeignKeyInformation struct {
		KeyName      string
		MainTable    TableColumn
		ForeignTable TableColumn
	}
)

func getForeignKeys(db *sql.DB, mainTableSchema, mainTableName, foreignTableSchema, foreignTableName string) []ForeignKeyInformation {
	var keys []ForeignKeyInformation
	if q, err := db.Query(sqlGetForeignKey, mainTableSchema, mainTableName, foreignTableSchema, foreignTableName); err != nil {
		panic(err)
	} else {
		keys = make([]ForeignKeyInformation, 0, 1)
		var key ForeignKeyInformation
		for q.Next() {
			if err := q.Scan(
				&key.MainTable.SchemaName,
				&key.KeyName,
				&key.MainTable.TableName,
				&key.MainTable.ColumnName,
				&key.ForeignTable.SchemaName,
				&key.ForeignTable.TableName,
				&key.ForeignTable.ColumnName,
			); err != nil {
				panic(err)
			} else {
				keys = append(keys, key)
			}
		}
	}
	return keys
}

func (c *ColumnAdding) GetOldColumnValue(db *sql.DB) string {
	if c.NewType.Value.Schema.Value.NotNull && c.NewType.Value.Schema.Value.Default == nil {
		// resolve foreign keys
		for _, key := range c.NewType.Value.Constraints {
			if strings.EqualFold(key.Type, "foreign key") {
				fkAttrs := key.Parameters.Parameter.(ForeignKey)
				splitName := strings.Split(fkAttrs.ToTable, ".") // TODO error!
				fk := getForeignKeys(db, c.TableSchema, c.TableName, splitName[0], splitName[1])
				if len(fk) == 1 {
					foreignKey := fk[0]
					return fmt.Sprintf("(select %s from %s where %s=%s.%s.%s)", fkAttrs.ToColumn, fkAttrs.ToTable, foreignKey.ForeignTable.ColumnName, c.TableSchema, c.TableName, foreignKey.MainTable.ColumnName)
				}
			}
		}
		panic(fmt.Sprintf("cannot resolve values for new column `%s`", c.ColumnName))
	}
	return "null"
}

func (c *ColumnRename) GetOldColumnValue(*sql.DB) string {
	return c.OldColumnName
}

func (c ColumnMutationCases) MakeSolution(db *sql.DB, schemaName, tableName string, table TableClass, root *Root) {
	w := bytes.NewBufferString(fmt.Sprintf("-- solution for %s table structure change\n", tableName))
	fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
	tmpTableName := fmt.Sprintf("%s_tmp_%d", tableName, rand.Int())
	var (
		tmpTableStruct = TableClass{
			Columns: table.Columns.CopyFlatColumns(),
		}
		newColumnList = make([]string, 0, len(tmpTableStruct.Columns))
		oldColumnList = make([]string, 0, len(tmpTableStruct.Columns))
	)
	for _, mutation := range c {
		newColumnList = append(newColumnList, mutation.GetNewColumnValue(db))
		oldColumnList = append(oldColumnList, mutation.GetOldColumnValue(db))
	}
	for _, column := range tmpTableStruct.Columns {
		if !arrayContains(newColumnList, column.Value.Name) {
			newColumnList = append(newColumnList, column.Value.Name)
			oldColumnList = append(oldColumnList, column.Value.Name)
		}
	}
	tmpTableStruct.generateSQL(schemaName, tmpTableName, root, w)
	tmpTableName = fmt.Sprintf("%s.%s", schemaName, tmpTableName)
	writer(w, "insert into %s(%s) select %s from %s;\n", tmpTableName, strings.Join(newColumnList, ","), strings.Join(oldColumnList, ","), fullTableName)
	writer(w, "drop table %s;\n", tmpTableName)
	println(w.String())
}

func (c columnStruct) checkDiffs(schemaName, tableName string, column ColumnRef) ColumnMutationCases {
	mutation := make([]ColumnMutationCase, 0, 0)
	if c.Domain != nil {
		domain, isDomain := column.Value.Schema.makeDomainName()
		if !isDomain || !strings.EqualFold(domain, *c.Domain) {
			mutation = append(mutation, &ColumnChange{
				ColumnCase: ColumnCase{
					TableSchema: schemaName,
					TableName:   tableName,
					ColumnName:  column.Value.Name,
				},
				OldType: c,
				NewType: column,
			})
			return mutation
		}
	}
	return mutation
}

func (c TableStruct) findSimilarType(column ColumnRef) (*columnStruct, int) {
	// TODO ...
	for i, s := range c {
		if domain, ok := column.Value.Schema.makeDomainName(); ok {
			if s.Domain != nil && strings.EqualFold(*s.Domain, domain) {
				return &s, i
			}
		} else {
			if strings.EqualFold(s.Type, column.Value.Schema.Value.Type) {
				if column.Value.Schema.Value.Length != nil && s.Max != nil && *s.Max == *column.Value.Schema.Value.Length {
					return &s, i
				} else if column.Value.Schema.Value.Length == nil && s.Max == nil {
					return &s, i
				}
			}
		}
	}
	return nil, -1
}

func (c TableStruct) checkDiffs(schema *SchemaRef, tableName string) ColumnMutationCases {
	table, ok := schema.Value.Tables[tableName]
	if !ok {
		panic(fmt.Sprintf("cannot found table `%s` in schema `%s`", tableName, schema.Value.Name))
	}
	deletedColumns := make(TableStruct, 0, 0)
	for _, columnStruct := range c {
		if !table.Columns.exists(columnStruct.Column) {
			deletedColumns = append(deletedColumns, columnStruct)
		}
	}
	mutations := make([]ColumnMutationCase, 0, 0)
	for _, column := range table.Columns {
		existing, found := c.filterTableColumn(column.Value.Name)
		if !found {
			if s, i := deletedColumns.findSimilarType(column); i > -1 {
				mutations = append(mutations, &ColumnRename{
					ColumnCase: ColumnCase{
						TableSchema: schema.Value.Name,
						TableName:   tableName,
						ColumnName:  column.Value.Name,
					},
					OldColumnName: s.Column,
				})
				deletedColumns = append(deletedColumns[:i], deletedColumns[i+1:]...)
			} else {
				mutations = append(mutations, &ColumnAdding{
					ColumnCase: ColumnCase{
						TableSchema: schema.Value.Name,
						TableName:   tableName,
						ColumnName:  column.Value.Name,
					},
					NewType: column,
				})
			}
		} else {
			mutations = append(mutations, existing.checkDiffs(schema.Value.Name, tableName, column)...)
		}
	}
	return mutations
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
	for tableName, tableStruct := range c.Value.Tables {
		tableColumns := columns.filterTableColumns(tableName)
		columnsMutations := tableColumns.checkDiffs(c, tableName)
		if len(columnsMutations) > 0 {
			columnsMutations.MakeSolution(db, schema, tableName, tableStruct, root)
		}
	}
}

func DatabaseDiff(root *Root, schemaName, dbConnectionString string, w io.Writer) {
	var (
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

func init() {
	rand.Seed(time.Now().UnixNano())
}

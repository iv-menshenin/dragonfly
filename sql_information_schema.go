package dragonfly

import (
	"database/sql"
	"strings"
)

const (
	sqlGetSchemaList = `
select schema_name, schema_owner
from information_schema.schemata
where schema_name not in ('information_schema','pg_catalog')
  and lower(catalog_name) = $1;`

	sqlGetAllTableColumns = `
select table_schema, table_name, ordinal_position, column_name, data_type, character_maximum_length, column_default, is_nullable != 'NO', numeric_precision, numeric_precision_radix, numeric_scale, domain_schema, domain_name, udt_name
from information_schema.columns
where table_schema not in ('information_schema','pg_catalog')
  and lower(table_catalog) = $1;`

	sqlGetAllDomains = `
select d.domain_schema, d.domain_name, d.data_type, d.character_maximum_length, d.domain_default, d.numeric_precision,
       d.numeric_precision_radix, d.numeric_scale, d.udt_name,
       not exists((select true from pg_type where typnotnull and typtype='d' and typname=d.domain_name limit 1))
  from information_schema.domains d
where d.domain_schema not in ('information_schema','pg_catalog')
  and lower(d.udt_catalog) = $1;`

	sqlGetAllTablesConstraints = `
select tc.table_schema, tc.table_name, tc.constraint_schema, tc.constraint_name, tc.constraint_type, kcu.column_name,
       ccu.table_schema, ccu.table_name, ccu.column_name
 from information_schema.table_constraints as tc
 join information_schema.key_column_usage as kcu
   on tc.constraint_name = kcu.constraint_name
  and tc.table_schema = kcu.table_schema
 left join information_schema.constraint_column_usage as ccu
   on ccu.constraint_name = tc.constraint_name
  and ccu.constraint_schema = tc.constraint_schema
  and tc.constraint_type='FOREIGN KEY'
where tc.table_schema not in ('information_schema','pg_catalog')
  and lower(tc.table_catalog) = $1;`
)

type (
	DomainStruct struct {
		DomainSchema string
		Domain       string
		Type         string
		Max          *int
		Default      *string
		Nullable     bool
		Precision    *int
		Radix        *int
		Scale        *int
		UdtName      string
		Used         *bool // WARNING: not allowed nil here
	} // TODO hide type and make builder, for protection Used field
	DomainsStruct map[string]DomainStruct
	ColumnStruct  struct {
		TableSchema  string
		TableName    string
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
		Used         *bool // WARNING: not allowed nil here
	} // TODO hide type and make builder, for protection Used field
	ColumnsStruct   map[string]ColumnStruct
	TableConstraint struct {
		TableSchema      string
		TableName        string
		ConstraintSchema string
		ConstraintName   string
		ConstraintType   string
		Columns          []string
		ForeignKey       *ForeignKeyInformation
	}
	ActualConstraints map[string]TableConstraint
	TableStruct       struct {
		Schema      string
		Name        string
		Columns     ColumnsStruct
		Constraints ActualConstraints
		Used        *bool // WARNING: not allowed nil here
	} // TODO hide type and make builder, for protection Used field
	TablesStruct map[string]TableStruct
	SchemaStruct struct {
		Name    string
		Owner   string
		Tables  TablesStruct
		Domains DomainsStruct
	}
	ActualSchemas struct {
		Schemas map[string]SchemaStruct
	}
)

func (c *ActualSchemas) getUnusedDomainAndSetItAsUsed(schemaName, domainName string) *DomainStruct {
	domain, ok := c.Schemas[strings.ToLower(schemaName)].Domains[strings.ToLower(domainName)]
	if ok {
		c.setDomainAsUsed(schemaName, domainName)
		return &domain
	}
	return nil
}

func (c *ActualSchemas) getUnusedTableAndSetItAsUsed(schemaName, tableName string) *TableStruct {
	table, ok := c.Schemas[strings.ToLower(schemaName)].Tables[strings.ToLower(tableName)]
	if ok {
		c.setTableAsUsed(schemaName, tableName)
		return &table
	}
	return nil
}

func (c *ActualSchemas) getUnusedColumnAndSetItAsUsed(schemaName, tableName, columnName string) *ColumnStruct {
	column, ok := c.Schemas[strings.ToLower(schemaName)].Tables[strings.ToLower(tableName)].Columns[strings.ToLower(columnName)]
	if ok {
		*column.Used = true
		return &column
	}
	return nil
}

func (c *ActualSchemas) getUnusedColumns(schemaName, tableName string) []ColumnStruct {
	table, ok := c.Schemas[strings.ToLower(schemaName)].Tables[strings.ToLower(tableName)]
	if !ok {
		return nil
	}
	columns := make([]ColumnStruct, 0, len(table.Columns))
	for _, column := range table.Columns {
		if !*column.Used {
			columns = append(columns, column)
		}
	}
	return columns
}

func (c *ActualSchemas) getUnusedDomains() (domains []DomainStruct) {
	domains = make([]DomainStruct, 0, 10)
	for _, schema := range c.Schemas {
		for name, domain := range schema.Domains {
			if !*domain.Used {
				domains = append(domains, schema.Domains[name])
			}
		}
	}
	return domains
}

func (c *ActualSchemas) getUnusedTables() (domains []TableStruct) {
	domains = make([]TableStruct, 0, 10)
	for _, schema := range c.Schemas {
		for name, table := range schema.Tables {
			if !*table.Used {
				domains = append(domains, schema.Tables[name])
			}
		}
	}
	return domains
}

func (c *ActualSchemas) getColumnConstraints(schemaName, tableName, columnName string) []TableConstraint {
	table, ok := c.Schemas[strings.ToLower(schemaName)].Tables[strings.ToLower(tableName)]
	if !ok {
		return nil
	}
	constraints := make([]TableConstraint, 0, len(table.Constraints))
	for i, constraint := range table.Constraints {
		if iArrayContains(constraint.Columns, columnName) {
			constraints = append(constraints, table.Constraints[i])
		}
	}
	return constraints
}

type (
	ColumnFullName struct {
		TableSchema string
		TableName   string
		ColumnName  string
	}
)

func (c *ActualSchemas) getDomainUsages(domainSchema, domainName string) []ColumnFullName {
	var usages = make([]ColumnFullName, 0, 50)
	for schemaName, schema := range c.Schemas {
		for tableName, table := range schema.Tables {
			for columnName, column := range table.Columns {
				if column.DomainSchema != nil && column.Domain != nil {
					if strings.EqualFold(*column.DomainSchema, domainSchema) && strings.EqualFold(*column.Domain, domainName) {
						usages = append(usages, ColumnFullName{
							TableSchema: schemaName,
							TableName:   tableName,
							ColumnName:  columnName,
						})
					}
				}
			}
		}
	}
	return usages
}

func (c *ActualSchemas) getForeignKey(schemaName, foreignSchema, tableName, foreignTable string) *ForeignKeyInformation {
	table, ok := c.Schemas[strings.ToLower(schemaName)].Tables[strings.ToLower(tableName)]
	if !ok {
		return nil
	}
	for i, constraint := range table.Constraints {
		if constraint.ForeignKey != nil {
			if strings.EqualFold(constraint.ForeignKey.ForeignTable.SchemaName, foreignSchema) && strings.EqualFold(constraint.ForeignKey.ForeignTable.TableName, foreignTable) {
				return table.Constraints[i].ForeignKey
			}
		}
	}
	return nil
}

func (c *ActualSchemas) setDomainAsUsed(domainSchema, domainName string) {
	domain, ok := c.Schemas[strings.ToLower(domainSchema)].Domains[strings.ToLower(domainName)]
	if !ok {
		panic("something went wrong. cannot mark domain as used")
	}
	*domain.Used = true
}

func (c *ActualSchemas) setTableAsUsed(tableSchema, tableName string) {
	table, ok := c.Schemas[strings.ToLower(tableSchema)].Tables[strings.ToLower(tableName)]
	if !ok {
		panic("something went wrong. cannot mark table as used")
	}
	*table.Used = true
}

func (c ActualConstraints) filterConstraints(schemaName, tableName string) ActualConstraints {
	constraints := make(ActualConstraints, 0)
	for name, constraint := range c {
		if strings.EqualFold(constraint.TableSchema, schemaName) && strings.EqualFold(constraint.TableName, tableName) {
			constraints[name] = c[name]
		}
	}
	return constraints
}

func getAllSchemaNames(db *sql.DB, catalog string) (list ActualSchemas) {
	list.Schemas = make(map[string]SchemaStruct, 10)
	if q, err := db.Query(sqlGetSchemaList, strings.ToLower(catalog)); err != nil {
		panic(err)
	} else {
		var schema SchemaStruct
		for q.Next() {
			if err := q.Scan(
				&schema.Name,
				&schema.Owner,
			); err != nil {
				panic(err)
			} else {
				schema.Tables = make(TablesStruct, 0)
				schema.Domains = make(DomainsStruct, 0)
				list.Schemas[strings.ToLower(schema.Name)] = schema
			}
		}
	}
	return
}

func getAllTables(db *sql.DB, catalog string) (columns []ColumnStruct) {
	if q, err := db.Query(sqlGetAllTableColumns, strings.ToLower(catalog)); err != nil {
		panic(err)
	} else {
		columns = make([]ColumnStruct, 0, 100)
		var column ColumnStruct
		for q.Next() {
			if err := q.Scan(
				&column.TableSchema,
				&column.TableName,
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
				column.Used = refBool(false)
				columns = append(columns, column)
			}
		}
	}
	return columns
}

func getAllDomains(db *sql.DB, catalog string) (domains []DomainStruct) {
	if q, err := db.Query(sqlGetAllDomains, strings.ToLower(catalog)); err != nil {
		panic(err)
	} else {
		domains = make([]DomainStruct, 0, 100)
		var domain DomainStruct
		for q.Next() {
			if err := q.Scan(
				&domain.DomainSchema,
				&domain.Domain,
				&domain.Type,
				&domain.Max,
				&domain.Default,
				&domain.Precision,
				&domain.Radix,
				&domain.Scale,
				&domain.UdtName,
				&domain.Nullable,
			); err != nil {
				panic(err)
			} else {
				domain.Used = refBool(false)
				domains = append(domains, domain)
			}
		}
	}
	return domains
}

func getAllConstraints(db *sql.DB, catalog string) (constraints ActualConstraints) {
	constraints = make(ActualConstraints, 0)
	type constraintFlat struct {
		TableSchema        string
		TableName          string
		ConstraintSchema   string
		ConstraintName     string
		ConstraintType     string
		ColumnName         string
		ForeignTableSchema *string
		ForeignTableName   *string
		ForeignTableColumn *string
	}
	if q, err := db.Query(sqlGetAllTablesConstraints, catalog); err != nil {
		panic(err)
	} else {
		constraintsColumn := make([]constraintFlat, 0, 10)
		var constraint constraintFlat
		for q.Next() {
			if err := q.Scan(
				&constraint.TableSchema,
				&constraint.TableName,
				&constraint.ConstraintSchema,
				&constraint.ConstraintName,
				&constraint.ConstraintType,
				&constraint.ColumnName,
				&constraint.ForeignTableSchema,
				&constraint.ForeignTableName,
				&constraint.ForeignTableColumn,
			); err != nil {
				panic(err)
			} else {
				var c TableConstraint
				if _, ok := constraints[strings.ToLower(constraint.ConstraintName)]; !ok {
					c = TableConstraint{
						TableSchema:      constraint.TableSchema,
						TableName:        constraint.TableName,
						ConstraintSchema: constraint.ConstraintSchema,
						ConstraintName:   constraint.ConstraintName,
						ConstraintType:   constraint.ConstraintType,
						Columns:          make([]string, 0, 1),
					}
				} else {
					c = constraints[strings.ToLower(constraint.ConstraintName)]
				}
				c.Columns = append(c.Columns, constraint.ColumnName)
				if constraint.ForeignTableColumn != nil {
					c.ForeignKey = &ForeignKeyInformation{
						KeyName: constraint.ConstraintName,
						MainTable: TableColumn{
							SchemaName: constraint.TableSchema,
							TableName:  constraint.TableName,
							ColumnName: constraint.ColumnName,
						},
						ForeignTable: TableColumn{
							SchemaName: *constraint.ForeignTableSchema,
							TableName:  *constraint.ForeignTableName,
							ColumnName: *constraint.ForeignTableColumn,
						},
					}
				}
				constraints[strings.ToLower(constraint.ConstraintName)] = c
				constraintsColumn = append(constraintsColumn, constraint)
			}
		}
	}
	return
}

func GetAllDatabaseInformation(db *sql.DB, dbName string) (info ActualSchemas) {
	info = getAllSchemaNames(db, dbName)
	allDomains := getAllDomains(db, dbName)
	allTables := getAllTables(db, dbName)
	allConstraints := getAllConstraints(db, dbName)
	for schemaName := range info.Schemas {
		schemaDomains := make(map[string]DomainStruct)
		for i, domain := range allDomains {
			if strings.EqualFold(domain.DomainSchema, schemaName) {
				schemaDomains[strings.ToLower(domain.Domain)] = allDomains[i]
			}
		}
		schemaColumns := make(map[string]map[string]ColumnStruct)
		for i, columnStruct := range allTables {
			if strings.EqualFold(columnStruct.TableSchema, schemaName) {
				tableName := columnStruct.TableName
				if _, ok := schemaColumns[strings.ToLower(tableName)]; !ok {
					schemaColumns[strings.ToLower(tableName)] = make(map[string]ColumnStruct, 20)
				}
				columnName := columnStruct.Column
				// there is no columns with same names
				schemaColumns[strings.ToLower(tableName)][strings.ToLower(columnName)] = allTables[i]
			}
		}
		schema := info.Schemas[schemaName]
		schema.Domains = schemaDomains
		for tableName, tableStruct := range schemaColumns {
			table := TableStruct{
				Used:        refBool(false),
				Schema:      schemaName,
				Name:        tableName,
				Columns:     tableStruct,
				Constraints: allConstraints.filterConstraints(schemaName, tableName),
			}
			schema.Tables[strings.ToLower(tableName)] = table
		}
		info.Schemas[schemaName] = schema
	}
	return
}

package dragonfly

import (
	"database/sql"
	"fmt"
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

	sqlGetRecordTypes = `
select n.nspname, t.typname, a.attname, a.attnum, at.typname, at.typnotnull, a.attnotnull,
       information_schema._pg_char_max_length(a.atttypid, a.atttypmod),
       information_schema._pg_numeric_precision(a.atttypid, a.atttypmod),
       information_schema._pg_numeric_scale(a.atttypid, a.atttypmod), n1.nspname
from pg_catalog.pg_type t
inner join pg_catalog.pg_namespace n on n.oid = t.typnamespace
inner join pg_catalog.pg_attribute a on a.attrelid = t.typrelid
inner join pg_catalog.pg_type at on at.oid = a.atttypid
left join pg_catalog.pg_namespace n1 on n1.oid = at.typnamespace and at.typtype = 'd'
where n.nspname not in ('information_schema','pg_catalog')
  and t.typtype = 'c'
  and not a.attisdropped
  and exists(
      select 1 from information_schema.user_defined_types dt
      where dt.user_defined_type_schema = n.nspname and dt.user_defined_type_name = t.typname
        and lower(dt.user_defined_type_catalog) = $1
  )
order by n.nspname, t.typname, a.attnum;`

	sqlGetEnumTypes = `
select n.nspname, t.typname, e.enumlabel, e.enumsortorder, t.typnotnull
from pg_enum e
join pg_type t on e.enumtypid = t.oid
inner join pg_catalog.pg_namespace n on n.oid = t.typnamespace;
`
)

type (
	rawEnumStruct struct {
		Schema    string
		Type      string
		Enum      string
		SortOrder int
		NotNull   bool
	}
	rawEnums      []rawEnumStruct
	rawTypeStruct struct {
		Schema       string
		TypeName     string
		AttrName     string
		AttrOrd      int
		AttrType     string
		TypeRequired bool
		AttrRequired bool
		MaxLength    *int
		Precision    *int
		Scale        *int
	}
	typesStruct     []rawTypeStruct
	rawDomainStruct struct {
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
	}
	rawColumnStruct struct {
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
	}
	actualConstraint struct {
		TableSchema      string
		TableName        string
		ConstraintSchema string
		ConstraintName   string
		ConstraintType   string
		Columns          []string
		ForeignKey       *ForeignKeyInformation
	}
	rawActualConstraints map[string]actualConstraint

	actualSchema struct {
		Name  string
		Owner string
	}
	rawActualSchemaNames struct {
		Schemas map[string]actualSchema
	}
)

func (c rawEnums) extractSchema(schema string) map[string]rawEnums {
	result := make(map[string]rawEnums, 0)
	for _, r := range c {
		if strings.EqualFold(r.Schema, schema) {
			if _, ok := result[r.Type]; !ok {
				result[r.Type] = make(rawEnums, 0)
			}
			result[r.Type] = append(result[r.Type], r)
		}
	}
	return result
}

func (c rawEnums) toEnumSchema() TypeSchema {
	var values = make([]EnumEntity, len(c), len(c))
	for _, r := range c {
		values[r.SortOrder-1] = EnumEntity{
			Value: r.Enum,
		}
	}
	return TypeSchema{
		TypeBase: TypeBase{Type: "enum"},
		Enum:     values,
		used:     refBool(false),
	}
}

func (c typesStruct) extractSchema(schema string) map[string]typesStruct {
	result := make(map[string]typesStruct, 0)
	for _, r := range c {
		if strings.EqualFold(r.Schema, schema) {
			if _, ok := result[r.TypeName]; !ok {
				result[r.TypeName] = make(typesStruct, 0)
			}
			result[r.TypeName] = append(result[r.TypeName], r)
		}
	}
	return result
}

func (c typesStruct) toRecordSchema() TypeSchema {
	var fields = make([]ColumnRef, len(c), len(c))
	for _, r := range c {
		fields[r.AttrOrd-1] = ColumnRef{
			Value: r.toColumn(),
			Ref:   nil,
			used:  refBool(false),
		}
	}
	return TypeSchema{
		TypeBase: TypeBase{Type: "record"},
		Fields:   fields,
		used:     refBool(false),
	}
}

func (c *rawTypeStruct) toColumn() Column {
	return Column{
		Name: c.AttrName,
		Schema: ColumnSchemaRef{
			Value: DomainSchema{
				TypeBase: TypeBase{
					Type:      c.AttrType,
					Length:    c.Precision,
					Precision: c.Scale,
				},
				NotNull: c.AttrRequired,
				Default: nil, // TODO
				Check:   nil,
				used:    refBool(false),
			},
			Ref: nil,
		},
		Constraints: nil,
		Tags:        nil,
		Description: "",
	}
}

func (c *rawDomainStruct) toDomainSchema() DomainSchema {
	return DomainSchema{
		TypeBase: TypeBase{
			Type:      c.Type,
			Length:    c.Max,
			Precision: c.Precision,
		},
		NotNull: !c.Nullable,
		Default: c.Default,
		Check:   nil, // TODO
		used:    refBool(false),
	}
}

func (c *rawColumnStruct) toColumnRef() ColumnRef {
	var columnSchemaRef *string = nil
	if c.DomainSchema != nil && c.Domain != nil {
		columnSchemaRef = stringToRef(fmt.Sprintf(pathToDomainTemplate, *c.DomainSchema, *c.Domain))
	}
	return ColumnRef{
		Value: Column{
			Name: c.Column,
			Schema: ColumnSchemaRef{
				Value: DomainSchema{
					TypeBase: TypeBase{
						Type:      c.Type,
						Length:    c.Max,
						Precision: c.Precision,
					},
					NotNull: !c.Nullable,
					Default: c.Default,
					Check:   nil, // TODO
					used:    refBool(false),
				},
				Ref: columnSchemaRef,
			},
			Constraints: nil,
			Tags:        nil,
			Description: "",
		},
		Ref:  nil,
		used: refBool(false),
	}
}

func (c *Root) getUnusedDomainAndSetItAsUsed(schemaName, domainName string) *DomainSchema {
	schema, ok := c.Schemas.tryToFind(schemaName)
	if ok {
		for name, domainSchema := range schema.Value.Domains {
			if strings.EqualFold(name, domainName) {
				*domainSchema.used = true
				return &domainSchema
			}
		}
	}
	return nil
}

func (c *Root) getUnusedTypeAndSetItAsUsed(schemaName, typeName string) *TypeSchema {
	schema, ok := c.Schemas.tryToFind(schemaName)
	if !ok {
		return nil
	}
	for name, userType := range schema.Value.Types {
		if strings.EqualFold(name, typeName) {
			*userType.used = true
			return &userType
		}
	}
	return nil
}

func (c *Root) getUnusedTableAndSetItAsUsed(schemaName, tableName string) *Table {
	if schema, ok := c.Schemas.tryToFind(schemaName); ok {
		for name, table := range schema.Value.Tables {
			if strings.EqualFold(name, tableName) && !*table.used {
				*table.used = true
				return &table
			}
		}
	}
	return nil
}

func (c *Root) getUnusedColumnAndSetItAsUsed(schemaName, tableName, columnName string) *ColumnRef {
	if schema, ok := c.Schemas.tryToFind(schemaName); ok {
		for name, table := range schema.Value.Tables {
			if strings.EqualFold(name, tableName) {
				for i, column := range table.Columns {
					if strings.EqualFold(column.Value.Name, columnName) && !*column.used {
						*column.used = true
						return &table.Columns[i]
					}
				}
			}
		}
	}
	return nil
}

func (c *Root) getUnusedColumns(schemaName, tableName string) []ColumnRef {
	if schema, ok := c.Schemas.tryToFind(schemaName); ok {
		for name, table := range schema.Value.Tables {
			if strings.EqualFold(name, tableName) {
				columns := make([]ColumnRef, 0, len(table.Columns))
				for _, column := range table.Columns {
					if !*column.used {
						columns = append(columns, column)
					}
				}
				return columns
			}
		}
	}
	return nil
}

func (c *Root) getUnusedDomains() (domains map[string]map[string]DomainSchema) {
	domains = make(map[string]map[string]DomainSchema, 10)
	for _, schema := range c.Schemas {
		schemaName := schema.Value.Name
		for name, domain := range schema.Value.Domains {
			if !*domain.used {
				if _, ok := domains[schemaName]; ok {
					domains[schemaName][name] = schema.Value.Domains[name]
				} else {
					domains[schemaName] = map[string]DomainSchema{
						name: schema.Value.Domains[name],
					}
				}
			}
		}
	}
	return domains
}

func (c *Root) getUnusedTables() (tables map[string]map[string]Table) {
	tables = make(map[string]map[string]Table)
	for _, schema := range c.Schemas {
		schemaName := schema.Value.Name
		for name, table := range schema.Value.Tables {
			if !*table.used {
				var (
					ok bool
					t  map[string]Table
				)
				if t, ok = tables[schemaName]; !ok {
					t = make(map[string]Table)
				}
				t[name] = table
				tables[schemaName] = t
			}
		}
	}
	return tables
}

func (c *Root) getUnusedTypes() (types map[string]map[string]TypeSchema) {
	types = make(map[string]map[string]TypeSchema)
	for _, schema := range c.Schemas {
		schemaName := schema.Value.Name
		for name, customType := range schema.Value.Types {
			if !*customType.used {
				var (
					ok bool
					t  map[string]TypeSchema
				)
				if t, ok = types[schemaName]; !ok {
					t = make(map[string]TypeSchema)
				}
				t[name] = customType
				types[schemaName] = t
			}
		}
	}
	return types
}

func (c *Root) getColumnConstraints(schemaName, tableName, columnName string) []Constraint {
	if schema, ok := c.Schemas.tryToFind(schemaName); ok {
		for name, table := range schema.Value.Tables {
			if strings.EqualFold(name, tableName) {
				constraints := make([]Constraint, 0, len(table.Constraints))
				for i, constraint := range table.Constraints {
					if iArrayContains(constraint.Columns, columnName) {
						constraints = append(constraints, table.Constraints[i].Constraint)
					}
				}
				return constraints
			}
		}
	}
	return nil
}

type (
	ColumnFullName struct {
		TableSchema string
		TableName   string
		ColumnName  string
	}
)

func (c *Root) getDomainUsages(domainSchema, domainName string) []ColumnFullName {
	var usages = make([]ColumnFullName, 0, 50)
	for _, schema := range c.Schemas {
		schemaName := schema.Value.Name
		for tableName, table := range schema.Value.Tables {
			for _, column := range table.Columns {
				if domainSchema1, domainName1, ok := column.Value.Schema.makeCustomType(); ok {
					if strings.EqualFold(domainSchema1, domainSchema) && strings.EqualFold(domainName1, domainName) {
						usages = append(usages, ColumnFullName{
							TableSchema: schemaName,
							TableName:   tableName,
							ColumnName:  column.Value.Name,
						})
					}
				}
			}
		}
	}
	return usages
}

func (c *Root) getForeignKey(schemaName, foreignSchema, tableName, foreignTable string) *ForeignKey {
	if schema, ok := c.Schemas.tryToFind(schemaName); ok {
		for name, table := range schema.Value.Tables {
			if strings.EqualFold(name, tableName) {
				for _, constraint := range table.Constraints {
					if constraint.Constraint.Type == ConstraintForeignKey {
						fk, ok := constraint.Constraint.Parameters.Parameter.(ForeignKey)
						if ok {
							if tableName := strings.Split(fk.ToTable, "."); len(tableName) == 2 {
								if strings.EqualFold(tableName[0], foreignSchema) && strings.EqualFold(tableName[1], foreignTable) {
									return &fk
								}
							}
						}
					}
				}
			}
		}
	}
	return nil
}

func (c rawActualConstraints) toTableConstraints() TableConstraints {
	constraints := make(TableConstraints, 0, len(c))
	for name, constraint := range c {
		var parameter interface{} = nil
		cType, ok := constraintReference[strings.ToLower(constraint.ConstraintType)]
		if !ok {
			panic(fmt.Sprintf("cannot resolve constraint type `%s` for `%s`", constraint.ConstraintType, name))
		}
		switch cType {
		case ConstraintPrimaryKey:
			parameter = nil // wow!
		case ConstraintForeignKey:
			parameter = ForeignKey{
				ToTable:  fmt.Sprintf("%s.%s", constraint.ForeignKey.ForeignTable.SchemaName, constraint.ForeignKey.ForeignTable.TableName),
				ToColumn: constraint.ForeignKey.ForeignTable.ColumnName,
				OnUpdate: nil, // TODO rules
				OnDelete: nil, // TODO rules
			}
		case ConstraintUniqueKey:
			parameter = Where{Where: ""} // TODO `where` constraint parameter
		case ConstraintCheck:
			parameter = Check{Expression: ""} // TODO check expression
		default:
			panic("unimplemented")
		}
		constraints = append(constraints, ConstraintSchema{
			Columns: constraint.Columns,
			Constraint: Constraint{
				Name:       constraint.ConstraintName,
				Type:       cType,
				Parameters: ConstraintParameters{Parameter: parameter},
			},
		})
	}
	return constraints
}

func (c rawActualConstraints) filterConstraints(schemaName, tableName string) rawActualConstraints {
	constraints := make(rawActualConstraints, 0)
	for name, constraint := range c {
		if strings.EqualFold(constraint.TableSchema, schemaName) && strings.EqualFold(constraint.TableName, tableName) {
			constraints[name] = c[name]
		}
	}
	return constraints
}

func getAllSchemaNames(db *sql.DB, catalog string) (list rawActualSchemaNames, err error) {
	var q *sql.Rows
	if q, err = db.Query(sqlGetSchemaList, strings.ToLower(catalog)); err != nil {
		return
	} else {
		list.Schemas = make(map[string]actualSchema, 10)
		var schema actualSchema
		for q.Next() {
			if err = q.Err(); err != nil {
				return
			}
			if err = q.Scan(
				&schema.Name,
				&schema.Owner,
			); err != nil {
				return
			} else {
				list.Schemas[schema.Name] = schema
			}
		}
	}
	return
}

func getAllTables(db *sql.DB, catalog string) (columns []rawColumnStruct, err error) {
	var q *sql.Rows
	if q, err = db.Query(sqlGetAllTableColumns, strings.ToLower(catalog)); err != nil {
		return
	} else {
		columns = make([]rawColumnStruct, 0, 100)
		var column rawColumnStruct
		for q.Next() {
			if err = q.Err(); err != nil {
				return
			}
			if err = q.Scan(
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
				return
			} else {
				columns = append(columns, column)
			}
		}
	}
	return
}

func getAllDomains(db *sql.DB, catalog string) (domains []rawDomainStruct, err error) {
	var q *sql.Rows
	if q, err = db.Query(sqlGetAllDomains, strings.ToLower(catalog)); err != nil {
		return
	} else {
		domains = make([]rawDomainStruct, 0, 100)
		var domain rawDomainStruct
		for q.Next() {
			if err = q.Err(); err != nil {
				return
			}
			if err = q.Scan(
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
				return
			} else {
				domain.Used = refBool(false)
				domains = append(domains, domain)
			}
		}
	}
	return
}

func getAllRecords(db *sql.DB, catalog string) (attributes []rawTypeStruct, err error) {
	var q *sql.Rows
	if q, err = db.Query(sqlGetRecordTypes, strings.ToLower(catalog)); err != nil {
		return
	} else {
		attributes = make([]rawTypeStruct, 0, 100)
		var attr rawTypeStruct
		for q.Next() {
			if err = q.Err(); err != nil {
				return
			}
			var namespace *string
			if err = q.Scan(
				&attr.Schema,
				&attr.TypeName,
				&attr.AttrName,
				&attr.AttrOrd,
				&attr.AttrType,
				&attr.TypeRequired,
				&attr.AttrRequired,
				&attr.MaxLength,
				&attr.Precision,
				&attr.Scale,
				&namespace,
			); err != nil {
				return
			} else {
				if namespace != nil {
					attr.AttrType = fmt.Sprintf("%s.%s", *namespace, attr.AttrType)
				}
				attributes = append(attributes, attr)
			}
		}
	}
	return
}

func getAllEnums(db *sql.DB, _ string) (enums rawEnums, err error) {
	var q *sql.Rows
	if q, err = db.Query(sqlGetEnumTypes); err != nil {
		return
	} else {
		enums = make(rawEnums, 0, 100)
		var attr rawEnumStruct
		for q.Next() {
			if err = q.Err(); err != nil {
				return
			}
			if err = q.Scan(
				&attr.Schema,
				&attr.Type,
				&attr.Enum,
				&attr.SortOrder,
				&attr.NotNull,
			); err != nil {
				return
			} else {
				enums = append(enums, attr)
			}
		}
	}
	return
}

func getAllConstraints(db *sql.DB, catalog string) (constraints rawActualConstraints, err error) {
	var q *sql.Rows
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
	if q, err = db.Query(sqlGetAllTablesConstraints, catalog); err != nil {
		return
	} else {
		constraints = make(rawActualConstraints, 0)
		constraintsColumn := make([]constraintFlat, 0, 10)
		var constraint constraintFlat
		for q.Next() {
			if err = q.Err(); err != nil {
				return
			}
			if err = q.Scan(
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
				return
			} else {
				var c actualConstraint
				if _, ok := constraints[strings.ToLower(constraint.ConstraintName)]; !ok {
					c = actualConstraint{
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

func getAllDatabaseInformation(db *sql.DB, dbName string) (info Root, err error) {
	var (
		allSchemas     rawActualSchemaNames
		allDomains     []rawDomainStruct
		allRecordTypes typesStruct
		allEnumTypes   rawEnums
		allTables      []rawColumnStruct
		allConstraints rawActualConstraints
	)
	if allSchemas, err = getAllSchemaNames(db, dbName); err != nil {
		return
	}
	if allDomains, err = getAllDomains(db, dbName); err != nil {
		return
	}
	if allRecordTypes, err = getAllRecords(db, dbName); err != nil {
		return
	}
	if allEnumTypes, err = getAllEnums(db, dbName); err != nil {
		return
	}
	if allTables, err = getAllTables(db, dbName); err != nil {
		return
	}
	if allConstraints, err = getAllConstraints(db, dbName); err != nil {
		return
	}
	info.Schemas = make([]SchemaRef, 0, len(allSchemas.Schemas))
	for actualSchemaName := range allSchemas.Schemas {
		schemaDomains := make(DomainsContainer, 0)
		for i, domain := range allDomains {
			if strings.EqualFold(domain.DomainSchema, actualSchemaName) {
				schemaDomains[domain.Domain] = allDomains[i].toDomainSchema()
			}
		}
		schemaTypes := make(TypesContainer, 0)
		for typeName, recordType := range allRecordTypes.extractSchema(actualSchemaName) {
			schemaTypes[typeName] = recordType.toRecordSchema()
		}
		for typeName, enumType := range allEnumTypes.extractSchema(actualSchemaName) {
			schemaTypes[typeName] = enumType.toEnumSchema()
		}
		schemaColumns := make(map[string]ColumnsContainer)
		for i, columnStruct := range allTables {
			if strings.EqualFold(columnStruct.TableSchema, actualSchemaName) {
				tableName := columnStruct.TableName
				if _, ok := schemaColumns[strings.ToLower(tableName)]; !ok {
					schemaColumns[strings.ToLower(tableName)] = make(ColumnsContainer, 0, 20)
				}
				schemaColumns[strings.ToLower(tableName)] = append(schemaColumns[strings.ToLower(tableName)], allTables[i].toColumnRef())
			}
		}
		schemaTables := make(TablesContainer)
		for tableName, tableStruct := range schemaColumns {
			table := Table{
				Columns:     tableStruct,
				Constraints: allConstraints.filterConstraints(actualSchemaName, tableName).toTableConstraints(),
				used:        refBool(false),
			}
			schemaTables[tableName] = table
		}
		schema := SchemaRef{
			Value: Schema{
				Name:    actualSchemaName,
				Types:   schemaTypes,
				Domains: schemaDomains,
				Tables:  schemaTables,
			},
			Ref: nil,
		}
		info.Schemas = append(info.Schemas, schema)
	}
	return
}

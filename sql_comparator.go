package dragonfly

import (
	"database/sql"
	_ "github.com/lib/pq"
	"go/token"
	"io"
	"math/rand"
	"strings"
	"time"
)

const (
	sqlGetSchemaStruct = `
select table_name, ordinal_position, column_name, data_type, character_maximum_length, column_default, is_nullable != 'NO', numeric_precision, numeric_precision_radix, numeric_scale, domain_schema, domain_name, udt_name
from information_schema.columns where table_schema=$1;`

	sqlGetDomains = `
select d.domain_schema, d.domain_name, d.data_type, d.character_maximum_length, d.domain_default, d.numeric_precision,
       d.numeric_precision_radix, d.numeric_scale, d.udt_name, d.udt_name,
       not exists((select true from pg_type where typnotnull and typtype='d' and typname=d.domain_name limit 1))
  from information_schema.domains d
 where d.domain_schema = $1;`

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

	sqlGetColumnFromForeignTable = `
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
  join information_schema.columns c
    on c.table_name = ccu.table_name
   and c.table_schema = ccu.table_schema
 where tc.constraint_type='FOREIGN KEY'
   and tc.table_schema=$1
   and tc.table_name=$2
   and c.column_name = $3;`

	sqlGetTableConstraints = `
select tc.constraint_name,
       kcu.column_name
from information_schema.table_constraints as tc
join information_schema.key_column_usage as kcu
  on tc.constraint_name = kcu.constraint_name
 and tc.table_schema = kcu.table_schema
 where tc.table_schema=$1
   and tc.table_name=$2;`

	sqlDomainUsages = `
select cdu.table_schema, cdu.table_name, cdu.column_name
  from information_schema.domains d
  join information_schema.column_domain_usage cdu
    on cdu.domain_schema = d.domain_schema
   and cdu.domain_name = d.domain_name
where d.domain_schema = $1
  and d.domain_name = $2;`
)

type (
	ColumnStruct struct {
		RelationName string
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
		Used         bool
	}
	TableStruct []ColumnStruct
)

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

func getForeignColumns(db *sql.DB, mainTableSchema, mainTableName, foreignColumn string) []ForeignKeyInformation {
	var keys []ForeignKeyInformation
	if q, err := db.Query(sqlGetColumnFromForeignTable, mainTableSchema, mainTableName, foreignColumn); err != nil {
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

func getSchemaTables(db *sql.DB, schema string) TableStruct {
	var columns TableStruct
	if q, err := db.Query(sqlGetSchemaStruct, schema); err != nil {
		panic(err)
	} else {
		columns = make([]ColumnStruct, 0, 100)
		var column ColumnStruct
		for q.Next() {
			if err := q.Scan(
				&column.RelationName,
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
	return columns
}

func getTableConstraints(db *sql.DB, schema, table string) ColumnConstraints {
	if q, err := db.Query(sqlGetTableConstraints, schema, table); err != nil {
		panic(err)
	} else {
		constraints := make(ColumnConstraints, 0, 10)
		var constraint ColumnConstraint
		for q.Next() {
			if err := q.Scan(
				&constraint.ColumnName,
				&constraint.ConstraintName,
			); err != nil {
				panic(err)
			} else {
				constraints = append(constraints, constraint)
			}
		}
		return constraints
	}
}

func getDomainUsages(db *sql.DB, schema, domain string) []ColumnFullName {
	if q, err := db.Query(sqlDomainUsages, schema, domain); err != nil {
		panic(err)
	} else {
		usages := make([]ColumnFullName, 0, 60)
		var usage ColumnFullName
		for q.Next() {
			if err := q.Scan(
				&usage.TableSchema,
				&usage.TableName,
				&usage.ColumnName,
			); err != nil {
				panic(err)
			} else {
				usages = append(usages, usage)
			}
		}
		return usages
	}
}

func getSchemaDomains(db *sql.DB, schema string) TableStruct {
	var columns TableStruct
	if q, err := db.Query(sqlGetDomains, schema); err != nil {
		panic(err)
	} else {
		columns = make([]ColumnStruct, 0, 100)
		var column ColumnStruct
		for q.Next() {
			if err := q.Scan(
				&column.DomainSchema,
				&column.Domain,
				&column.Type,
				&column.Max,
				&column.Default,
				&column.Precision,
				&column.Radix,
				&column.Scale,
				&column.RelationName,
				&column.UdtName,
				&column.Nullable,
			); err != nil {
				panic(err)
			} else {
				columns = append(columns, column)
			}
		}
	}
	return columns
}

func makeDomainsComparator(db *sql.DB, schema string, newDomains map[string]DomainSchema, root *Root) DomainsComparator {
	domains := make(DomainsComparator, 0, len(newDomains))
	currDomains := getSchemaDomains(db, schema)
	for i := range currDomains {
		currDomains[i].Used = false
	}
	for domainName := range newDomains {
		domainStruct := newDomains[domainName]
		if current, i := currDomains.findDomain(schema, domainName); i > -1 {
			domains = append(domains, DomainComparator{
				Name: NameComparator{
					Actual: *current.Domain,
					New:    domainName,
				},
				Schema: NameComparator{
					Actual: schema,
					New:    schema,
				},
				DomainStruct: DomainStructComparator{
					OldStructure: current,
					NewStructure: &domainStruct,
				},
			})
			// we prefer to remove the found entity from the collection,
			// to search without intersections with existing relationships
			// but there is an effect on the reference values, therefore use the key Used
			currDomains[i].Used = true
		} else {
			// not found
			domains = append(domains, DomainComparator{
				Name: NameComparator{
					Actual: "",
					New:    domainName,
				},
				Schema: NameComparator{
					Actual: schema,
					New:    schema,
				},
				DomainStruct: DomainStructComparator{
					OldStructure: nil,
					NewStructure: &domainStruct,
				},
			})
		}
	}
	// considering that I deleted the found, now in this collection I have everything that remains
	for i, current := range currDomains {
		if current.Used {
			continue
		}
		var found = false
		// these are either superfluous or renamed domains
		for index, diff := range domains {
			bindTheseDomains := func() {
				found = true
				domains[index] = DomainComparator{
					Name: NameComparator{
						Actual: *current.Domain,
						New:    diff.Name.New,
					},
					Schema: NameComparator{
						Actual: *current.DomainSchema,
						New:    diff.Schema.New,
					},
					DomainStruct: DomainStructComparator{
						OldStructure: &current,
						NewStructure: diff.DomainStruct.NewStructure,
					},
				}
			}
			// try to find among those that have a new name
			if diff.Name.Actual == "" && strings.EqualFold(diff.DomainStruct.NewStructure.Type, current.UdtName) {
				// we must make sure our assumption is correct
				matches := make(map[string]int, 5)
				usages := getDomainUsages(db, *current.DomainSchema, *current.Domain)
				for _, usage := range usages {
					var column Column
					if root.follow(root, []string{
						"schemas",
						usage.TableSchema,
						"tables",
						usage.TableName,
						"columns",
						usage.ColumnName,
					}, &column) {
						// it is the same domain
						// TODO domain schema?
						if _, d, ok := column.Schema.makeDomainName(); ok && strings.EqualFold(d, diff.Name.New) {
							if _, ok := matches[strings.ToLower(d)]; ok {
								matches[strings.ToLower(d)] += 1
							} else {
								matches[strings.ToLower(d)] = 1
							}
						}
					}
				}
				keys, vals := sortMap(matches).getSortedKeysValues()
				if len(keys) == 1 {
					bindTheseDomains()
					break
				} else if len(keys) > 1 {
					if vals[0] > vals[1]*2 {
						bindTheseDomains()
						break
					}
				}
			}
		}
		if !found {
			// domains to deleting
			domains = append(domains, DomainComparator{
				Name: NameComparator{
					Actual: *current.Domain,
					New:    "",
				},
				Schema: NameComparator{
					Actual: *current.DomainSchema,
					New:    "",
				},
				DomainStruct: DomainStructComparator{
					OldStructure: &currDomains[i],
					NewStructure: nil,
				},
			})
		}
	}
	return domains
}

func makeColumnsComparator(db *sql.DB, schemaName, tableName string, table TableClass, currColumns TableStruct) ColumnsComparator {
	var (
		tableConstraints ColumnConstraints
		columns          = make(ColumnsComparator, 0, 0)
	)
	for ci, column := range table.Columns {
		var comparator ColumnComparator
		comparator.TableName = tableName
		comparator.SchemaName = schemaName
		comparator.Name.New = column.Value.Name
		comparator.NewStruct = &table.Columns[ci]
		if actualColumn, i := currColumns.findColumn(tableName, column.Value.Name); i > -1 {
			comparator.Name.Actual = actualColumn.Column
			comparator.ActualStruct = actualColumn
			// cannot be deleted, there is an effect on the reference values
			currColumns[i].Used = true
		} else if tableConstraints == nil {
			tableConstraints = getTableConstraints(db, schemaName, tableName)
		}
		columns = append(columns, comparator)
	}
	// not matched
	for i, actualColumn := range currColumns {
		if actualColumn.Used {
			continue
		}
		var matches = make(map[string]int, 0)
		for _, column := range columns {
			if column.ActualStruct == nil {
				// comparing data types
				if domainSchema, domainName, ok := column.NewStruct.Value.Schema.makeDomainName(); ok {
					if actualColumn.Domain == nil || !strings.EqualFold(domainName, *actualColumn.Domain) || (actualColumn.DomainSchema != nil && !strings.EqualFold(domainSchema, *actualColumn.DomainSchema)) {
						continue
					}
				} else {
					if !strings.EqualFold(column.NewStruct.Value.Schema.Value.Type, actualColumn.UdtName) {
						continue
					}
				}
				// comparing data length
				if !(column.NewStruct.Value.Schema.Value.Length == nil && actualColumn.Max == nil) &&
					((column.NewStruct.Value.Schema.Value.Length != nil && actualColumn.Max == nil) ||
						(column.NewStruct.Value.Schema.Value.Length == nil && actualColumn.Max != nil) ||
						(*column.NewStruct.Value.Schema.Value.Length != *actualColumn.Max)) {
					continue
				}
				// comparing not null != nullable
				if column.NewStruct.Value.Schema.Value.NotNull == actualColumn.Nullable {
					continue
				}
				// comparing constraints if exists
				var foundSameConstraint = false
				for _, constraint := range tableConstraints {
					if strings.EqualFold(constraint.ColumnName, actualColumn.Column) {
						for _, newConstraint := range column.NewStruct.Value.Constraints {
							if strings.EqualFold(newConstraint.Name, constraint.ConstraintName) {
								foundSameConstraint = true
								break
							}
						}
						for _, newConstraint := range table.Constraints {
							if iArrayContains(newConstraint.Columns, constraint.ColumnName) && strings.EqualFold(newConstraint.Constraint.Name, constraint.ConstraintName) {
								foundSameConstraint = true
								break
							}
						}
						if foundSameConstraint {
							break
						}
					}
				}
				if foundSameConstraint {
					matches[column.Name.New] = 1
					break
				} else {
					matches[column.Name.New] = 0
				}
			}
		}
		keys, vals := sortMap(matches).getSortedKeysValues()
		if len(keys) == 1 || (len(keys) > 1 && vals[0] > 0) {
			// we can assume that we found what we need
			for ci, column := range columns {
				if strings.EqualFold(column.Name.New, keys[0]) {
					columns[ci].Name.Actual = actualColumn.Column
					columns[ci].ActualStruct = &currColumns[i]
				}
			}
		} else {
			// for deleting only
			columns = append(columns, ColumnComparator{
				TableName:  tableName,
				SchemaName: schemaName,
				Name: NameComparator{
					Actual: actualColumn.Column,
					New:    "",
				},
				ActualStruct: &currColumns[i],
				NewStruct:    nil,
			})
		}
	}
	return columns
}

func makeTablesComparator(db *sql.DB, schema string, tables map[string]TableClass) TablesComparator {
	tablesComparator := make(TablesComparator, 0, 0)
	allColumns := getSchemaTables(db, schema)
	for tableName, tableStruct := range tables {
		var comparator TableComparator
		actualColumns := allColumns.filterTableColumns(tableName)
		comparator.ColumnsComparator = makeColumnsComparator(db, schema, tableName, tableStruct, actualColumns)
		tablesComparator = append(tablesComparator, comparator)
	}
	return tablesComparator
}

func (c *SchemaRef) makeSolution(
	db *sql.DB,
	schema string,
	root *Root,
	w io.Writer,
) (
	install []SqlStmt,
	afterInstall []SqlStmt,
) {
	install = make([]SqlStmt, 0, 0)
	afterInstall = make([]SqlStmt, 0, 0)
	domains := makeDomainsComparator(db, schema, c.Value.Domains, root)
	for _, domain := range domains {
		first, second := domain.makeSolution()
		install = append(install, first...)
		afterInstall = append(afterInstall, second...)
	}
	tables := makeTablesComparator(db, schema, c.Value.Tables)
	for _, table := range tables {
		first, second := table.makeSolution(db)
		install = append(install, first...)
		afterInstall = append(afterInstall, second...)
	}
	return
}

type (
	ColumnFullName struct {
		TableSchema string
		TableName   string
		ColumnName  string
	}
	ColumnConstraint struct {
		ColumnName     string
		ConstraintName string
	}
	ColumnConstraints []ColumnConstraint
)

type (
	NameComparator struct {
		Actual string
		New    string
	}
	ColumnComparator struct {
		TableName, SchemaName string
		Name                  NameComparator
		ActualStruct          *ColumnStruct
		NewStruct             *ColumnRef
	}
	ColumnsComparator []ColumnComparator

	TableStructComparator struct {
		OldStructure *TableStruct
		NewStructure *TableClass
	}
	TableComparator struct {
		Name              NameComparator
		Schema            NameComparator
		TableStruct       TableStructComparator
		ColumnsComparator ColumnsComparator
	}
	TablesComparator []TableComparator

	DomainStructComparator struct {
		OldStructure *ColumnStruct
		NewStructure *DomainSchema
	}
	DomainComparator struct {
		Name         NameComparator
		Schema       NameComparator
		DomainStruct DomainStructComparator
	}
	DomainsComparator []DomainComparator
)

func (c DomainComparator) makeSolution() (install []SqlStmt, afterInstall []SqlStmt) {
	install = make([]SqlStmt, 0, 0)
	afterInstall = make([]SqlStmt, 0, 0)
	if c.DomainStruct.OldStructure == nil {
		install = append(install, makeDomain(c.Schema.New, c.Name.New, *c.DomainStruct.NewStructure))
		return
	}
	if c.DomainStruct.NewStructure == nil {
		afterInstall = append(afterInstall, makeDomainDrop(c.Schema.Actual, c.Name.Actual))
		return
	}
	if !strings.EqualFold(c.Schema.New, c.Schema.Actual) {
		install = append(install, makeDomainSetSchema(c.Name.Actual, c.Schema))
	}
	if !strings.EqualFold(c.Name.New, c.Name.Actual) {
		install = append(install, makeDomainRename(c.Schema.New, c.Name))
	}
	if (c.DomainStruct.NewStructure.NotNull && c.DomainStruct.OldStructure.Nullable) ||
		(!c.DomainStruct.NewStructure.NotNull && !c.DomainStruct.OldStructure.Nullable) {
		install = append(install, makeDomainSetNotNull(c.Schema.New, c.Name.New, c.DomainStruct.NewStructure.NotNull))
	}
	if !(c.DomainStruct.NewStructure.Default == nil && c.DomainStruct.OldStructure.Default == nil) &&
		((c.DomainStruct.NewStructure.Default == nil && c.DomainStruct.OldStructure.Default != nil) ||
			(c.DomainStruct.NewStructure.Default != nil && c.DomainStruct.OldStructure.Default == nil) ||
			(*c.DomainStruct.NewStructure.Default != *c.DomainStruct.OldStructure.Default)) {
		install = append(install, makeDomainSetDefault(c.Schema.New, c.Name.New, c.DomainStruct.NewStructure.Default))
	}
	return
}

func (c TableComparator) makeSolution(db *sql.DB) (install []SqlStmt, afterInstall []SqlStmt) {
	install = make([]SqlStmt, 0, 0)
	afterInstall = make([]SqlStmt, 0, 0)
	// TODO drop constraints
	if !strings.EqualFold(c.Schema.Actual, c.Schema.New) {
		install = append(install, makeTableSetSchema(c.Name.Actual, c.Schema))
	}
	if !strings.EqualFold(c.Name.Actual, c.Name.New) {
		install = append(install, makeTableRename(c.Schema.New, c.Name))
	}
	for _, columnComparator := range c.ColumnsComparator {
		first, second := columnComparator.makeSolution(db)
		install = append(install, first...)
		afterInstall = append(afterInstall, second...)
	}
	// TODO add constraints (afterinstall)
	return
}

func (c ColumnComparator) makeSolution(db *sql.DB) (install []SqlStmt, afterInstall []SqlStmt) {
	/*
		TODO make two modes: soft and hard
		  in soft mode we can alter table:
				1.	alter domain reqCode drop not null;
				2.	alter table geo.regions add column country_code reqCode;
				3.	update geo.regions r set country_code = (select c.code from geo.countries c where c.id = r.country_id);
				4.	alter domain reqCode set not null;
		  in hard mode we must create temporary table and do something like that:
				1.	create temporary table;
				2.	insert into temporary table all records from source;
				3.	drop (with foreign constraints) and recreate source table with another structure;
				4.	fill new table and restore all constraints;
	*/
	install = make([]SqlStmt, 0, 0)
	afterInstall = make([]SqlStmt, 0, 0)
	if c.Name.Actual == "" {
		domainSchema, domainName, isDomain := c.NewStruct.Value.Schema.makeDomainName()
		makeValidateNotNull := false
		if c.NewStruct.Value.Schema.Value.NotNull && c.NewStruct.Value.Schema.Value.Default == nil {
			if isDomain {
				install = append(install, makeDomainSetNotNull(domainSchema, domainName, false))
			} else {
				c.NewStruct.Value.Schema.Value.NotNull = false
			}
			makeValidateNotNull = true
		}
		install = append(install, makeColumnAdd(c.SchemaName, c.TableName, *c.NewStruct))
		if makeValidateNotNull {
			install = append(install, fixEmptyColumn(db, c.SchemaName, c.TableName, *c.NewStruct)...)
			if isDomain {
				install = append(install, makeDomainSetNotNull(domainSchema, domainName, true))
			} else {
				c.NewStruct.Value.Schema.Value.NotNull = false
				install = append(install, makeColumnAlterSetNotNull(domainSchema, domainName, c.NewStruct.Value.Name, true))
			}
		}
		return
	}
	if c.Name.New == "" {
		afterInstall = append(afterInstall, makeColumnDrop(c.SchemaName, c.TableName, c.Name.Actual, true, true))
		return
	}
	if !strings.EqualFold(c.Name.Actual, c.Name.New) {
		install = append(install, makeColumnRename(c.SchemaName, c.TableName, c.Name))
	}
	// TODO
	//  ALTER COLUMN
	return
}

func fixEmptyColumn(db *sql.DB, schema, table string, column ColumnRef) []SqlStmt {
	// this is the case when we change the data type of the relationship between the dependent and dependent table
	// simply put, the foreign key is changing
	for _, constraint := range column.Value.Constraints {
		if constraint.Type == ConstraintForeignKey {
			if fk, ok := constraint.Parameters.Parameter.(ForeignKey); ok {
				foreignTable := fk.ToTable
				foreignSchema := schema
				if tab := strings.Split(fk.ToTable, "."); len(tab) > 1 {
					foreignTable = tab[1]
					foreignSchema = tab[0]
				}
				existingFK := getForeignKeys(db, schema, table, foreignSchema, foreignTable)
				srcColumn := fk.ToColumn
				if len(existingFK) == 0 {
					existingFK = getForeignColumns(db, schema, table, column.Value.Name)
					srcColumn = column.Value.Name
				}
				if len(existingFK) == 1 {
					eFk := existingFK[0]
					return []SqlStmt{
						&UpdateStmt{
							Table: TableDesc{
								Table: &Selector{
									Name:      table,
									Container: schema,
								},
								Alias: "dest",
							},
							Set: []SqlExpr{
								&BinaryExpr{
									Left: &Literal{Text: column.Value.Name},
									Right: &BracketBlock{
										Statement: &SelectStmt{
											Columns: []SqlExpr{
												&UnaryExpr{Ident: &Literal{Text: srcColumn}},
											},
											From: TableDesc{
												Table: &Selector{
													Name:      eFk.ForeignTable.TableName,
													Container: eFk.ForeignTable.SchemaName,
												},
												Alias: "",
											},
											Where: &BinaryExpr{
												Left: &Literal{Text: eFk.ForeignTable.ColumnName},
												Right: &UnaryExpr{
													Ident: &Selector{
														Name:      eFk.MainTable.ColumnName,
														Container: "dest",
													},
												},
												Op: token.ASSIGN,
											},
										},
									},
									Op: token.ASSIGN,
								},
							},
							Where: nil,
						},
					}
				}
			}
		}
	}
	return nil
}

func (c TableStruct) findDomain(domainSchema, domainName string) (*ColumnStruct, int) {
	for i, s := range c {
		if s.Used {
			continue
		}
		if s.DomainSchema != nil && strings.EqualFold(*s.DomainSchema, domainSchema) &&
			s.Domain != nil && strings.EqualFold(*s.Domain, domainName) {
			return &c[i], i
		}
	}
	return nil, -1
}

func (c TableStruct) findColumn(tableName, columnName string) (*ColumnStruct, int) {
	for i, s := range c {
		if s.Used {
			continue
		}
		if strings.EqualFold(s.RelationName, tableName) && strings.EqualFold(s.Column, columnName) {
			return &c[i], i
		}
	}
	return nil, -1
}

func (c TableStruct) filterTableColumns(tableName string) TableStruct {
	filtered := make(TableStruct, 0, 20)
	for i, s := range c {
		if strings.EqualFold(s.RelationName, tableName) {
			filtered = append(filtered, c[i])
		}
	}
	return filtered
}

func DatabaseDiff(root *Root, schemaName, dbConnectionString string, w io.Writer) {
	var (
		db, err = sql.Open("postgres", dbConnectionString)
	)
	if err != nil {
		panic(err)
	}

	install := make([]SqlStmt, 0, 0)
	afterInstall := make([]SqlStmt, 0, 0)
	for _, schema := range root.Schemas {
		if schemaName == "" || schemaName == schema.Value.Name {
			first, second := schema.makeSolution(db, schema.Value.Name, root, w)
			install = append(install, first...)
			afterInstall = append(afterInstall, second...)
		}
	}

	writer(w, "/*\n    BEGIN OF UPDATE SCRIPT\n    SCHEMA FILTER: %s\n*/", schemaName)
	writer(w, "\n/* SECTION INSTALL %s */", strings.Repeat("=", 58))
	for _, stmt := range install {
		writer(w, "\n/* statement: %s */\n%s;\n", stmt.GetComment(), stmt.MakeStmt())
	}
	writer(w, "\n/* SECTION AFTER INSTALL %s */", strings.Repeat("=", 52))
	for _, stmt := range afterInstall {
		writer(w, "\n/* statement: %s */\n%s;\n", stmt.GetComment(), stmt.MakeStmt())
	}
	writer(w, "\n/* END OF UPDATE SCRIPT %s */", strings.Repeat("=", 53))
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

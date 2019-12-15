package dragonfly

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"go/token"
	"io"
	"math/rand"
	"net/url"
	"strings"
	"time"
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

func makeUnusedDomainsComparator(
	db *ActualSchemas,
	schemaName string,
	newDomainName string,
	newDomain DomainSchema,
	root *Root,
) (
	comparator *DomainComparator,
) {
	domains := make(map[string]DomainStruct, 50)
	matches := make(map[string]int, 50)
	// considering that I deleted the found, now in this collection I have everything that remains
	for _, actualDomain := range db.getUnusedDomains() {
		key := fmt.Sprintf("%s.%s", actualDomain.DomainSchema, actualDomain.Domain)
		domains[key] = actualDomain
		matches[key] = 0
		// try to find among those that have a new name
		if !strings.EqualFold(newDomain.Type, actualDomain.UdtName) {
			continue
		}
		if newDomain.NotNull == actualDomain.Nullable {
			continue
		}
		usages := db.getDomainUsages(actualDomain.DomainSchema, actualDomain.Domain)
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
				if s, d, ok := column.Schema.makeDomainName(); ok && strings.EqualFold(d, newDomainName) && strings.EqualFold(s, schemaName) {
					if strings.EqualFold(schemaName, actualDomain.DomainSchema) {
						matches[key] += 2
					} else {
						matches[key] += 1
					}
				}
			}
		}
	}
	keys, vals := sortMap(matches).getSortedKeysValues()
	if len(keys) == 1 || (len(keys) > 1 && vals[0] > vals[1]*2) {
		actualDomain := domains[keys[0]]
		*actualDomain.Used = true
		return &DomainComparator{
			Name: NameComparator{
				Actual: actualDomain.Domain,
				New:    newDomainName,
			},
			Schema: NameComparator{
				Actual: actualDomain.DomainSchema,
				New:    schemaName,
			},
			DomainStruct: DomainStructComparator{
				OldStructure: &actualDomain,
				NewStructure: &newDomain,
			},
		}
	} else {
		// new domain
		return &DomainComparator{
			Name: NameComparator{
				Actual: "",
				New:    newDomainName,
			},
			Schema: NameComparator{
				Actual: "",
				New:    schemaName,
			},
			DomainStruct: DomainStructComparator{
				OldStructure: nil,
				NewStructure: &newDomain,
			},
		}
	}
}

func makeDomainsComparator(
	db *ActualSchemas,
	schema string,
	newDomains map[string]DomainSchema,
) (
	domains DomainsComparator,
	postpone []string,
) {
	domains = make(DomainsComparator, 0, len(newDomains)) // matched
	postpone = make([]string, 0, len(newDomains))         // not matched
	for domainName := range newDomains {
		var (
			newDomain = newDomains[domainName] // copy because we need a reference value
			oldDomain = db.getUnusedDomainAndSetItAsUsed(schema, domainName)
		)
		if oldDomain != nil {
			domains = append(domains, DomainComparator{
				Name: NameComparator{
					Actual: oldDomain.Domain,
					New:    domainName,
				},
				Schema: NameComparator{
					Actual: schema,
					New:    schema,
				},
				DomainStruct: DomainStructComparator{
					OldStructure: oldDomain,
					NewStructure: &newDomain,
				},
			})
		} else {
			postpone = append(postpone, domainName)
		}
	}
	return
}

func makeTablesComparator(
	db *ActualSchemas,
	schema string,
	tables map[string]TableClass,
) (
	tablesComparator TablesComparator,
	postpone []string,
) {
	tablesComparator = make(TablesComparator, 0, len(tables)) // matched
	postpone = make([]string, 0, 0)                           // not matched
	for tableName, tableStruct := range tables {
		var (
			newStruct = tables[tableName]
			oldStruct = db.getUnusedTableAndSetItAsUsed(schema, tableName)
		)
		if oldStruct != nil {
			tablesComparator = append(tablesComparator, TableComparator{
				Name: NameComparator{
					Actual: oldStruct.Name,
					New:    tableName,
				},
				Schema: NameComparator{
					Actual: oldStruct.Schema,
					New:    schema,
				},
				TableStruct: TableStructComparator{
					OldStructure: oldStruct,
					NewStructure: &newStruct,
				},
				ColumnsComparator: makeColumnsComparator(db, schema, tableName, tableStruct, *oldStruct),
			})
		} else {
			// new tables
			postpone = append(postpone, tableName)
		}
	}
	return
}

func (c *TableClass) getAllColumnConstraints(columnName string) []Constraint {
	var constraints = make([]Constraint, 0, 0)
	for i, constraint := range c.Constraints {
		if iArrayContains(constraint.Columns, columnName) {
			constraints = append(constraints, c.Constraints[i].Constraint)
		}
	}
	if column, ok := c.Columns.tryToFind(columnName); ok {
		constraints = append(constraints, column.Value.Constraints...)
	}
	return constraints
}

func makeColumnsComparator(
	db *ActualSchemas,
	schemaName, tableName string,
	table TableClass,
	currTable TableStruct,
) (
	columns ColumnsComparator,
) {
	columns = make(ColumnsComparator, 0, len(currTable.Columns))
	for ci, column := range table.Columns {
		var (
			comparator ColumnComparator
			oldColumn  = db.getUnusedColumnAndSetItAsUsed(schemaName, tableName, column.Value.Name)
		)
		comparator.TableName = tableName
		comparator.SchemaName = schemaName
		comparator.Name.New = column.Value.Name
		comparator.NewStruct = &table.Columns[ci]
		if oldColumn != nil {
			comparator.Name.Actual = oldColumn.Column
			comparator.ActualStruct = oldColumn
		}
		columns = append(columns, comparator)
	}
	// not matched
	unusedColumns := db.getUnusedColumns(schemaName, tableName)
	for i, actualColumn := range unusedColumns {
		var matches = make(map[string]int, 0)
		for _, column := range columns {
			if column.ActualStruct != nil {
				continue
			}
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
			actualConstraints := db.getColumnConstraints(schemaName, tableName, actualColumn.Column)
			newConstraints := table.getAllColumnConstraints(column.Name.New)
			if itHaveSameConstraints(actualConstraints, newConstraints) {
				matches[column.Name.New] = 1
			} else {
				matches[column.Name.New] = 0
			}
		}
		keys, vals := sortMap(matches).getSortedKeysValues()
		if len(keys) == 1 || (len(keys) > 1 && vals[0] > 0) {
			// we can assume that we found what we need
			for ci, column := range columns {
				if strings.EqualFold(column.Name.New, keys[0]) {
					columns[ci].Name.Actual = actualColumn.Column
					columns[ci].ActualStruct = &unusedColumns[i]
					*unusedColumns[i].Used = true // TODO maybe method?
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
				ActualStruct: &unusedColumns[i],
				NewStruct:    nil,
			})
		}
	}
	return columns
}

func itHaveSameConstraints(constraints []TableConstraint, constraints2 []Constraint) bool {
	for _, constraint1 := range constraints {
		for _, constraint2 := range constraints2 {
			if strings.EqualFold(constraint1.ConstraintName, constraint2.Name) {
				return true
			}
			if strings.EqualFold(constraint1.ConstraintType, "primary key") && constraint2.Type == ConstraintPrimaryKey {
				return true
			}
			if constraint1.ForeignKey != nil && constraint2.Type == ConstraintForeignKey {
				if fk, ok := constraint2.Parameters.Parameter.(ForeignKey); ok {
					if strings.EqualFold(fk.ToTable, constraint1.ForeignKey.ForeignTable.TableName) && strings.EqualFold(fk.ToColumn, constraint1.ForeignKey.ForeignTable.ColumnName) {
						return true
					}
				}
			}
		}
	}
	return false
}

type (
	postponedObjects struct {
		domains []string
		tables  []string
	}
)

func (c *SchemaRef) diffKnown(
	db *ActualSchemas,
	schema string,
	root *Root,
	w io.Writer,
) (
	install []SqlStmt,
	afterInstall []SqlStmt,
	postponed postponedObjects,
) {
	install = make([]SqlStmt, 0, 0)
	afterInstall = make([]SqlStmt, 0, 0)
	domains, domainsPostponed := makeDomainsComparator(db, schema, c.Value.Domains) // TODO postponed
	postponed.domains = domainsPostponed
	for _, domain := range domains {
		first, second := domain.makeSolution()
		install = append(install, first...)
		afterInstall = append(afterInstall, second...)
	}
	tables, tablesPostponed := makeTablesComparator(db, schema, c.Value.Tables) // TODO postponed
	postponed.tables = tablesPostponed
	for _, table := range tables {
		first, second := table.makeSolution(db)
		install = append(install, first...)
		afterInstall = append(afterInstall, second...)
	}
	return
}

func (c *SchemaRef) diffPostponed(
	postponed postponedObjects,
	db *ActualSchemas,
	schema string,
	root *Root,
	w io.Writer,
) (
	install []SqlStmt,
	afterInstall []SqlStmt,
) {
	install = make([]SqlStmt, 0, 0)
	afterInstall = make([]SqlStmt, 0, 0)

	for _, domainName := range postponed.domains {
		domain, ok := c.Value.Domains[domainName]
		if !ok {
			panic("something went wrong. check the domain name character case mistakes")
		}
		if comparator := makeUnusedDomainsComparator(db, schema, domainName, domain, root); comparator != nil {
			first, second := comparator.makeSolution()
			install = append(install, first...)
			afterInstall = append(afterInstall, second...)
		}
	}
	// TODO table postponed comparator
	return
}

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
		OldStructure *DomainStruct
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

func (c TableComparator) makeSolution(
	db *ActualSchemas,
) (
	install []SqlStmt,
	afterInstall []SqlStmt,
) {
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

func (c ColumnComparator) makeSolution(db *ActualSchemas) (install []SqlStmt, afterInstall []SqlStmt) {
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

func fixEmptyColumn(db *ActualSchemas, schema, table string, column ColumnRef) []SqlStmt {
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
				if existingFK := db.getForeignKey(schema, foreignSchema, table, foreignTable); existingFK != nil {
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
												&UnaryExpr{Ident: &Literal{Text: fk.ToColumn}},
											},
											From: TableDesc{
												Table: &Selector{
													Name:      existingFK.ForeignTable.TableName,
													Container: existingFK.ForeignTable.SchemaName,
												},
												Alias: "",
											},
											Where: &BinaryExpr{
												Left: &Literal{Text: existingFK.ForeignTable.ColumnName},
												Right: &UnaryExpr{
													Ident: &Selector{
														Name:      existingFK.MainTable.ColumnName,
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

type (
	ConnectionOptions struct {
		Driver   string
		UserName string
		Password string
		Host     string
		Database string
	}
)

func DatabaseDiff(root *Root, optionSchemaName string, options ConnectionOptions, w io.Writer) {
	dbConnectionString := fmt.Sprintf(
		"%s://%s:%s@%s/%s",
		options.Driver,
		url.QueryEscape(options.UserName),
		url.QueryEscape(options.Password),
		options.Host,
		options.Database,
	)
	var (
		db, err = sql.Open(options.Driver, dbConnectionString)
	)
	if err != nil {
		panic(err)
	}

	var (
		actualStructure        = GetAllDatabaseInformation(db, options.Database)
		install                = make([]SqlStmt, 0, 0)
		afterInstall           = make([]SqlStmt, 0, 0)
		postponedSchemaObjects = make(map[string]postponedObjects, 0)
	)
	for _, schema := range root.Schemas {
		// process all
		first, second, postponed := schema.diffKnown(&actualStructure, schema.Value.Name, root, w)
		postponedSchemaObjects[schema.Value.Name] = postponed
		// save needed
		if optionSchemaName == "" || strings.EqualFold(optionSchemaName, schema.Value.Name) {
			install = append(install, first...)
			afterInstall = append(afterInstall, second...)
		}
	}
	for _, schema := range root.Schemas {
		postponedSchema, ok := postponedSchemaObjects[schema.Value.Name]
		if !ok {
			continue
		}
		first, second := schema.diffPostponed(postponedSchema, &actualStructure, schema.Value.Name, root, w)
		// save needed
		if optionSchemaName == "" || strings.EqualFold(optionSchemaName, schema.Value.Name) {
			install = append(install, first...)
			afterInstall = append(afterInstall, second...)
		}
	}

	writer(w, "/*\n    BEGIN OF UPDATE SCRIPT\n    SCHEMA FILTER: %s\n*/", optionSchemaName)
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
	// TODO move from here
	rand.Seed(time.Now().UnixNano())
}

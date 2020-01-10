package dragonfly

import (
	"fmt"
	_ "github.com/lib/pq"
	"go/token"
	"math/rand"
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
	current *Root,
	schemaName string,
	newDomainName string,
	newDomain DomainSchema,
	new *Root,
) (
	comparator *DomainComparator,
) {
	domains := make(map[string]DomainSchema, 50)
	matches := make(map[string]int, 50)
	// considering that I deleted the found, now in this collection I have everything that remains
	for domainSchemaName, actualDomains := range current.getUnusedDomains() {
		for actualDomainName, actualDomain := range actualDomains {
			key := fmt.Sprintf("%s.%s", domainSchemaName, actualDomainName)
			domains[key] = actualDomain
			matches[key] = 0
			// try to find among those that have a new name
			if !strings.EqualFold(newDomain.Type, actualDomain.Type) {
				continue
			}
			if newDomain.NotNull != actualDomain.NotNull {
				continue
			}
			usages := current.getDomainUsages(domainSchemaName, actualDomainName)
			for _, usage := range usages {
				var column Column
				if new.follow(new, []string{
					"schemas",
					usage.TableSchema,
					"tables",
					usage.TableName,
					"columns",
					usage.ColumnName,
				}, &column) {
					// it is the same domain
					if s, d, ok := column.Schema.makeCustomType(); ok && strings.EqualFold(d, newDomainName) && strings.EqualFold(s, schemaName) {
						if strings.EqualFold(schemaName, domainSchemaName) {
							matches[key] += 2
						} else {
							matches[key] += 1
						}
					}
				}
			}
		}
	}
	keys, vals := sortMap(matches).getSortedKeysValues()
	if (len(keys) == 1 && vals[0] > 0) || (len(keys) > 1 && vals[0] > vals[1]*2) {
		actualDomainSchemaName, actualDomainName := strings.Split(keys[0], ".")[0], strings.Split(keys[0], ".")[1]
		actualDomain := domains[keys[0]]
		*actualDomain.used = true
		return &DomainComparator{
			Name: NameComparator{
				Actual: actualDomainName,
				New:    newDomainName,
			},
			Schema: NameComparator{
				Actual: actualDomainSchemaName,
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
	current *Root,
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
			oldDomain = current.getUnusedDomainAndSetItAsUsed(schema, domainName)
		)
		if oldDomain != nil {
			// both domains with same schema and name
			domains = append(domains, DomainComparator{
				Name: NameComparator{
					Actual: domainName,
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

func makeUnusedTablesComparator(
	current *Root,
	schemaName string,
	newTableName string,
	newTable Table,
) (
	comparator *TableComparator,
) {
	tables := make(map[string]Table, 50)
	matches := make(map[string]int, 50)
	for tableSchemaName, actualTables := range current.getUnusedTables() {
		for tableName, actualTable := range actualTables {
			key := fmt.Sprintf("%s.%s", tableSchemaName, tableName)
			matches[key] = 0
			tables[key] = actualTable
			for _, newColumn := range newTable.Columns {
				for _, actualColumn := range actualTable.Columns {
					if strings.EqualFold(newColumn.Value.Name, actualColumn.Value.Name) {
						matches[key] += 1
					}
				}
			}
			matches[key] -= len(newTable.Columns) - matches[key]
		}
	}
	keys, vals := sortMap(matches).getSortedKeysValues()
	if (len(keys) == 1 && vals[0] > 0) || (len(keys) > 1 && vals[0] > vals[1]*2) {
		actualSchemaName, actualTableName := strings.Split(keys[0], ".")[0], strings.Split(keys[0], ".")[1]
		actualTable := tables[keys[0]]
		*actualTable.used = true
		return &TableComparator{
			Name: NameComparator{
				Actual: actualTableName,
				New:    newTableName,
			},
			Schema: NameComparator{
				Actual: actualSchemaName,
				New:    schemaName,
			},
			TableStruct: TableStructComparator{
				OldStructure: &actualTable,
				NewStructure: &newTable,
			},
		}
	} else {
		// new table
		return &TableComparator{
			Name: NameComparator{
				Actual: "",
				New:    newTableName,
			},
			Schema: NameComparator{
				Actual: "",
				New:    schemaName,
			},
			TableStruct: TableStructComparator{
				OldStructure: nil,
				NewStructure: &newTable,
			},
		}
	}
}

func makeTypesComparator(
	current *Root,
	schema string,
	newTypes map[string]DomainSchema,
) (
	typesComparator []DomainComparator,
	postpone []string,
) {
	return
}

func makeTablesComparator(
	current *Root,
	schema string,
	tables map[string]Table,
) (
	tablesComparator TablesComparator,
	postpone []string,
) {
	tablesComparator = make(TablesComparator, 0, len(tables)) // matched
	postpone = make([]string, 0, 0)                           // not matched
	for tableName, tableStruct := range tables {
		var (
			newStruct = tables[tableName]
			oldStruct = current.getUnusedTableAndSetItAsUsed(schema, tableName)
		)
		if oldStruct != nil {
			// both tables has same name
			tablesComparator = append(tablesComparator, TableComparator{
				Name: NameComparator{
					Actual: tableName,
					New:    tableName,
				},
				Schema: NameComparator{
					Actual: schema,
					New:    schema,
				},
				TableStruct: TableStructComparator{
					OldStructure: oldStruct,
					NewStructure: &newStruct,
				},
				ColumnsComparator: makeColumnsComparator(current, schema, tableName, tableStruct, *oldStruct),
			})
		} else {
			// new tables
			postpone = append(postpone, tableName)
		}
	}
	return
}

func (c *Table) getAllColumnConstraints(columnName string) []Constraint {
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
	current *Root,
	schemaName, tableName string,
	table Table,
	currTable Table,
) (
	columns ColumnsComparator,
) {
	columns = make(ColumnsComparator, 0, len(currTable.Columns))
	for ci, column := range table.Columns {
		var (
			comparator ColumnComparator
			oldColumn  = current.getUnusedColumnAndSetItAsUsed(schemaName, tableName, column.Value.Name)
		)
		comparator.TableName = tableName
		comparator.SchemaName = schemaName
		comparator.Name.New = column.Value.Name
		comparator.NewStruct = &table.Columns[ci]
		if oldColumn != nil {
			// both columns with same schema and name
			comparator.Name.Actual = column.Value.Name
			comparator.ActualStruct = oldColumn
		}
		columns = append(columns, comparator)
	}
	// not matched
	unusedColumns := current.getUnusedColumns(schemaName, tableName)
	for i, actualColumn := range unusedColumns {
		var matches = make(map[string]int, 0)
		for _, column := range columns {
			if column.ActualStruct != nil {
				continue
			}
			// comparing data types
			if customSchema, customType, isCustom := column.NewStruct.Value.Schema.makeCustomType(); isCustom {
				if typeSchema, typeName, isCustom := actualColumn.Value.Schema.makeCustomType(); !isCustom || !strings.EqualFold(customType, typeName) || !strings.EqualFold(customSchema, typeSchema) {
					continue
				}
			} else {
				if !strings.EqualFold(column.NewStruct.Value.Schema.Value.Type, actualColumn.Value.Schema.Value.Type) {
					continue
				}
			}
			// comparing data length
			if !(column.NewStruct.Value.Schema.Value.Length == nil && actualColumn.Value.Schema.Value.Length == nil) &&
				((column.NewStruct.Value.Schema.Value.Length != nil && actualColumn.Value.Schema.Value.Length == nil) ||
					(column.NewStruct.Value.Schema.Value.Length == nil && actualColumn.Value.Schema.Value.Length != nil) ||
					(*column.NewStruct.Value.Schema.Value.Length != *actualColumn.Value.Schema.Value.Length)) {
				continue
			}
			// comparing not null != nullable
			if column.NewStruct.Value.Schema.Value.NotNull == !actualColumn.Value.Schema.Value.NotNull {
				continue
			}
			// comparing constraints if exists
			actualConstraints := current.getColumnConstraints(schemaName, tableName, actualColumn.Value.Name)
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
					columns[ci].Name.Actual = actualColumn.Value.Name
					columns[ci].ActualStruct = &unusedColumns[i]
					*unusedColumns[i].used = true // TODO maybe method?
				}
			}
		} else {
			// for deleting only
			columns = append(columns, ColumnComparator{
				TableName:  tableName,
				SchemaName: schemaName,
				Name: NameComparator{
					Actual: actualColumn.Value.Name,
					New:    "",
				},
				ActualStruct: &unusedColumns[i],
				NewStruct:    nil,
			})
		}
	}
	return columns
}

func itHaveSameConstraints(constraints, constraints2 []Constraint) bool {
	for _, constraint1 := range constraints {
		for _, constraint2 := range constraints2 {
			if strings.EqualFold(constraint1.Name, constraint2.Name) {
				return true
			}
			if constraint1.Type == ConstraintPrimaryKey && constraint2.Type == ConstraintPrimaryKey {
				return true
			}
			if constraint1.Type == ConstraintForeignKey && constraint2.Type == ConstraintForeignKey {
				fk1, ok1 := constraint1.Parameters.Parameter.(ForeignKey)
				fk2, ok2 := constraint2.Parameters.Parameter.(ForeignKey)
				if ok1 && ok2 {
					if strings.EqualFold(fk1.ToTable, fk2.ToTable) && strings.EqualFold(fk1.ToColumn, fk2.ToColumn) {
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
	current *Root,
	schema string,
	new *Root,
) (
	preInstall []SqlStmt,
	install []SqlStmt,
	afterInstall []SqlStmt,
	postponed postponedObjects,
) {
	preInstall = make([]SqlStmt, 0, 0)
	install = make([]SqlStmt, 0, 0)
	domains, domainsPostponed := makeDomainsComparator(current, schema, c.Value.Domains)
	postponed.domains = domainsPostponed
	for _, domain := range domains {
		first, second := domain.makeSolution()
		preInstall = append(preInstall, first...)
		afterInstall = append(afterInstall, second...)
	}
	customTypes, _ := makeTypesComparator(current, schema, c.Value.Types) // TODO postponed
	for _, domain := range customTypes {
		first, second := domain.makeSolution()
		preInstall = append(preInstall, first...)
		afterInstall = append(afterInstall, second...)
	}

	tables, tablesPostponed := makeTablesComparator(current, schema, c.Value.Tables)
	postponed.tables = tablesPostponed
	for _, table := range tables {
		first, second := table.makeSolution(current)
		preInstall = append(preInstall, first...)
		install = append(install, second...)
	}
	return
}

func (c *SchemaRef) diffPostponed(
	postponed postponedObjects,
	current *Root,
	schema string,
	new *Root,
) (
	preInstall []SqlStmt,
	install []SqlStmt,
	afterInstall []SqlStmt,
) {
	preInstall = make([]SqlStmt, 0, 0)
	install = make([]SqlStmt, 0, 0)
	afterInstall = make([]SqlStmt, 0, 0)
	for _, domainName := range postponed.domains {
		domain, ok := c.Value.Domains[domainName]
		if !ok {
			panic("something went wrong. check the domain name character case mistakes")
		}
		if comparator := makeUnusedDomainsComparator(current, schema, domainName, domain, new); comparator != nil {
			first, second := comparator.makeSolution()
			preInstall = append(preInstall, first...)
			afterInstall = append(afterInstall, second...)
		}
	}
	for _, tableName := range postponed.tables {
		table, ok := c.Value.Tables[tableName]
		if !ok {
			panic("something went wrong. check the table name character case mistakes")
		}
		if comparator := makeUnusedTablesComparator(current, schema, tableName, table); comparator != nil {
			first, second := comparator.makeSolution(current)
			install = append(install, first...)
			afterInstall = append(afterInstall, second...)
		}
	}
	// TODO table postponed comparator
	return
}

func (c *SchemaRef) prepareDeleting(
	current *Root,
	schema string,
	new *Root,
) (
	preInstall []SqlStmt,
	install []SqlStmt,
	afterInstall []SqlStmt,
) {
	preInstall = make([]SqlStmt, 0, 0)
	install = make([]SqlStmt, 0, 0)
	afterInstall = make([]SqlStmt, 0, 0)
	// TODO drop tables
	for domainSchemaName, unusedDomains := range current.getUnusedDomains() {
		for domainName, unusedDomain := range unusedDomains {
			if strings.EqualFold(domainSchemaName, schema) {
				comparator := DomainComparator{
					Name: NameComparator{
						Actual: domainName,
						New:    "",
					},
					Schema: NameComparator{
						Actual: domainSchemaName,
						New:    "",
					},
					DomainStruct: DomainStructComparator{
						OldStructure: &unusedDomain,
						NewStructure: nil,
					},
				}
				first, second := comparator.makeSolution()
				preInstall = append(preInstall, first...)
				afterInstall = append(afterInstall, second...)
			}
		}
	}
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
		ActualStruct          *ColumnRef
		NewStruct             *ColumnRef
	}
	ColumnsComparator []ColumnComparator

	TableStructComparator struct {
		OldStructure *Table
		NewStructure *Table
	}
	TableComparator struct {
		Name              NameComparator
		Schema            NameComparator
		TableStruct       TableStructComparator
		ColumnsComparator ColumnsComparator
	}
	TablesComparator []TableComparator

	DomainStructComparator struct {
		OldStructure *DomainSchema
		NewStructure *DomainSchema
	}
	DomainComparator struct {
		Name         NameComparator
		Schema       NameComparator
		DomainStruct DomainStructComparator
	}
	DomainsComparator []DomainComparator
)

/*
	pre-install: create domain, alter domain (except 'set not null')
	post-install: drop domain and 'set not null'
*/
func (c DomainComparator) makeSolution() (preInstall []SqlStmt, postInstall []SqlStmt) {
	preInstall = make([]SqlStmt, 0, 0)
	postInstall = make([]SqlStmt, 0, 0)
	if c.DomainStruct.OldStructure == nil {
		preInstall = append(preInstall, makeDomain(c.Schema.New, c.Name.New, *c.DomainStruct.NewStructure))
		return
	}
	if c.DomainStruct.NewStructure == nil {
		postInstall = append(postInstall, makeDomainDrop(c.Schema.Actual, c.Name.Actual))
		return
	}
	if !strings.EqualFold(c.Schema.New, c.Schema.Actual) {
		preInstall = append(preInstall, makeDomainSetSchema(c.Name.Actual, c.Schema))
	}
	if !strings.EqualFold(c.Name.New, c.Name.Actual) {
		preInstall = append(preInstall, makeDomainRename(c.Schema.New, c.Name))
	}
	if c.DomainStruct.NewStructure.NotNull && !c.DomainStruct.OldStructure.NotNull {
		postInstall = append(postInstall, makeDomainSetNotNull(c.Schema.New, c.Name.New, true))
	} else if !c.DomainStruct.NewStructure.NotNull && c.DomainStruct.OldStructure.NotNull {
		preInstall = append(preInstall, makeDomainSetNotNull(c.Schema.New, c.Name.New, false))
	}
	if !(c.DomainStruct.NewStructure.Default == nil && c.DomainStruct.OldStructure.Default == nil) &&
		((c.DomainStruct.NewStructure.Default == nil && c.DomainStruct.OldStructure.Default != nil) ||
			(c.DomainStruct.NewStructure.Default != nil && c.DomainStruct.OldStructure.Default == nil) ||
			(*c.DomainStruct.NewStructure.Default != *c.DomainStruct.OldStructure.Default)) {
		preInstall = append(preInstall, makeDomainSetDefault(c.Schema.New, c.Name.New, c.DomainStruct.NewStructure.Default))
	}
	return
}

func (c TableComparator) makeSolution(
	current *Root,
) (
	install []SqlStmt,
	afterInstall []SqlStmt,
) {
	install = make([]SqlStmt, 0, 0)
	afterInstall = make([]SqlStmt, 0, 0)
	if c.Schema.Actual == "" && c.Schema.New != "" {
		install = append(install, makeTableCreate(c.Schema.New, c.Name.New, *c.TableStruct.NewStructure))
		return
	}
	if c.Schema.Actual != "" && c.Schema.New == "" {
		install = append(install, makeTableDrop(c.Schema.Actual, c.Name.Actual))
		return
	}
	// TODO drop constraints
	if !strings.EqualFold(c.Schema.Actual, c.Schema.New) {
		install = append(install, makeTableSetSchema(c.Name.Actual, c.Schema))
	}
	if !strings.EqualFold(c.Name.Actual, c.Name.New) {
		install = append(install, makeTableRename(c.Schema.New, c.Name))
	}
	if c.ColumnsComparator != nil {
		for _, columnComparator := range c.ColumnsComparator {
			first, second := columnComparator.makeSolution(current)
			install = append(install, first...)
			afterInstall = append(afterInstall, second...)
		}
	}
	// TODO add constraints (afterinstall)
	return
}

func (c ColumnComparator) makeSolution(current *Root) (install []SqlStmt, afterInstall []SqlStmt) {
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
		customSchema, customType, isCustom := c.NewStruct.Value.Schema.makeCustomType()
		makeValidateNotNull := false
		if c.NewStruct.Value.Schema.Value.NotNull && c.NewStruct.Value.Schema.Value.Default == nil {
			if isCustom {
				install = append(install, makeDomainSetNotNull(customSchema, customType, false))
			} else {
				c.NewStruct.Value.Schema.Value.NotNull = false
			}
			makeValidateNotNull = true
		}
		install = append(install, makeColumnAdd(c.SchemaName, c.TableName, *c.NewStruct))
		if makeValidateNotNull {
			install = append(install, fixEmptyColumn(current, c.SchemaName, c.TableName, *c.NewStruct)...)
			if isCustom {
				install = append(install, makeDomainSetNotNull(customSchema, customType, true))
			} else {
				c.NewStruct.Value.Schema.Value.NotNull = false
				install = append(install, makeColumnAlterSetNotNull(customSchema, customType, c.NewStruct.Value.Name, true))
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

func fixEmptyColumn(current *Root, schema, table string, column ColumnRef) []SqlStmt {
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
				if existingFK := current.getForeignKey(schema, foreignSchema, table, foreignTable); existingFK != nil {
					fkSchema, fkTable := schema, existingFK.ToTable
					if tableSep := strings.Split(existingFK.ToTable, "."); len(tableSep) > 1 {
						fkSchema, fkTable = tableSep[0], tableSep[1]
					}
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
													Name:      fkSchema,
													Container: fkTable,
												},
												Alias: "",
											},
											Where: &BinaryExpr{
												Left: &Literal{Text: existingFK.ToColumn},
												Right: &UnaryExpr{
													Ident: &Selector{
														Name:      existingFK.ToColumn,
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

func init() {
	// TODO move from here
	rand.Seed(time.Now().UnixNano())
}

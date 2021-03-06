package dragonfly

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	sqt "github.com/iv-menshenin/sql-ast"
	_ "github.com/lib/pq"
	"go/token"
	"math/rand"
	"reflect"
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
				if new.follow([]string{
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
	var result = DomainComparator{
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
	utils.SomethingMatched(
		matches,
		func(matchedName string) {
			result.Schema.Actual, result.Name.Actual = strings.Split(matchedName, ".")[0], strings.Split(matchedName, ".")[1]
			actualDomain := domains[matchedName]
			*actualDomain.used = true
			result.DomainStruct.OldStructure = &actualDomain
		},
	)
	return &result
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
	var result = TableComparator{
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
	utils.SomethingMatched(
		matches,
		func(matchedName string) {
			result.Schema.Actual, result.Name.Actual = strings.Split(matchedName, ".")[0], strings.Split(matchedName, ".")[1]
			actualTable := tables[matchedName]
			*actualTable.used = true
			result.TableStruct.OldStructure = &actualTable
		},
	)
	return &result
}

func makeTypesComparator(
	current *Root,
	schema string,
	newTypes map[string]TypeSchema,
) (
	typesComparator []TypeComparator,
	postpone []string,
) {
	for userTypeName := range newTypes {
		var (
			newType = newTypes[userTypeName]
			oldType = current.getUnusedTypeAndSetItAsUsed(schema, userTypeName)
		)
		if oldType != nil {
			// both domains with same schema and name
			typesComparator = append(typesComparator, TypeComparator{
				Name: NameComparator{
					Actual: userTypeName,
					New:    userTypeName,
				},
				Schema: NameComparator{
					Actual: schema,
					New:    schema,
				},
				TypeStruct: TypeStructComparator{
					OldStructure: oldType,
					NewStructure: &newType,
				},
			})
		} else {
			postpone = append(postpone, userTypeName)
		}
	}
	return
}

func makeUnusedTypesComparator(
	current *Root,
	schemaName string,
	newTypeName string,
	newType TypeSchema,
) (
	comparator *TypeComparator,
) {
	types := make(map[string]TypeSchema, 50)
	matches := make(map[string]int, 50)
	for tableSchemaName, actualTypes := range current.getUnusedTypes() {
		for typeName, actualType := range actualTypes {
			key := fmt.Sprintf("%s.%s", tableSchemaName, typeName)
			matches[key] = 0
			if strings.EqualFold(typeName, newTypeName) {
				matches[key] = 1 + len(newType.Fields)
			}
			types[key] = actualType
			for _, newColumn := range newType.Fields {
				for _, actualColumn := range actualType.Fields {
					if strings.EqualFold(newColumn.Value.Name, actualColumn.Value.Name) {
						matches[key] += 1
					}
				}
			}
			matches[key] -= len(newType.Fields) - matches[key]
		}
	}
	var result = TypeComparator{
		Name: NameComparator{
			Actual: "",
			New:    newTypeName,
		},
		Schema: NameComparator{
			Actual: "",
			New:    schemaName,
		},
		TypeStruct: TypeStructComparator{
			OldStructure: nil,
			NewStructure: &newType,
		},
	}
	utils.SomethingMatched(
		matches,
		func(matchedName string) {
			result.Schema.Actual, result.Name.Actual = strings.Split(matchedName, ".")[0], strings.Split(matchedName, ".")[1]
			actualType := types[matchedName]
			*actualType.used = true
			result.TypeStruct.OldStructure = &actualType
		},
	)
	return &result
}

func makeTablesComparator(
	current, new *Root,
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
				ColumnsComparator: makeColumnsComparator(current, new, schema, tableName, tableStruct, *oldStruct),
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
		if utils.ArrayContainsCI(constraint.Columns, columnName) {
			constraints = append(constraints, c.Constraints[i].Constraint)
		}
	}
	if column, ok := c.Columns.tryToFind(columnName); ok {
		constraints = append(constraints, column.Value.Constraints...)
	}
	return constraints
}

func makeColumnsComparator(
	current, new *Root,
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
		if t := strings.Split(column.Value.Schema.Value.Type, "."); len(t) == 2 {
			var colType TypeSchema
			if new.follow([]string{"schemas", t[0], "types", t[1]}, &colType) {
				if strings.EqualFold(colType.Type, "json") || strings.EqualFold(colType.Type, "map") {
					// JSON normalization
					table.Columns[ci].Value.Schema.Value.Type = "json"
				}
			}
		}
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
		keys, vals := utils.SortMap(matches).GetSortedKeysValues()
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
		types   []string
	}
)

func (c *SchemaRef) diffKnown(
	current *Root,
	schema string,
	new *Root,
) (
	preInstall []sqt.SqlStmt,
	install []sqt.SqlStmt,
	afterInstall []sqt.SqlStmt,
	postponed postponedObjects,
) {
	preInstall = make([]sqt.SqlStmt, 0, 0)
	install = make([]sqt.SqlStmt, 0, 0)
	preInstall = append(preInstall, &sqt.CreateStmt{
		Target: sqt.TargetSchema,
		Name:   &sqt.Literal{Text: schema},
		IfNotX: true,
	})
	domains, domainsPostponed := makeDomainsComparator(current, schema, c.Value.Domains)
	postponed.domains = domainsPostponed
	for _, domain := range domains {
		first, second := domain.makeSolution()
		preInstall = append(preInstall, first...)
		afterInstall = append(afterInstall, second...)
	}
	customTypes, typesPostponed := makeTypesComparator(current, schema, c.Value.Types)
	postponed.types = typesPostponed
	for _, customType := range customTypes {
		first, second := customType.makeSolution(current)
		preInstall = append(preInstall, first...)
		afterInstall = append(afterInstall, second...)
	}

	tables, tablesPostponed := makeTablesComparator(current, new, schema, c.Value.Tables)
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
	preInstall []sqt.SqlStmt,
	install []sqt.SqlStmt,
	afterInstall []sqt.SqlStmt,
) {
	preInstall = make([]sqt.SqlStmt, 0, 0)
	install = make([]sqt.SqlStmt, 0, 0)
	afterInstall = make([]sqt.SqlStmt, 0, 0)
	for _, domainName := range postponed.domains {
		domain, ok := c.Value.Domains[domainName]
		if !ok {
			panic("something went wrong")
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
			panic("something went wrong")
		}
		if comparator := makeUnusedTablesComparator(current, schema, tableName, table); comparator != nil {
			first, second := comparator.makeSolution(current)
			install = append(install, first...)
			afterInstall = append(afterInstall, second...)
		}
	}
	for _, customTypeName := range postponed.types {
		customType, ok := c.Value.Types[customTypeName]
		if !ok {
			panic("something went wrong")
		}
		if comparator := makeUnusedTypesComparator(current, schema, customTypeName, customType); comparator != nil {
			first, second := comparator.makeSolution(current)
			preInstall = append(preInstall, first...)
			afterInstall = append(afterInstall, second...)
		}
	}
	return
}

func (c *SchemaRef) prepareDeleting(
	current *Root,
	schema string,
	new *Root,
) (
	preInstall []sqt.SqlStmt,
	install []sqt.SqlStmt,
	afterInstall []sqt.SqlStmt,
) {
	preInstall = make([]sqt.SqlStmt, 0, 0)
	install = make([]sqt.SqlStmt, 0, 0)
	afterInstall = make([]sqt.SqlStmt, 0, 0)
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
	TypeStructComparator struct {
		OldStructure *TypeSchema
		NewStructure *TypeSchema
	}
	DomainComparator struct {
		Name         NameComparator
		Schema       NameComparator
		DomainStruct DomainStructComparator
	}
	DomainsComparator []DomainComparator
	TypeComparator    struct {
		Name       NameComparator
		Schema     NameComparator
		TypeStruct TypeStructComparator
	}
	TypesComparator []TypeComparator

	migrationAction int8
)

const (
	matchedElement migrationAction = iota
	alterElement
	createElement
	dropElement
)

func compareDefault(old, new interface{}) migrationAction {
	if old == nil && new == nil {
		return matchedElement
	}
	if old == nil && new != nil {
		return createElement
	}
	if old != nil && new == nil {
		return dropElement
	}
	oldV, newV := reflect.ValueOf(old), reflect.ValueOf(new)
	if old != nil && oldV.Kind() == reflect.Ptr {
		oldV = oldV.Elem()
	}
	if new != nil && newV.Kind() == reflect.Ptr {
		newV = newV.Elem()
	}
	if fmt.Sprintf("%T:%v", oldV, oldV) == fmt.Sprintf("%T:%v", newV, newV) {
		return matchedElement
	}
	return alterElement
}

/*
	pre-install: create domain, alter domain (except 'set not null')
	post-install: drop domain and 'set not null'
*/
func (c DomainComparator) makeSolution() (preInstall []sqt.SqlStmt, postInstall []sqt.SqlStmt) {
	preInstall = make([]sqt.SqlStmt, 0, 0)
	postInstall = make([]sqt.SqlStmt, 0, 0)
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
	switch compareDefault(c.DomainStruct.NewStructure.Default, c.DomainStruct.OldStructure.Default) {
	case alterElement:
		preInstall = append(preInstall, makeDomainSetDefault(c.Schema.New, c.Name.New, c.DomainStruct.NewStructure.Default))
	case createElement:
		preInstall = append(preInstall, makeDomainSetDefault(c.Schema.New, c.Name.New, c.DomainStruct.NewStructure.Default))
	case dropElement:
		preInstall = append(preInstall, makeDomainSetDefault(c.Schema.New, c.Name.New, nil))
	}
	return
}

func (c TypeComparator) makeSolution(current *Root) (preInstall []sqt.SqlStmt, postInstall []sqt.SqlStmt) {
	/*
		TODO: for types that already uses we must do something like this:
			create type [schema].tmp_[name] as ([field1], [field2], ...);
			alter table [related_table] add [column]_tmp1 [schema].tmp_[name];
			update [related_table] set [column]_tmp1 = row(([column]).[field1], ([column]).[field2], ...) where [column] is not null;
			alter table [related_table] drop [column];
			*** ALTERING TYPE ***
			alter table [related_table] add [column] [schema].[name];
			update [related_table] set [column] = row(([column]_tmp1).lat, ([column]_tmp1).lng) where [column]_tmp1 is not null;
			alter table [related_table] drop [column]_tmp1;
			alter table [related_table] alter [column] set not null;
			drop type [schema].tmp_[name]

	*/
	preInstall = make([]sqt.SqlStmt, 0, 0)
	postInstall = make([]sqt.SqlStmt, 0, 0)
	// https://www.postgresql.org/docs/9.1/sql-createtype.html
	if c.TypeStruct.OldStructure == nil {
		preInstall = append(preInstall, makeType(c.Schema.New, c.Name.New, *c.TypeStruct.NewStructure))
		return
	}
	if c.TypeStruct.NewStructure == nil {
		postInstall = append(postInstall, makeTypeDrop(c.Schema.Actual, c.Name.Actual))
		return
	}
	// https://www.postgresql.org/docs/9.1/sql-altertype.html
	if !strings.EqualFold(c.Schema.New, c.Schema.Actual) {
		preInstall = append(preInstall, makeTypeSetSchema(c.Name.Actual, c.Schema))
	}
	if !strings.EqualFold(c.Name.New, c.Name.Actual) {
		preInstall = append(preInstall, makeTypeRename(c.Schema.New, c.Name))
	}
	for _, s := range c.TypeStruct.NewStructure.Fields {
		if f, ok := c.TypeStruct.OldStructure.Fields.tryToFind(s.Value.Name); ok {
			*f.used = true
			*s.used = true
			if schemaType, nameType, ok := s.Value.Schema.makeCustomType(); ok {
				// compare domain name
				fieldDomain := fmt.Sprintf("%s.%s", schemaType, nameType)
				if !strings.EqualFold(fieldDomain, f.Value.Schema.Value.TypeBase.Type) {
					preInstall = append(preInstall, makeTypeAlterAttributeDataType(c.Schema.New, c.Name.New, s.Value.Name, TypeBase{Type: fieldDomain}))
				}
			} else {
				if !isMatchedTypes(f.Value.Schema.Value.TypeBase, s.Value.Schema.Value.TypeBase) {
					preInstall = append(preInstall, makeTypeAlterAttributeDataType(c.Schema.New, c.Name.New, s.Value.Name, s.Value.Schema.Value.TypeBase))
				}
			}
		}
		// ALTER
	}
	// TODO
	//  ADD ATTRIBUTE
	//  ADD VALUE
	return
}

func (c TableComparator) makeSolution(
	current *Root,
) (
	install []sqt.SqlStmt,
	afterInstall []sqt.SqlStmt,
) {
	install = make([]sqt.SqlStmt, 0, 0)
	afterInstall = make([]sqt.SqlStmt, 0, 0)
	columnConstraints := make(TableConstraints, 0, 4)
	for _, column := range c.TableStruct.NewStructure.Columns {
		for i := range column.Value.Constraints {
			columnConstraints = append(columnConstraints, ConstraintSchema{
				Columns:    []string{column.Value.Name},
				Constraint: column.Value.Constraints[i],
			})
		}
	}
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
	allConstraints := append(c.TableStruct.NewStructure.Constraints, columnConstraints...)
	for _, constraint := range allConstraints {
		if exists, ok := c.TableStruct.OldStructure.Constraints.tryToFind(constraint.Constraint.Name); ok {
			// TODO if used?
			*exists.Constraint.used = true
			*constraint.Constraint.used = true
			// TODO merge
		} else {
			afterInstall = append(afterInstall, &sqt.AlterStmt{
				Target: sqt.TargetTable,
				Name: &sqt.Selector{
					Name:      c.Name.New,
					Container: c.Schema.New,
				},
				Alter: makeAddConstraintExpr(constraint.Columns, constraint.Constraint),
			})
		}
	}
	for _, constraint := range c.TableStruct.OldStructure.Constraints {
		if !*constraint.Constraint.used {
			install = append(install, makeConstraintDropStmt(
				c.Schema.New,
				c.Name.New,
				constraint.Constraint.Name,
				true,
				true,
			))
		}
	}
	return
}

func (c ColumnComparator) makeSolution(current *Root) (install []sqt.SqlStmt, afterInstall []sqt.SqlStmt) {
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
	install = make([]sqt.SqlStmt, 0, 0)
	afterInstall = make([]sqt.SqlStmt, 0, 0)
	if c.Name.Actual == "" {
		install = append(install, makeColumnAdd(c.SchemaName, c.TableName, *c.NewStruct))
		// TODO not for domains
		if c.NewStruct.Value.Schema.Value.Default != nil {
			install = append(install, makeAlterColumnSetDefault(c.SchemaName, c.TableName, c.Name.New, c.NewStruct.Value.Schema.Value.Default))
		}
		if c.NewStruct.Value.Schema.Value.NotNull {
			if c.NewStruct.Value.Schema.Value.Default != nil {
				install = append(install, makeUpdateWholeColumnStatement(c.SchemaName, c.TableName, c.Name.New, c.NewStruct.Value.Schema.Value.Default))
			}
			install = append(install, makeAlterColumnSetNotNull(c.SchemaName, c.TableName, c.Name.New, true))
		}
		return
	}
	if c.Name.New == "" {
		afterInstall = append(afterInstall, makeColumnDropStmt(c.SchemaName, c.TableName, c.Name.Actual, true, true))
		return
	}
	if !strings.EqualFold(c.Name.Actual, c.Name.New) {
		install = append(install, makeColumnRename(c.SchemaName, c.TableName, c.Name))
	}
	if typeSchema, typeName, ok := c.NewStruct.Value.Schema.makeCustomType(); ok {
		if _, oldTypeName, ok := c.ActualStruct.Value.Schema.makeCustomType(); ok {
			// strings.EqualFold(typeSchema, oldTypeSchema) &&
			// TODO need to resolve schema changes?
			if !strings.EqualFold(typeName, oldTypeName) {
				install = append(install, makeAlterColumnSetDomain(c.SchemaName, c.TableName, c.Name.New, fmt.Sprintf("%s.%s", typeSchema, typeName)))
			}
		} else {
			install = append(install, makeAlterColumnSetDomain(c.SchemaName, c.TableName, c.Name.New, fmt.Sprintf("%s.%s", typeSchema, typeName)))
		}
	} else {
		if !isMatchedTypes(c.NewStruct.Value.Schema.Value.TypeBase, c.ActualStruct.Value.Schema.Value.TypeBase) {
			install = append(install, makeAlterColumnSetType(c.SchemaName, c.TableName, c.Name.New, c.NewStruct.Value.Schema.Value))
		}
	}
	// TODO
	//  ALTER COLUMN
	//  bool to timestamp: alter table [schema].[table] alter column [name] type timestamptz using case when [name] then now() end;
	return
}

func fixEmptyColumn(current *Root, schema, table string, column ColumnRef) []sqt.SqlStmt {
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
				if fromColumn, existingFK := current.getForeignKey(schema, foreignSchema, table, foreignTable); existingFK != nil {
					fkSchema, fkTable := schema, existingFK.ToTable
					if tableSep := strings.Split(existingFK.ToTable, "."); len(tableSep) > 1 {
						fkSchema, fkTable = tableSep[0], tableSep[1]
					}
					return []sqt.SqlStmt{
						&sqt.UpdateStmt{
							Table: sqt.TableDesc{
								Table: &sqt.Selector{
									Name:      table,
									Container: schema,
								},
								Alias: "dest",
							},
							Set: []sqt.SqlExpr{
								&sqt.BinaryExpr{
									Left: &sqt.Literal{Text: column.Value.Name},
									Right: &sqt.BracketBlock{
										Statement: &sqt.SelectStmt{
											Columns: []sqt.SqlExpr{
												&sqt.UnaryExpr{Ident: &sqt.Literal{Text: fk.ToColumn}},
											},
											From: sqt.TableDesc{
												Table: &sqt.Selector{
													Name:      fkTable,
													Container: fkSchema,
												},
												Alias: "",
											},
											Where: &sqt.BinaryExpr{
												Left: &sqt.Literal{Text: existingFK.ToColumn},
												Right: &sqt.UnaryExpr{
													Ident: &sqt.Selector{
														Name:      fromColumn,
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
		ConnStr  string
	}
)

func init() {
	// TODO move from here
	rand.Seed(time.Now().UnixNano())
}

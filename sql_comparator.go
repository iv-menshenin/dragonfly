package dragonfly

import (
	"database/sql"
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
			currDomains = append(currDomains[:i], currDomains[i+1:]...)
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
	for _, current := range currDomains {
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
						if d, ok := column.Schema.makeDomainName(); ok && strings.EqualFold(d, diff.Name.New) {
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
					OldStructure: &current,
					NewStructure: nil,
				},
			})
		}
	}
	return domains
}

func (c *SchemaRef) checkDiff(db *sql.DB, schema string, root *Root, w io.Writer) {
	statements := make([]SqlStmt, 0, 100)
	domains := makeDomainsComparator(db, schema, c.Value.Domains, root)
	for _, domain := range domains {
		statements = append(statements, domain.makeSolution()...)
	}
	for _, stmt := range statements {
		writer(w, "\n/* statement: %s */\n%s\n", stmt.GetComment(), stmt.MakeStmt())
	}
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
	ColumnsComparator []ColumnComparator
	ColumnComparator  struct {
		Name         NameComparator
		ActualStruct *ColumnStruct
		NewStruct    *ColumnRef
	}
	TableStructComparator struct {
		OldStructure *TableStruct
		NewStructure *TableClass
	}
	TableComparator struct {
		Name             NameComparator
		Schema           NameComparator
		TableStruct      TableStructComparator
		ColumnComparator ColumnsComparator
	}

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

func (c DomainComparator) makeSolution() []SqlStmt {
	stmts := make([]SqlStmt, 0, 0)
	if c.DomainStruct.OldStructure == nil {
		stmts = append(stmts, makeDomain(c.Schema.New, c.Name.New, *c.DomainStruct.NewStructure))
		return stmts
	}
	if c.DomainStruct.NewStructure == nil {
		// TODO afterinstall
		stmts = append(stmts, makeDomainDrop(c.Schema.Actual, c.Name.Actual))
		return stmts
	}
	if !strings.EqualFold(c.Schema.New, c.Schema.Actual) {
		stmts = append(stmts, makeDomainRenameSchema(c.Schema.Actual, c.Name.Actual, c.Schema))
	}
	if !strings.EqualFold(c.Name.New, c.Name.Actual) {
		stmts = append(stmts, makeDomainRenameDomain(c.Schema.New, c.Name.Actual, c.Name))
	}
	if (c.DomainStruct.NewStructure.NotNull && c.DomainStruct.OldStructure.Nullable) ||
		(!c.DomainStruct.NewStructure.NotNull && !c.DomainStruct.OldStructure.Nullable) {
		stmts = append(stmts, makeDomainSetNotNull(c.Schema.New, c.Name.New, c.DomainStruct.NewStructure.NotNull))
	}
	if !(c.DomainStruct.NewStructure.Default == nil && c.DomainStruct.OldStructure.Default == nil) &&
		((c.DomainStruct.NewStructure.Default == nil && c.DomainStruct.OldStructure.Default != nil) ||
			(c.DomainStruct.NewStructure.Default != nil && c.DomainStruct.OldStructure.Default == nil) ||
			(*c.DomainStruct.NewStructure.Default != *c.DomainStruct.OldStructure.Default)) {
		stmts = append(stmts, makeDomainSetDefault(c.Schema.New, c.Name.New, c.DomainStruct.OldStructure.Default))
	}
	return stmts
}

func (c TableStruct) findDomain(domainSchema, domainName string) (*ColumnStruct, int) {
	for i, s := range c {
		if s.DomainSchema != nil && strings.EqualFold(*s.DomainSchema, domainSchema) &&
			s.Domain != nil && strings.EqualFold(*s.Domain, domainName) {
			return &s, i
		}
	}
	return nil, -1
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

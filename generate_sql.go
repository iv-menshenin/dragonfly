package dragonfly

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

func (c *DomainSchema) generateSQL(schemaName, domainName string, db *Root, w io.Writer) {
	writer(w, "create domain %s.%s %s;\n", schemaName, domainName, c.describeSQL())
}

func (c *SchemaRef) generateSQL(schemaName string, db *Root, w io.Writer) {
	writeHead := func(stage string) {
		writer(
			w,
			"\n/*\n\tSchema: %s %s\n\t%s %s\n*/\n",
			schemaName,
			strings.Repeat("*", 71-len(schemaName)),
			stage, strings.Repeat("*", 79-len(stage)),
		)
	}
	if schemaName != "public" {
		// do not create `public`
		writeHead("Creation")
		writer(w, "drop schema if exists %s cascade;\n", schemaName)
		writer(w, "create schema %s;\n", schemaName)
	}
	if len(c.Value.Domains) > 0 {
		writeHead("Domains")
	}
	for domainName, domain := range c.Value.Domains {
		domain.generateSQL(schemaName, domainName, db, w)
	}
	if len(c.Value.Tables) > 0 {
		writeHead("Tables")
	}
	for tableName, table := range c.Value.Tables {
		if table.Description != "" {
			writer(w, "-- description: %s\n", table.Description)
		}
		table.generateSQL(schemaName, tableName, db, w)
	}
}

func (c *ConstraintSchema) generateSQL(schemaName, tableName string, constraintIndex int, db *Root, w io.Writer) {
	writer(w, ",\n\t%s", c.Constraint.describeSQL(schemaName, tableName, c.Columns, constraintIndex))
}

func (c *TableClass) generateSQL(schemaName, tableName string, db *Root, w io.Writer) {
	writer(w, "create table %s.%s(\n", schemaName, tableName)
	for i, column := range c.Columns {
		column.generateSQL(schemaName, tableName, db, w)
		if i < len(c.Columns)-1 {
			writer(w, ",\n")
		}
	}
	for i, constraint := range c.Constraints {
		constraint.generateSQL(schemaName, tableName, i, db, w)
	}
	writer(w, "\n);\n")
}

func (c *ConstraintParameters) describeSQL(columns []string) string {
	var parameters string
	if c.Parameter != nil {
		if foreign, ok := c.Parameter.(ForeignKey); ok {
			if len(columns) > 0 {
				parameters += "(" + strings.Join(columns, ", ") + ")"
			}
			parameters += " references " + foreign.ToTable + "(" + foreign.ToColumn + ")"
			if foreign.OnDelete != nil {
				parameters += " on delete " + *foreign.OnDelete
			}
			if foreign.OnUpdate != nil {
				parameters += " on update " + *foreign.OnUpdate
			}
		} else if check, ok := c.Parameter.(Check); ok {
			parameters += "(" + check.Expression + ")"
		} else if where, ok := c.Parameter.(Where); ok {
			parameters += where.Where
		}
	}
	return parameters
}

func (r *Constraint) describeSQL(schemaName, tableName string, columns []string, constraintIndex int) string {
	constrType := r.Type
	var (
		foreignTable  string
		foreignColumn string
	)
	if fk, ok := r.Parameters.Parameter.(ForeignKey); ok {
		foreignTable = fk.ToTable
		foreignColumn = fk.ToColumn
	}
	constrName := evalTemplateParameters(
		r.Name,
		map[string]string{
			cNN:            strconv.Itoa(constraintIndex),
			cTable:         tableName,
			cSchema:        schemaName,
			cForeignTable:  foreignTable,
			cForeignColumn: foreignColumn,
		},
	)
	if strings.Count(constrName, "%s") == 1 {
		constrName = fmt.Sprintf(constrName, tableName)
	}
	if strings.Count(constrName, "%s") > 1 {
		constrName = fmt.Sprintf(constrName, schemaName, tableName)
	}
	var parameters = r.Parameters.describeSQL(columns)
	return "constraint " + constrName + " " + constrType + parameters
}

func (c *DomainSchema) describeSQL() interface{} {
	colType := c.Type
	if c.Length != nil {
		if c.Precision != nil {
			colType += "(" + strconv.Itoa(*c.Length) + "," + strconv.Itoa(*c.Precision) + ")"
		} else {
			colType += "(" + strconv.Itoa(*c.Length) + ")"
		}
	}
	nullable := " null"
	if c.NotNull {
		nullable = " not null"
	}
	defValue := ""
	if c.Default != nil {
		defValue = " default " + *c.Default
	}
	check := ""
	if c.Check != nil {
		check = " check(" + *c.Check + ")"
	}
	return colType + nullable + defValue + check
}

func (c *ColumnRef) generateSQL(schemaName, tableName string, db *Root, w io.Writer) {
	writer(w, "\t%s %s", c.Value.Name, c.Value.Schema.Value.describeSQL())
	for i, constraint := range c.Value.Constraints {
		writer(w, " %s", constraint.describeSQL(schemaName, tableName, nil, i))
	}
}

func GenerateSql(db *Root, schemaName string, w io.Writer) {
	for _, schema := range db.Schemas {
		if schemaName == "" || schemaName == schema.Value.Name {
			schema.generateSQL(schema.Value.Name, db, w)
		}
	}
}

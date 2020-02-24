package dragonfly

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	"io"
	"strconv"
	"strings"
)

func (c *DomainSchema) generateSQL(schemaName, domainName string, db *Root, w io.Writer) {
	utils.WriteWrapper(w, "create domain %s.%s %s;\n", schemaName, domainName, c.describeSQL())
}

func (c *SchemaRef) generateSQL(schemaName string, db *Root, w io.Writer) {
	writeHead := func(stage string) {
		utils.WriteWrapper(
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
		utils.WriteWrapper(w, "drop schema if exists %s cascade;\n", schemaName)
		utils.WriteWrapper(w, "create schema %s;\n", schemaName)
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
			utils.WriteWrapper(w, "-- description: %s\n", table.Description)
		}
		table.generateSQL(schemaName, tableName, db, w)
	}
}

func (c *ConstraintSchema) generateSQL(schemaName, tableName string, constraintIndex int, db *Root, w io.Writer) {
	utils.WriteWrapper(w, ",\n\t%s", c.Constraint.describeSQL(schemaName, tableName, c.Columns, constraintIndex))
}

func (c *Table) generateSQL(schemaName, tableName string, db *Root, w io.Writer) {
	utils.WriteWrapper(w, "create table %s.%s(\n", schemaName, tableName)
	for i, column := range c.Columns {
		column.generateSQL(schemaName, tableName, db, w)
		if i < len(c.Columns)-1 {
			utils.WriteWrapper(w, ",\n")
		}
	}
	for i, constraint := range c.Constraints {
		constraint.generateSQL(schemaName, tableName, i, db, w)
	}
	utils.WriteWrapper(w, "\n);\n")
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
	return fmt.Sprintf("constraint %s %s%s", r.Name, r.Type, r.Parameters.describeSQL(columns))
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

func (c *ColumnRef) describeSQL() string {
	if schema, customType, ok := c.Value.Schema.makeCustomType(); ok {
		return fmt.Sprintf("%s.%s", schema, customType)
	} else {
		return fmt.Sprintf("%s", c.Value.Schema.Value.describeSQL())
	}
}

func (c *ColumnRef) generateSQL(schemaName, tableName string, db *Root, w io.Writer) {
	utils.WriteWrapper(w, "\t%s %s", c.Value.Name, c.describeSQL())
	for i, constraint := range c.Value.Constraints {
		utils.WriteWrapper(w, " %s", constraint.describeSQL(schemaName, tableName, nil, i))
	}
}

// TODO deprecated, should be removed soon
func GenerateSql(db *Root, schemaName string, w io.Writer) {
	for _, schema := range db.Schemas {
		if schemaName == "" || schemaName == schema.Value.Name {
			schema.generateSQL(schema.Value.Name, db, w)
		}
	}
}

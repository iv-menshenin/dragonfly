package dragonfly

import (
	"fmt"
	"go/token"
	"strconv"
	"strings"
)

type (
	SqlTarget int
	Nullable  bool
	SetDrop   bool

	SqlStmt interface {
		GetComment() string
		MakeStmt() string
	}
	SqlIdent interface {
		GetName() string
	}
	SqlExpr interface {
		Expression() string
	}

	SqlNullable struct {
		Nullable Nullable
	}
	SqlRename struct {
		Target  SqlTarget
		OldName SqlIdent
		NewName SqlIdent
	}

	Default struct {
		Default SqlExpr
	}
	Literal struct {
		Text string
	}
	Selector struct {
		Name      string
		Container string
	}

	SchemaExpr struct {
		SchemaName string
	}
	SetMetadataExpr struct {
		Set SqlExpr
	}
	SetDropExpr struct {
		SetDrop SetDrop
		Expr    SqlExpr
	}
	DropExpr struct {
		Target            SqlTarget
		Name              SqlIdent
		IfExists, Cascade bool
	}
	AddExpr struct {
		Target     SqlTarget
		Name       SqlIdent
		Definition SqlExpr
	}
	AlterExpr struct {
		Target SqlTarget
		Name   SqlIdent
		Alter  SqlExpr
	}
	BinaryExpr struct {
		Left  SqlExpr
		Right SqlExpr
		Op    token.Token
	}
	UnaryExpr struct {
		Ident SqlIdent
	}
	BracketBlock struct {
		Expr      SqlExpr
		Statement SqlStmt
	}

	AlterStmt struct {
		Target SqlTarget
		Name   SqlIdent
		Alter  SqlExpr
	}
	CreateStmt struct {
		Target SqlTarget
		Name   SqlIdent
		Create SqlExpr
	}
	DropStmt struct {
		Target SqlTarget
		Name   SqlIdent
	}
	UpdateStmt struct {
		Table TableDesc
		Set   []SqlExpr
		Where SqlExpr
	}
	SelectStmt struct {
		Columns []SqlExpr
		From    TableDesc
		Where   SqlExpr
	}

	TableDesc struct {
		Table SqlIdent
		Alias string
	}

	TypeDescription struct {
		Type              string
		Length, Precision *int
		Null              Nullable
		Default, Check    *string
	}
)

const (
	TargetNone SqlTarget = iota
	TargetSchema
	TargetTable
	TargetColumn
	TargetDomain

	NullableNull    Nullable = true
	NullableNotNull Nullable = false

	SetDropDrop SetDrop = false
	SetDropSet  SetDrop = true
)

var (
	targetDescriptor = map[SqlTarget]string{
		TargetSchema: "schema",
		TargetTable:  "table",
		TargetColumn: "column",
		TargetDomain: "domain",
	}
)

func (c SqlTarget) String() string {
	if s, ok := targetDescriptor[c]; ok {
		return s
	}
	panic("unknown target %v")
}

func makeSetDropExpr(setDrop bool, expr SqlExpr) SqlExpr {
	return &SetDropExpr{
		SetDrop: SetDrop(setDrop),
		Expr:    expr,
	}
}

func makeAddColumnExpr(column ColumnRef) SqlExpr {
	return &AddExpr{
		Target:     TargetColumn,
		Name:       &Literal{Text: column.Value.Name},
		Definition: &Literal{Text: column.describeSQL()}, // TODO this is not literal
	}
}

func (c *Literal) GetName() string {
	return c.Text
}

func (c *Selector) GetName() string {
	return fmt.Sprintf("%s.%s", c.Container, c.Name)
}

func (c *AlterStmt) GetComment() string {
	return fmt.Sprintf("altering %s %s", c.Target, c.Name.GetName())
}

func (c *AlterStmt) MakeStmt() string {
	return fmt.Sprintf("alter %s %s %s", c.Target, c.Name.GetName(), c.Alter.Expression())
}

func (c *CreateStmt) GetComment() string {
	return fmt.Sprintf("creating %s %s", c.Target, c.Name.GetName())
}

func (c *CreateStmt) MakeStmt() string {
	return fmt.Sprintf("create %s %s %s", c.Target, c.Name.GetName(), c.Create.Expression())
}

func (c *DropStmt) GetComment() string {
	return fmt.Sprintf("deleting %s %s", c.Target, c.Name.GetName())
}

func (c *DropStmt) MakeStmt() string {
	return fmt.Sprintf("drop %s %s", c.Target, c.Name.GetName())
}

func (c *UpdateStmt) GetComment() string {
	return fmt.Sprintf("updating %s data", c.Table.Table.GetName())
}

func (c *UpdateStmt) MakeStmt() string {
	var (
		clauseSet   = make([]string, 0, len(c.Set))
		clauseWhere = "1 = 1"
	)
	for _, set := range c.Set {
		clauseSet = append(clauseSet, set.Expression())
	}
	if c.Where != nil {
		clauseWhere = c.Where.Expression()
	}
	return fmt.Sprintf("update %s %s set %s where %s", c.Table.Table.GetName(), c.Table.Alias, strings.Join(clauseSet, ", "), clauseWhere)
}

func (c *SelectStmt) GetComment() string {
	return fmt.Sprintf("select data from %s", c.From.Table.GetName())
}

func (c *SelectStmt) MakeStmt() string {
	var (
		clauseColumns = make([]string, 0, len(c.Columns))
		clauseWhere   = "1 = 1"
	)
	for _, col := range c.Columns {
		clauseColumns = append(clauseColumns, col.Expression())
	}
	if c.Where != nil {
		clauseWhere = c.Where.Expression()
	}
	return fmt.Sprintf("select %s from %s %s where %s", strings.Join(clauseColumns, ", "), c.From.Table.GetName(), c.From.Alias, clauseWhere)
}

func makeDomainSetSchema(domain string, rename NameComparator) SqlStmt {
	return &AlterStmt{
		Target: TargetDomain,
		Name: &Selector{
			Name:      domain,
			Container: rename.Actual,
		},
		Alter: &SetMetadataExpr{
			Set: &SchemaExpr{
				SchemaName: rename.New,
			},
		},
	}
}

func makeDomainRename(schema string, rename NameComparator) SqlStmt {
	return &AlterStmt{
		Target: TargetDomain,
		Name: &Selector{
			Name:      rename.Actual,
			Container: schema,
		},
		Alter: &SqlRename{
			NewName: &Literal{Text: rename.New},
		},
	}
}

func makeDomainSetNotNull(schema, domain string, notNull bool) SqlStmt {
	return &AlterStmt{
		Target: TargetDomain,
		Name: &Selector{
			Name:      domain,
			Container: schema,
		},
		Alter: makeSetDropExpr(notNull, &Literal{Text: "not null"}),
	}
}

func makeDomainSetDefault(schema, domain string, defaultValue *string) SqlStmt {
	if defaultValue == nil {
		return &AlterStmt{
			Target: TargetDomain,
			Name: &Selector{
				Name:      domain,
				Container: schema,
			},
			Alter: makeSetDropExpr(false, &Literal{Text: "default"}),
		}
	} else {
		return &AlterStmt{
			Target: TargetDomain,
			Name: &Selector{
				Name:      domain,
				Container: schema,
			},
			Alter: makeSetDropExpr(true, &Default{Default: &Literal{Text: *defaultValue}}),
		}
	}
}

func makeDomain(schema, domain string, domainSchema DomainSchema) SqlStmt {
	return &CreateStmt{
		Target: TargetDomain,
		Name: &Selector{
			Name:      domain,
			Container: schema,
		},
		Create: &TypeDescription{
			Type:      domainSchema.Type,
			Length:    domainSchema.Length,
			Precision: domainSchema.Precision,
			Null:      Nullable(!domainSchema.NotNull),
			Default:   domainSchema.Default,
			Check:     domainSchema.Check,
		},
	}
}

func makeDomainDrop(schema, domain string) SqlStmt {
	return &DropStmt{
		Target: TargetDomain,
		Name: &Selector{
			Name:      domain,
			Container: schema,
		},
	}
}

/* COLUMNS */

func makeColumnRename(schema, table string, name NameComparator) SqlStmt {
	return &AlterStmt{
		Target: TargetTable,
		Name: &Selector{
			Name:      table,
			Container: schema,
		},
		Alter: &SqlRename{
			Target:  TargetColumn,
			OldName: &Literal{Text: name.Actual},
			NewName: &Literal{Text: name.New},
		},
	}
}

func makeColumnAdd(schema, table string, column ColumnRef) SqlStmt {
	return &AlterStmt{
		Target: TargetTable,
		Name: &Selector{
			Name:      table,
			Container: schema,
		},
		Alter: makeAddColumnExpr(column),
	}
}

func makeColumnDrop(schema, table, column string, ifExists, cascade bool) SqlStmt {
	return &AlterStmt{
		Target: TargetTable,
		Name: &Selector{
			Name:      table,
			Container: schema,
		},
		Alter: &DropExpr{
			Target:   TargetColumn,
			Name:     &Literal{Text: column},
			IfExists: ifExists,
			Cascade:  cascade,
		},
	}
}

func makeColumnAlterSetNotNull(schema, table, column string, notNull bool) SqlStmt {
	return &AlterStmt{
		Target: TargetTable,
		Name: &Selector{
			Name:      table,
			Container: schema,
		},
		Alter: &AlterExpr{
			Target: TargetColumn,
			Name:   &Literal{Text: column},
			Alter:  makeSetDropExpr(notNull, &Literal{Text: "not null"}), // TODO not literal
		},
	}
}

/* TABLE */

func makeTableSetSchema(table string, rename NameComparator) SqlStmt {
	return &AlterStmt{
		Target: TargetTable,
		Name: &Selector{
			Name:      table,
			Container: rename.Actual,
		},
		Alter: &SetMetadataExpr{
			Set: &SchemaExpr{
				SchemaName: rename.New,
			},
		},
	}
}

func makeTableRename(schema string, rename NameComparator) SqlStmt {
	return &AlterStmt{
		Target: TargetTable,
		Name: &Selector{
			Name:      rename.Actual,
			Container: schema,
		},
		Alter: &SqlRename{
			NewName: &Literal{Text: rename.New},
		},
	}
}

func (c *SqlNullable) Expression() string {
	if c.Nullable == NullableNotNull {
		return "not null"
	} else {
		return "null"
	}
}

func (c *SetDropExpr) Expression() string {
	if c.SetDrop == SetDropSet {
		return "set " + c.Expr.Expression()
	} else {
		return "drop " + c.Expr.Expression()
	}
}

func (c *DropExpr) Expression() string {
	cascadeExpr, ifExistsExpr := "", ""
	if c.IfExists {
		ifExistsExpr = " if exists"
	}
	if c.Cascade {
		cascadeExpr = " cascade"
	}
	return fmt.Sprintf("drop %s %s %s%s", c.Target, ifExistsExpr, c.Name.GetName(), cascadeExpr)
}

func (c *AddExpr) Expression() string {
	return fmt.Sprintf("add %s %s %s", c.Target, c.Name.GetName(), c.Definition.Expression())
}

func (c *BinaryExpr) Expression() string {
	return fmt.Sprintf("%s%s%s", c.Left.Expression(), c.Op, c.Right.Expression())
}

func (c *UnaryExpr) Expression() string {
	return fmt.Sprintf("%s", c.Ident.GetName())
}

func (c *BracketBlock) Expression() string {
	if c.Expr != nil {
		if c.Statement != nil {
			panic("BracketBlock allows just Expr or Statement not both")
		}
		return fmt.Sprintf("(%s)", c.Expr.Expression())
	} else {
		if c.Statement == nil {
			panic("BracketBlock require Expr or Statement")
		}
		return fmt.Sprintf("(\n/* %s */\n%s\n)", c.Statement.GetComment(), c.Statement.MakeStmt())
	}
}

func (c *AlterExpr) Expression() string {
	return fmt.Sprintf("alter %s %s %s", c.Target, c.Name.GetName(), c.Alter.Expression())
}

func (c *SchemaExpr) Expression() string {
	return fmt.Sprintf("schema %s", c.SchemaName)
}

func (c *SetMetadataExpr) Expression() string {
	return fmt.Sprintf("set %s", c.Set.Expression())
}

func (c *Default) Expression() string {
	return fmt.Sprintf("default %s", c.Default.Expression())
}

func (c *SqlRename) Expression() string {
	if c.Target == TargetNone {
		return fmt.Sprintf("rename to %s", c.NewName.GetName())
	}
	return fmt.Sprintf("rename %s %s to %s", c.Target, c.OldName.GetName(), c.NewName.GetName())
}

func (c *Literal) Expression() string {
	return c.Text
}

func (c *TypeDescription) Expression() string {
	colType := c.Type
	if c.Length != nil {
		if c.Precision != nil {
			colType += "(" + strconv.Itoa(*c.Length) + "," + strconv.Itoa(*c.Precision) + ")"
		} else {
			colType += "(" + strconv.Itoa(*c.Length) + ")"
		}
	}
	nullable := " null"
	if c.Null == NullableNotNull {
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

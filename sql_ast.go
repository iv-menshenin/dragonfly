package dragonfly

import (
	"fmt"
	"strconv"
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
		OldName SqlIdent
		NewName SqlIdent
	}

	Default struct {
		Default SqlExpr
	}
	Literal struct {
		Text string
	}
	SelectorExpr struct {
		Name      string
		Container string
	}

	SetMetadataExpr struct {
		Set SqlExpr
	}
	SetDropExpr struct {
		SetDrop SetDrop
		Expr    SqlExpr
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
	TypeDescription struct {
		Type              string
		Length, Precision *int
		Null              Nullable
		Default, Check    *string
	}
)

const (
	TargetSchema SqlTarget = iota
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

func (c *Literal) GetName() string {
	return c.Text
}

func (c *SelectorExpr) GetName() string {
	return fmt.Sprintf("%s.%s", c.Container, c.Name)
}

func (c *AlterStmt) GetComment() string {
	return fmt.Sprintf("altering %s %s", c.Target, c.Name.GetName())
}

func (c *AlterStmt) MakeStmt() string {
	return fmt.Sprintf("alter %s %s %s;", c.Target, c.Name.GetName(), c.Alter.Expression())
}

func (c *CreateStmt) GetComment() string {
	return fmt.Sprintf("creating %s %s", c.Target, c.Name.GetName())
}

func (c *CreateStmt) MakeStmt() string {
	return fmt.Sprintf("create %s %s %s;", c.Target, c.Name.GetName(), c.Create.Expression())
}

func (c *DropStmt) GetComment() string {
	return fmt.Sprintf("deleting %s %s", c.Target, c.Name.GetName())
}

func (c *DropStmt) MakeStmt() string {
	return fmt.Sprintf("drop %s %s;", c.Target, c.Name.GetName())
}

func makeDomainRenameSchema(schema, domain string, rename NameComparator) SqlStmt {
	return &AlterStmt{
		Target: TargetDomain,
		Name: &SelectorExpr{
			Name:      domain,
			Container: schema,
		},
		Alter: &SetMetadataExpr{
			Set: &SqlNullable{
				Nullable: NullableNotNull,
			},
		},
	}
}

func makeDomainRenameDomain(schema, domain string, rename NameComparator) SqlStmt {
	return &AlterStmt{
		Target: TargetDomain,
		Name: &SelectorExpr{
			Name:      domain,
			Container: schema,
		},
		Alter: &SqlRename{
			OldName: &Literal{Text: rename.Actual},
			NewName: &Literal{Text: rename.New},
		},
	}
}

func makeDomainSetNotNull(schema, domain string, notNull bool) SqlStmt {
	return &AlterStmt{
		Target: TargetDomain,
		Name: &SelectorExpr{
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
			Name: &SelectorExpr{
				Name:      domain,
				Container: schema,
			},
			Alter: makeSetDropExpr(false, &Literal{Text: "default"}),
		}
	} else {
		return &AlterStmt{
			Target: TargetDomain,
			Name: &SelectorExpr{
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
		Name: &SelectorExpr{
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
		Name: &SelectorExpr{
			Name:      domain,
			Container: schema,
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

func (c *SetMetadataExpr) Expression() string {
	return fmt.Sprintf("set %s", c.Set.Expression())
}

func (c *Default) Expression() string {
	return fmt.Sprintf("default %s", c.Default.Expression())
}

func (c *SqlRename) Expression() string {
	return fmt.Sprintf("rename to %s", c.NewName.GetName())
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

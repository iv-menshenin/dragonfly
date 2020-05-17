package sql_ast

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	"go/token"
	"strconv"
	"strings"
)

var sqlReservedWords = []string{
	"all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "authorization",
	"binary", "both", "case", "cast", "check", "collate", "column", "concurrently", "constraint", "create",
	"cross", "current_catalog", "current_date", "current_role", "current_schema", "current_time",
	"current_timestamp", "current_user", "default", "deferrable", "desc", "distinct", "do", "else", "end",
	"except", "fetch", "for", "foreign", "freeze", "from", "full", "grant", "group", "having",
	"ilike", "in", "initially", "inner", "intersect", "into", "is", "isnull", "join", "leading", "left",
	"like", "limit", "localtime", "localtimestamp", "natural", "not", "notnull", "null", "offset", "on",
	"only", "or", "order", "outer", "over", "overlaps", "placing", "primary", "references", "returning",
	"right", "select", "session_user", "similar", "some", "symmetric", "table", "then", "to", "trailing",
	"union", "unique", "user", "using", "variadic", "verbose", "when", "where", "window", "with",
}

type (
	WithoutNameIdent struct{}
)

func (c *WithoutNameIdent) GetName() string {
	return ""
}

type (
	True  struct{}
	False struct{}
)

func (c *True) String() string {
	return "true"
}

func (c *True) expression() int { return 0 }

func (c *True) dependedOn() Dependencies {
	return nil
}

func (c *False) String() string {
	return "false"
}

func (c *False) expression() int { return 0 }

func (c *False) dependedOn() Dependencies {
	return nil
}

type (
	Literal struct {
		Text string
	}
)

func (c *Literal) GetName() string {
	return c.String()
}

func (c *Literal) String() string {
	if utils.ArrayContainsCI(sqlReservedWords, c.Text) {
		return "\"" + c.Text + "\""
	}
	return c.Text
}

func (c *Literal) expression() int { return 0 }

func (c *Literal) dependedOn() Dependencies {
	return nil
}

type (
	Selector struct {
		Name      string
		Container string
	}
)

func (c *Selector) GetName() string {
	return fmt.Sprintf("%s.%s", c.Container, c.Name)
}

func (c *Selector) String() string {
	return c.GetName()
}

func (c *Selector) expression() int { return 0 }

func (c *Selector) dependedOn() Dependencies {
	return dependedOn2(c.Container, c.Name)
}

type (
	AlterAttributeExpr struct {
		AttributeName string
		AlterExpr     SqlExpr
	}
)

func (c *AlterAttributeExpr) String() string {
	if dataType, ok := c.AlterExpr.(*DataTypeExpr); ok {
		return utils.NonEmptyStringsConcatSpaceSeparated("alter attribute", c.AttributeName, "type", dataType)
	} else {
		return utils.NonEmptyStringsConcatSpaceSeparated("alter attribute", c.AttributeName, c.AlterExpr)
	}
}

func (c *AlterAttributeExpr) expression() int { return 0 }

func (c *AlterAttributeExpr) dependedOn() Dependencies {
	return c.AlterExpr.dependedOn()
}

type (
	SetDropExpr struct {
		SetDrop SetDrop
		Expr    SqlExpr
	}
)

func (c *SetDropExpr) String() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.SetDrop, c.Expr)
}

func (c *SetDropExpr) expression() int { return 0 }

func (c *SetDropExpr) dependedOn() Dependencies {
	return nil
}

type (
	AddExpr struct {
		Target     SqlTarget
		Name       SqlIdent
		Definition SqlExpr
	}
	DropExpr struct {
		Target            SqlTarget
		Name              SqlIdent
		IfExists, Cascade bool
	}
	AlterExpr struct {
		Target SqlTarget
		Name   SqlIdent
		Alter  SqlExpr
	}
)

func (c *AddExpr) String() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("add", c.Target, c.Name, c.Definition)
}

func (c *AddExpr) expression() int { return 0 }

func (c *AddExpr) dependedOn() Dependencies {
	return c.Definition.dependedOn()
}

func (c *DropExpr) String() string {
	cascadeExpr, ifExistsExpr := "", ""
	if c.IfExists {
		ifExistsExpr = "if exists"
	}
	if c.Cascade {
		cascadeExpr = "cascade"
	}
	return utils.NonEmptyStringsConcatSpaceSeparated("drop", c.Target, ifExistsExpr, c.Name, cascadeExpr)
}

func (c *DropExpr) expression() int { return 0 }

func (c *DropExpr) dependedOn() Dependencies {
	return nil
}

func (c *AlterExpr) String() string {
	var subTarget = ""
	if _, ok := c.Alter.(*DataTypeExpr); ok {
		subTarget = "type"
	}
	if _, ok := c.Alter.(*Default); ok {
		subTarget = "default"
	}
	return utils.NonEmptyStringsConcatSpaceSeparated("alter", c.Target, c.Name, subTarget, c.Alter)
}

func (c *AlterExpr) expression() int { return 0 }

func (c *AlterExpr) dependedOn() Dependencies {
	return c.Alter.dependedOn()
}

type (
	BinaryExpr struct {
		Left  SqlExpr
		Right SqlExpr
		Op    token.Token
	}
	UnaryExpr struct {
		Ident SqlIdent
	}
)

func (c *BinaryExpr) String() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.Left, c.Op, c.Right)
}

func (c *BinaryExpr) expression() int { return 0 }

func (c *BinaryExpr) dependedOn() Dependencies {
	return concatDependencies(c.Left.dependedOn(), c.Right.dependedOn())
}

func (c *UnaryExpr) String() string {
	return c.Ident.GetName()
}

func (c *UnaryExpr) expression() int { return 0 }

func (c *UnaryExpr) dependedOn() Dependencies {
	return nil
}

type (
	SchemaExpr struct {
		SchemaName string
	}
)

func (c *SchemaExpr) String() string {
	return fmt.Sprintf("schema %s", c.SchemaName)
}

func (c *SchemaExpr) expression() int { return 0 }

func (c *SchemaExpr) dependedOn() Dependencies {
	return dependedOn2(c.SchemaName, "")
}

type (
	SetExpr struct {
		Set SqlExpr
	}
)

func (c *SetExpr) String() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("set", c.Set.String())
}

func (c *SetExpr) expression() int { return 0 }

func (c *SetExpr) dependedOn() Dependencies {
	return c.Set.dependedOn()
}

type (
	Default struct {
		Default SqlExpr
	}
)

func (c *Default) String() string {
	if c.Default == nil {
		return ""
	}
	return fmt.Sprintf("default %s", c.Default)
}

func (c *Default) expression() int { return 0 }

func (c *Default) dependedOn() Dependencies {
	return nil
}

type (
	SqlRename struct {
		Target  SqlTarget
		OldName SqlIdent
		NewName SqlIdent
	}
)

func (c *SqlRename) String() string {
	if c.Target == TargetNone {
		return utils.NonEmptyStringsConcatSpaceSeparated("rename", "to", c.NewName)
	}
	return utils.NonEmptyStringsConcatSpaceSeparated("rename", c.Target, c.OldName, "to", c.NewName)
}

func (c *SqlRename) expression() int { return 0 }

func (c *SqlRename) dependedOn() Dependencies {
	return nil
}

type (
	FncCall struct {
		Name SqlIdent
		Args []SqlExpr
	}
)

func (c *FncCall) String() string {
	var argmStr = make([]string, 0, len(c.Args))
	for _, arg := range c.Args {
		argmStr = append(argmStr, arg.String())
	}
	return fmt.Sprintf("%s(%s)", c.Name.GetName(), strings.Join(argmStr, ", "))
}

func (c *FncCall) expression() int { return 0 }

func (c *FncCall) dependedOn() Dependencies {
	var result Dependencies
	for _, a := range c.Args {
		result = concatDependencies(result, a.dependedOn())
	}
	return result
}

type (
	Integer struct {
		X int
	}
	String struct {
		X string
	}
)

func (c *Integer) String() string {
	return strconv.Itoa(c.X)
}

func (c *Integer) expression() int { return 0 }

func (c *Integer) dependedOn() Dependencies {
	return nil
}

func (c *String) String() string {
	return "'" + strings.Replace(c.X, "'", "''", -1) + "'"
}

func (c *String) expression() int { return 0 }

func (c *String) dependedOn() Dependencies {
	return nil
}

type (
	NotNullClause struct{}
)

func (c *NotNullClause) String() string {
	return "not null"
}

func (c *NotNullClause) expression() int { return 0 }

func (c *NotNullClause) dependedOn() Dependencies {
	return nil
}

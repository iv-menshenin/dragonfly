package sql_ast

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	"strings"
)

type (
	AlterStmt struct {
		Target SqlTarget
		Name   SqlIdent
		Alter  SqlExpr
	}
	CreateStmt struct {
		Target SqlTarget
		Name   SqlIdent
		Create SqlExpr
		IfNotX bool
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
)

func (c *AlterStmt) String() string {
	return fmt.Sprintf("alter %s %s %s", c.Target, c.Name.GetName(), c.Alter.String())
}

func (c *AlterStmt) statement() int { return 0 }

func (c *AlterStmt) dependedOn() Dependencies {
	return c.Alter.dependedOn()
}

func (c *AlterStmt) solved() Dependencies {
	var s, o, f string
	if name, ok := c.Name.(*Selector); ok {
		s, o = name.Container, name.Name
	} else if n := strings.Split(c.Name.GetName(), "."); len(n) > 1 {
		s, o = n[0], n[1]
	} else {
		if c.Target == TargetSchema {
			s = c.Name.GetName()
		} else {
			panic("cannot resolve schema for `" + c.Name.GetName() + "`")
		}
	}
	if add, ok := c.Alter.(*AddExpr); ok {
		f = add.Name.GetName()
	}
	return dependedOn3(s, o, f)
}

func (c *CreateStmt) String() string {
	ifNotExists := ""
	if c.IfNotX {
		ifNotExists = "if not exists"
	}
	return utils.NonEmptyStringsConcatSpaceSeparated("create", c.Target, ifNotExists, c.Name.GetName(), c.Create)
}

func (c *CreateStmt) statement() int { return 0 }

func (c *CreateStmt) dependedOn() Dependencies {
	if c.Create != nil {
		return c.Create.dependedOn()
	}
	return nil
}

func (c *CreateStmt) solved() (result Dependencies) {
	var s, o string
	if name, ok := c.Name.(*Selector); ok {
		s, o = name.Container, name.Name
	} else if n := strings.Split(c.Name.GetName(), "."); len(n) > 1 {
		s, o = n[0], n[1]
	} else {
		if c.Target == TargetSchema {
			s = c.Name.GetName()
		} else {
			panic("cannot resolve schema for `" + c.Name.GetName() + "`")
		}
	}
	result = dependedOn2(s, o)
	if c.Create != nil {
		if body, ok := c.Create.(*TableBodyDescriber); ok {
			for _, f := range body.Fields {
				result = concatDependencies(result, dependedOn3(s, o, f.Name.GetName()))
				f.Name.GetName()
			}
		}
	}
	return result
}

func (c *DropStmt) String() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("drop", c.Target, c.Name.GetName())
}

func (c *DropStmt) statement() int { return 0 }

func (c *DropStmt) dependedOn() Dependencies {
	return nil
}

func (c *DropStmt) solved() (result Dependencies) {
	return nil
}

func (c *UpdateStmt) String() string {
	var (
		clauseSet   = make([]string, 0, len(c.Set))
		clauseWhere = "1 = 1"
	)
	for _, set := range c.Set {
		clauseSet = append(clauseSet, set.String())
	}
	if c.Where != nil {
		clauseWhere = c.Where.String()
	}
	return fmt.Sprintf("update %s %s set %s where %s", c.Table.Table.GetName(), c.Table.Alias, strings.Join(clauseSet, ", "), clauseWhere)
}

func (c *UpdateStmt) statement() int { return 0 }

func (c *UpdateStmt) dependedOn() Dependencies {
	var result = make(Dependencies, 0)
	for _, s := range c.Set {
		result = concatDependencies(result, s.dependedOn())
	}
	return result
}

func (c *UpdateStmt) solved() (result Dependencies) {
	return nil
}

func (c *SelectStmt) String() string {
	var (
		clauseColumns = make([]string, 0, len(c.Columns))
		clauseWhere   = "1 = 1"
	)
	for _, col := range c.Columns {
		clauseColumns = append(clauseColumns, col.String())
	}
	if c.Where != nil {
		clauseWhere = c.Where.String()
	}
	return fmt.Sprintf("select %s from %s %s where %s", strings.Join(clauseColumns, ", "), c.From.Table.GetName(), c.From.Alias, clauseWhere)
}

func (c *SelectStmt) statement() int { return 0 }

func (c *SelectStmt) dependedOn() Dependencies {
	return nil
}

func (c *SelectStmt) solved() (result Dependencies) {
	return nil
}

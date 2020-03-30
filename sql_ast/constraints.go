package sql_ast

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	"strings"
)

type (
	ConstraintExpr interface {
		ConstraintInterface
		SqlExpr
	}
	ConstraintInterface interface {
		ConstraintString() string
		ConstraintParams() string
		dependencies() Dependencies
	}
	NamedConstraintExpr struct {
		Name       SqlIdent
		Constraint ConstraintInterface
	}
	UnnamedConstraintExpr struct {
		Constraint ConstraintInterface
	}
	ConstraintWithColumns struct {
		Columns    []string
		Constraint ConstraintExpr
	}
	ConstraintCommon struct {
		InColumn bool
	}
	// not null
	ConstraintNullableExpr struct {
		ConstraintCommon
		Nullable Nullable
	}
	// check
	ConstraintCheckExpr struct {
		ConstraintCommon
		Expression SqlExpr
		Where      SqlExpr
	}
	// default
	ConstraintDefaultExpr struct {
		ConstraintCommon
		Expression SqlExpr
	}
	// primary key
	ConstraintPrimaryKeyExpr struct {
		ConstraintCommon
	}
	// unique
	ConstraintUniqueExpr struct {
		ConstraintCommon
		Where SqlExpr
	}
	// foreign key
	ConstraintForeignKeyExpr struct {
		ConstraintCommon
		ToTable  SqlIdent
		ToColumn string
		OnDelete OnDeleteUpdateRule
		OnUpdate OnDeleteUpdateRule
	}
)

func (c *NamedConstraintExpr) ConstraintString() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("constraint", c.Name.GetName(), c.Constraint.ConstraintString())
}

func (c *NamedConstraintExpr) ConstraintParams() string {
	return c.Constraint.ConstraintParams()
}

func (c *NamedConstraintExpr) String() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.ConstraintString(), c.ConstraintParams())
}

func (c *NamedConstraintExpr) expression() int { return 0 }

func (c *NamedConstraintExpr) dependedOn() Dependencies {
	return c.dependencies()
}

func (c *NamedConstraintExpr) dependencies() Dependencies {
	return c.Constraint.dependencies()
}

func (c *UnnamedConstraintExpr) ConstraintString() string {
	return c.Constraint.ConstraintString()
}

func (c *UnnamedConstraintExpr) ConstraintParams() string {
	return c.Constraint.ConstraintParams()
}

func (c *UnnamedConstraintExpr) String() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.Constraint.ConstraintString(), c.Constraint.ConstraintParams())
}

func (c *UnnamedConstraintExpr) expression() int { return 0 }

func (c *UnnamedConstraintExpr) dependedOn() Dependencies {
	return c.dependencies()
}

func (c *UnnamedConstraintExpr) dependencies() Dependencies {
	return c.Constraint.dependencies()
}

func (c *ConstraintWithColumns) ConstraintString() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.Constraint.ConstraintString(), "(", strings.Join(c.Columns, ", "), ")")
}

func (c *ConstraintWithColumns) ConstraintParams() string {
	return c.Constraint.ConstraintParams()
}

func (c *ConstraintWithColumns) String() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.ConstraintString(), c.ConstraintParams())
}

func (c *ConstraintWithColumns) expression() int { return 0 }

func (c *ConstraintWithColumns) dependedOn() Dependencies {
	return c.dependencies()
}

func (c *ConstraintWithColumns) dependencies() Dependencies {
	return c.Constraint.dependencies()
}

func (c *ConstraintNullableExpr) ConstraintString() string {
	if c == nil {
		return ""
	}
	switch c.Nullable {
	case NullableNotNull:
		return "not null"
	case NullableNull:
		return "null"
	default:
		return ""
	}
}

func (c *ConstraintNullableExpr) ConstraintParams() string {
	return ""
}

func (c *ConstraintNullableExpr) dependencies() Dependencies {
	return nil
}

func (c *ConstraintCheckExpr) ConstraintString() string {
	return fmt.Sprintf("check (%s)", c.Expression.String())
}

func (c *ConstraintCheckExpr) ConstraintParams() string {
	if c.Where != nil {
		return "where " + c.Where.String()
	}
	return ""
}

func (c *ConstraintCheckExpr) dependencies() Dependencies {
	return nil
}

func (c *ConstraintDefaultExpr) ConstraintString() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("default", c.Expression.String())
}

func (c *ConstraintDefaultExpr) ConstraintParams() string {
	return ""
}

func (c *ConstraintDefaultExpr) dependencies() Dependencies {
	return nil
}

func (c *ConstraintPrimaryKeyExpr) ConstraintString() string {
	return "primary key"
}

func (c *ConstraintPrimaryKeyExpr) ConstraintParams() string {
	return ""
}

func (c *ConstraintPrimaryKeyExpr) dependencies() Dependencies {
	return nil
}

func (c *ConstraintUniqueExpr) ConstraintString() string {
	return "unique"
}

func (c *ConstraintUniqueExpr) ConstraintParams() string {
	if c.Where != nil {
		return "where " + c.Where.String()
	}
	return ""
}

func (c *ConstraintUniqueExpr) dependencies() Dependencies {
	return nil
}

func (c *ConstraintForeignKeyExpr) ConstraintString() string {
	if c.InColumn {
		return ""
	}
	return "foreign key"
}

func (c *ConstraintForeignKeyExpr) ConstraintParams() string {
	updateRules := ""
	if int(c.OnUpdate) > -1 {
		updateRules += fmt.Sprintf(" on update %s", c.OnUpdate)
	}
	if int(c.OnDelete) > -1 {
		updateRules += fmt.Sprintf(" on delete %s", c.OnDelete)
	}
	return fmt.Sprintf("references %s (%s)%s", c.ToTable.GetName(), c.ToColumn, updateRules)
}

func (c *ConstraintForeignKeyExpr) dependencies() Dependencies {
	if tableName := strings.Split(c.ToTable.GetName(), "."); len(tableName) > 1 {
		return dependedOn3(tableName[0], tableName[1], c.ToColumn)
	}
	panic("unknown schema for table `" + c.ToTable.GetName() + "`")
}

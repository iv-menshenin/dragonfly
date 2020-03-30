package sql_ast

import (
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	"strings"
)

type (
	BracketBlock struct {
		Expr      []SqlExpr
		Statement SqlStmt
	}
)

func (c *BracketBlock) String() string {
	if len(c.Expr) > 0 {
		if c.Statement != nil {
			panic("BracketBlock allows just Expr or Statement not both")
		}
		var exprs = make([]string, 0, len(c.Expr))
		for _, expr := range c.Expr {
			exprs = append(exprs, expr.String())
		}
		return utils.NonEmptyStringsConcatSpaceSeparated("(\n\t", strings.Join(exprs, ",\n\t"), "\n)")
	} else {
		if c.Statement == nil {
			panic("BracketBlock require Expr or Statement")
		}
		return fmt.Sprintf("(%s)", c.Statement)
	}
}

func (c *BracketBlock) expression() int { return 0 }

func (c *BracketBlock) dependedOn() Dependencies {
	if c.Statement != nil {
		return c.Statement.dependedOn()
	}
	var result Dependencies
	for _, expr := range c.Expr {
		result = concatDependencies(result, expr.dependedOn())
	}
	return result
}

type (
	TableBodyDescriber struct {
		Fields      []*SqlField
		Constraints []ConstraintExpr
	}
)

func (c *TableBodyDescriber) String() string {
	var columns = make([]string, 0, len(c.Fields)+len(c.Constraints))
	for _, fld := range c.Fields {
		columns = append(columns, fmt.Sprintf("\n\t%s", fld))
	}
	for _, cts := range c.Constraints {
		columns = append(columns, fmt.Sprintf("\n\t%s", cts.String()))
	}
	return "(" + strings.Join(columns, ",") + "\n)"
}

func (c *TableBodyDescriber) expression() int { return 0 }

func (c *TableBodyDescriber) dependedOn() Dependencies {
	var result = make(Dependencies, 0, len(c.Constraints))
	for _, constraint := range c.Constraints {
		result = concatDependencies(result, constraint.dependedOn())
	}
	for _, field := range c.Fields {
		result = concatDependencies(result, field.dependedOn())
	}
	return result
}

type (
	SqlField struct {
		Name        SqlIdent
		Describer   *DataTypeExpr
		Constraints []ConstraintExpr
	}
)

func (c *SqlField) String() string {
	var constraintsClause = make([]string, 0, len(c.Constraints))
	if len(c.Constraints) > 0 {
		for _, constraint := range c.Constraints {
			constraintsClause = append(constraintsClause, constraint.String())
		}
	}
	return utils.NonEmptyStringsConcatSpaceSeparated(c.Name.GetName(), c.Describer, strings.Join(constraintsClause, " "))
}

func (c *SqlField) expression() int { return 0 }

func (c *SqlField) dependedOn() Dependencies {
	var result = make(Dependencies, 0, len(c.Constraints))
	for _, constraint := range c.Constraints {
		result = concatDependencies(result, constraint.dependedOn())
	}
	return concatDependencies(result, c.Describer.dependedOn())
}

type (
	DataTypeExpr struct {
		DataType  string
		IsArray   bool
		Length    *int
		Precision *int
		Collation *string
	} // NotNull and Default - this is not about data type, this is about Constraints
)

func (c *DataTypeExpr) String() string {
	var dataType = c.DataType
	if c.Length != nil {
		if c.Precision != nil {
			dataType += fmt.Sprintf("(%d, %d)", *c.Length, *c.Precision)
		} else {
			dataType += fmt.Sprintf("(%d)", *c.Length)
		}
	}
	if c.IsArray {
		dataType += "[]"
	}
	if c.Collation == nil {
		return dataType
	} else {
		return dataType + " collate " + *c.Collation
	}
}

func (c *DataTypeExpr) expression() int { return 0 }

func (c *DataTypeExpr) dependedOn() Dependencies {
	if sepType := strings.Split(c.DataType, "."); len(sepType) > 1 {
		return dependedOn2(sepType[0], sepType[1])
	}
	return nil
}

type (
	RecordDescription struct {
		Fields []SqlExpr
	}
	EnumDescription struct {
		Values []*String
	}
)

func (c *RecordDescription) String() string {
	var s = make([]string, len(c.Fields))
	for i, f := range c.Fields {
		s[i] = f.String()
	}
	return fmt.Sprintf(" as (%s)", strings.Join(s, ", "))
}

func (c *RecordDescription) expression() int { return 0 }

func (c *RecordDescription) dependedOn() Dependencies {
	var result Dependencies
	for _, f := range c.Fields {
		result = concatDependencies(result, f.dependedOn())
	}
	return result
}

func (c *EnumDescription) String() string {
	var values = make([]string, 0, len(c.Values))
	for _, v := range c.Values {
		values = append(values, fmt.Sprintf("%s", v))
	}
	return fmt.Sprintf(" as enum (%s)", strings.Join(values, ","))
}

func (c *EnumDescription) expression() int { return 0 }

func (c *EnumDescription) dependedOn() Dependencies {
	return nil
}

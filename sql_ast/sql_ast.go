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
	SqlTarget          int
	Nullable           bool
	SetDrop            bool
	OnDeleteUpdateRule int

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
	Integer struct {
		X int
	}
	FncCall struct {
		Name SqlIdent
		Args []SqlExpr
	}

	NotNullClause struct{}

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
	AlterAttributeExpr struct {
		AttributeName string
		AlterExpr     SqlExpr
	}
	AlterDataTypeExpr struct {
		DataType  string
		Length    *int
		Precision *int
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
	DataTypeExpr struct {
		DataType  string
		IsArray   bool
		Length    *int
		Precision *int
		Collation *string
	} // NotNull and Default - this is not about data type, this is about Constraints

	ConstraintExpr interface {
		ConstraintInterface
		SqlExpr
	}
	ConstraintInterface interface {
		ConstraintString() string
		ConstraintParams() string
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

	ColumnDefinitionExpr struct {
		Name        SqlIdent
		DataType    string
		Collation   *string
		Constraints []ConstraintExpr
	}
	BracketBlock struct {
		Expr      []SqlExpr
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

	TableDesc struct {
		Table SqlIdent
		Alias string
	}

	RecordDescription struct {
		Fields []SqlExpr
	}
	EnumDescription struct {
		Values []string
	}

	TypeDescription struct {
		Type              string
		Length, Precision *int
		Null              Nullable
		Default, Check    *string
	}
)

/* <FIELDS AND TYPES> =============================================================================================== */

type (
	TableBodyDescriber struct {
		Fields      []FieldDescriber
		Constraints []ConstraintExpr
	}
)

func (c *TableBodyDescriber) Expression() string {
	var columns = make([]string, 0, len(c.Fields)+len(c.Constraints))
	for _, fld := range c.Fields {
		columns = append(columns, fmt.Sprintf("\n\t%s", fld))
	}
	for _, cts := range c.Constraints {
		columns = append(columns, fmt.Sprintf("\n\t%s", cts.Expression()))
	}
	return "(" + strings.Join(columns, ",") + "\n)"
}

type (
	FieldDescriber interface {
		fmt.Stringer
		fieldDescriber() string
	}
	TypeDescriber interface {
		fmt.Stringer
		typeDescriber() string
	}
	SqlField struct {
		Name        SqlIdent
		Describer   TypeDescriber
		Constraints []ConstraintExpr
	} // FieldDescriber
	FullTypeDesc struct {
		ShortTypeDesc
		Nullable *SqlNullable
		Default  SqlExpr
	} // TypeDescriber
	ShortTypeDesc struct {
		TypeName  SqlExpr
		Collation *string
	} // TypeDescriber
)

func (c *SqlField) fieldDescriber() string {
	return c.String()
}
func (c *SqlField) String() string {
	var constraintsClause = make([]string, 0, len(c.Constraints))
	if len(c.Constraints) > 0 {
		for _, constraint := range c.Constraints {
			constraintsClause = append(constraintsClause, constraint.Expression())
		}
	}
	return utils.NonEmptyStringsConcatSpaceSeparated(c.Name.GetName(), c.Describer, strings.Join(constraintsClause, " "))
}

func (c *ShortTypeDesc) typeDescriber() string {
	return c.String()
}
func (c *ShortTypeDesc) String() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.TypeName.Expression(), c.Collation)
}

func (c *FullTypeDesc) typeDescriber() string {
	return c.String()
}
func (c *FullTypeDesc) String() string {
	var (
		nullableClause string
		defaultClause  string
	)
	if c.Nullable != nil {
		nullableClause = c.Nullable.Expression()
	}
	if c.Default != nil {
		defaultClause = c.Default.Expression()
	}
	return utils.NonEmptyStringsConcatSpaceSeparated(c.ShortTypeDesc.typeDescriber(), nullableClause, defaultClause)
}

/* </FIELDS AND TYPES> ============================================================================================== */

const (
	TargetNone SqlTarget = iota
	TargetSchema
	TargetTable
	TargetColumn
	TargetDomain
	TargetType

	RuleNoAction OnDeleteUpdateRule = iota
	RuleCascade
	RuleRestrict
	RuleSetNull
	RuleSetDefault

	NullableNull    Nullable = true
	NullableNotNull Nullable = false

	SetDropDrop SetDrop = false
	SetDropSet  SetDrop = true
)

func (c OnDeleteUpdateRule) String() string {
	switch c {
	case RuleCascade:
		return "cascade"
	case RuleRestrict:
		return "restrict"
	case RuleSetNull:
		return "set null"
	case RuleSetDefault:
		return "set default"
	default:
		return "no action"
	}
}

var (
	targetDescriptor = map[SqlTarget]string{
		TargetSchema: "schema",
		TargetTable:  "table",
		TargetColumn: "column",
		TargetDomain: "domain",
		TargetType:   "type",
	}
)

func (c SqlTarget) String() string {
	if s, ok := targetDescriptor[c]; ok {
		return s
	}
	panic("unknown target %v")
}

func (c *Literal) GetName() string {
	return c.Expression()
}

func (c *Selector) GetName() string {
	return fmt.Sprintf("%s.%s", c.Container, c.Name)
}

func (c *Selector) Expression() string {
	return c.GetName()
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
	createStmt := "create %s %s"
	if c.IfNotX {
		createStmt = "create %s if not exists %s"
	}
	if c.Create != nil {
		return utils.NonEmptyStringsConcatSpaceSeparated(fmt.Sprintf(createStmt, c.Target, c.Name.GetName()), c.Create.Expression())
	} else {
		return fmt.Sprintf(createStmt, c.Target, c.Name.GetName())
	}
}

func (c *DropStmt) GetComment() string {
	return fmt.Sprintf("deleting %s %s", c.Target, c.Name.GetName())
}

func (c *DropStmt) MakeStmt() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("drop", c.Target, c.Name.GetName())
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

func (c *SqlNullable) Expression() string {
	switch c.Nullable {
	case NullableNotNull:
		return "not null"
	case NullableNull:
		return "null"
	default:
		return ""
	}
}

func (c *SetDropExpr) Expression() string {
	if sqlExpr := c.Expr.Expression(); sqlExpr != "" {
		if c.SetDrop == SetDropSet {
			return utils.NonEmptyStringsConcatSpaceSeparated("set", c.Expr.Expression())
		} else {
			return utils.NonEmptyStringsConcatSpaceSeparated("drop", c.Expr.Expression())
		}
	}
	return ""
}

func (c *AlterAttributeExpr) Expression() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("alter attribute", c.AttributeName, c.AlterExpr.Expression())
}

func (c *AlterDataTypeExpr) Expression() string {
	// TODO use datatypefillers
	if c.Length == nil && c.Precision == nil {
		return utils.NonEmptyStringsConcatSpaceSeparated("type", c.DataType)
	}
	if c.Length != nil {
		if c.Precision == nil {
			return fmt.Sprintf("type %s(%d)", c.DataType, *c.Length)
		} else {
			return fmt.Sprintf("type %s(%d, %d)", c.DataType, *c.Length, *c.Precision)
		}
	} else {
		// Length == nil && Precision != nil
		return fmt.Sprintf("type %s(%d)", c.DataType, *c.Precision)
	}
}

func (c *DropExpr) Expression() string {
	cascadeExpr, ifExistsExpr := "", ""
	if c.IfExists {
		ifExistsExpr = "if exists"
	}
	if c.Cascade {
		cascadeExpr = "cascade"
	}
	return utils.NonEmptyStringsConcatSpaceSeparated("drop", c.Target, ifExistsExpr, c.Name.GetName(), cascadeExpr)
}

func (c *AddExpr) Expression() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("add", c.Target, c.Name.GetName(), c.Definition.Expression())
}

func (c *BinaryExpr) Expression() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.Left.Expression(), c.Op, c.Right.Expression())
}

func (c *UnaryExpr) Expression() string {
	return c.Ident.GetName()
}

func (c *DataTypeExpr) Expression() string {
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

/* CONSTRAINTS */

func (c *NamedConstraintExpr) ConstraintString() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("constraint", c.Name.GetName(), c.Constraint.ConstraintString())
}

func (c *NamedConstraintExpr) ConstraintParams() string {
	return c.Constraint.ConstraintParams()
}

func (c *NamedConstraintExpr) Expression() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.ConstraintString(), c.ConstraintParams())
}

func (c *UnnamedConstraintExpr) ConstraintString() string {
	return c.Constraint.ConstraintString()
}

func (c *UnnamedConstraintExpr) ConstraintParams() string {
	return c.Constraint.ConstraintParams()
}

func (c *UnnamedConstraintExpr) Expression() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.Constraint.ConstraintString(), c.Constraint.ConstraintParams())
}

func (c *ConstraintWithColumns) ConstraintString() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.Constraint.ConstraintString(), "(", strings.Join(c.Columns, ", "), ")")
}

func (c *ConstraintWithColumns) ConstraintParams() string {
	return c.Constraint.ConstraintParams()
}

func (c *ConstraintWithColumns) Expression() string {
	return utils.NonEmptyStringsConcatSpaceSeparated(c.ConstraintString(), c.ConstraintParams())
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

func (c *ConstraintCheckExpr) ConstraintString() string {
	return fmt.Sprintf("check (%s)", c.Expression.Expression())
}

func (c *ConstraintCheckExpr) ConstraintParams() string {
	if c.Where != nil {
		return "where " + c.Where.Expression()
	}
	return ""
}

func (c *ConstraintDefaultExpr) ConstraintString() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("default", c.Expression.Expression())
}

func (c *ConstraintDefaultExpr) ConstraintParams() string {
	return ""
}

func (c *ConstraintPrimaryKeyExpr) ConstraintString() string {
	return "primary key"
}

func (c *ConstraintPrimaryKeyExpr) ConstraintParams() string {
	return ""
}

func (c *ConstraintUniqueExpr) ConstraintString() string {
	return "unique"
}

func (c *ConstraintUniqueExpr) ConstraintParams() string {
	if c.Where != nil {
		return "where " + c.Where.Expression()
	}
	return ""
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

/* */

func (c *ColumnDefinitionExpr) Expression() string {
	constraints := make([]interface{}, 0, len(c.Constraints))
	for _, constraint := range c.Constraints {
		constraints = append(constraints, utils.NonEmptyStringsConcatSpaceSeparated(constraint.ConstraintString(), constraint.ConstraintParams()))
	}
	return utils.NonEmptyStringsConcatSpaceSeparated(c.Name.GetName(), c.DataType, c.Collation, utils.NonEmptyStringsConcatSpaceSeparated(constraints...))
}

func (c *BracketBlock) Expression() string {
	if len(c.Expr) > 0 {
		if c.Statement != nil {
			panic("BracketBlock allows just Expr or Statement not both")
		}
		var exprs = make([]string, 0, len(c.Expr))
		for _, expr := range c.Expr {
			exprs = append(exprs, expr.Expression())
		}
		return utils.NonEmptyStringsConcatSpaceSeparated("(\n\t", strings.Join(exprs, ",\n\t"), "\n)")
	} else {
		if c.Statement == nil {
			panic("BracketBlock require Expr or Statement")
		}
		return utils.NonEmptyStringsConcatSpaceSeparated("\n/*", c.Statement.GetComment(), "*/\n(", c.Statement.MakeStmt(), ")")
	}
}

func (c *AlterExpr) Expression() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("alter", c.Target, c.Name.GetName(), c.Alter.Expression())
}

func (c *SchemaExpr) Expression() string {
	return fmt.Sprintf("schema %s", c.SchemaName)
}

func (c *SetMetadataExpr) Expression() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("set", c.Set.Expression())
}

func (c *Default) Expression() string {
	return utils.NonEmptyStringsConcatSpaceSeparated("default", c.Default.Expression())
}

func (c *SqlRename) Expression() string {
	if c.Target == TargetNone {
		return utils.NonEmptyStringsConcatSpaceSeparated("rename", "to", c.NewName.GetName())
	}
	return utils.NonEmptyStringsConcatSpaceSeparated("rename", c.Target, c.OldName.GetName(), "to", c.NewName.GetName())
}

func (c *Literal) Expression() string {
	if utils.ArrayContainsCI(sqlReservedWords, c.Text) {
		return "\"" + c.Text + "\""
	}
	return c.Text
}

func (c *FncCall) Expression() string {
	var argmStr = make([]string, 0, len(c.Args))
	for _, arg := range c.Args {
		argmStr = append(argmStr, arg.Expression())
	}
	return fmt.Sprintf("%s(%s)", c.Name.GetName(), strings.Join(argmStr, ", "))
}

func (c *Integer) Expression() string {
	return strconv.Itoa(c.X)
}

func (c *NotNullClause) Expression() string {
	return "not null"
}

func (c *RecordDescription) Expression() string {
	var s = make([]string, len(c.Fields))
	for i, f := range c.Fields {
		s[i] = f.Expression()
	}
	return fmt.Sprintf(" as (%s)", strings.Join(s, ", "))
}

func (c *EnumDescription) Expression() string {
	// TODO caution. may sql inject by '
	return fmt.Sprintf(" as enum ('%s')", strings.Join(c.Values, "', '"))
}

func (c *TypeDescription) Expression() string {
	colType := c.Type
	if c.Length != nil {
		colType += "(" + utils.NonEmptyStringsConcatCommaSeparated(c.Length, c.Precision) + ")"
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

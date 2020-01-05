package dragonfly

import (
	"fmt"
	"go/token"
	"strconv"
	"strings"
)

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
		Name        string
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

func stringToOnDeleteUpdateRule(s *string) OnDeleteUpdateRule {
	if s == nil {
		return RuleNoAction
	}
	switch strings.ToLower(*s) {
	case "cascade":
		return RuleCascade
	case "restrict":
		return RuleRestrict
	case "set null":
		return RuleSetNull
	case "default":
		return RuleSetDefault
	default:
		panic(fmt.Sprintf("cannot resolve update (delete) rule `%s`", *s))
	}
}

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

func makeTableDrop(schema, tableName string) SqlStmt {
	return &DropStmt{
		Target: TargetTable,
		Name: &Selector{
			Name:      tableName,
			Container: schema,
		},
	}
}

func makeConstraintInterface(inColumn bool, constraintDef Constraint) ConstraintInterface {
	var newConstraint ConstraintInterface
	switch constraintDef.Type {
	case ConstraintPrimaryKey:
		newConstraint = &ConstraintPrimaryKeyExpr{
			ConstraintCommon: ConstraintCommon{InColumn: inColumn},
		}
	case ConstraintCheck:
		if params, ok := constraintDef.Parameters.Parameter.(Check); ok {
			newConstraint = &ConstraintCheckExpr{
				ConstraintCommon: ConstraintCommon{InColumn: inColumn},
				Expression:       &Literal{Text: params.Expression},
				Where:            nil,
			}
		} else {
			panic("the check constraint should contains the check expression")
		}
	case ConstraintUniqueKey:
		if params, ok := constraintDef.Parameters.Parameter.(Where); ok {
			newConstraint = &ConstraintUniqueExpr{
				ConstraintCommon: ConstraintCommon{InColumn: inColumn},
				Where:            &Literal{Text: params.Where},
			}
		} else {
			newConstraint = &ConstraintUniqueExpr{
				ConstraintCommon: ConstraintCommon{InColumn: inColumn},
				Where:            nil,
			}
		}
	case ConstraintForeignKey:
		if params, ok := constraintDef.Parameters.Parameter.(ForeignKey); ok {
			newConstraint = &ConstraintForeignKeyExpr{
				ConstraintCommon: ConstraintCommon{InColumn: inColumn},
				ToTable:          &Literal{Text: params.ToTable},
				ToColumn:         params.ToColumn,
				OnDelete:         stringToOnDeleteUpdateRule(params.OnDelete),
				OnUpdate:         stringToOnDeleteUpdateRule(params.OnUpdate),
			}
		} else {
			panic("the foreign key constraint should contains the parameters")
		}
	}
	return newConstraint
}

func makeConstraintsExpr(inColumn bool, constraintSet []Constraint) []ConstraintExpr {
	var constraints = make([]ConstraintExpr, 0, len(constraintSet))
	for _, constraintDef := range constraintSet {
		if constraintDef.Name != "" {
			constraints = append(constraints, &NamedConstraintExpr{
				Name:       &Literal{constraintDef.Name},
				Constraint: makeConstraintInterface(inColumn, constraintDef),
			})
		} else {
			constraints = append(constraints, &UnnamedConstraintExpr{
				Constraint: makeConstraintInterface(inColumn, constraintDef),
			})
		}
	}
	return constraints
}

func makeTableCreate(schemaName, tableName string, tableStruct TableClass) SqlStmt {
	/*
		https://postgrespro.ru/docs/postgresql/9.6/sql-createtable
	*/
	var columnsAndConstraintsDefinition = make([]SqlExpr, 0, len(tableStruct.Columns)+len(tableStruct.Constraints))
	for _, column := range tableStruct.Columns {
		columnType := ""
		customSchema, customType, isCustom := column.Value.Schema.makeCustomType()
		if isCustom {
			columnType = fmt.Sprintf("%s.%s", customSchema, customType)
		} else {
			columnType = column.Value.Schema.Value.Type
			if column.Value.Schema.Value.Length != nil {
				if column.Value.Schema.Value.Precision != nil {
					columnType += "(" + strconv.Itoa(*column.Value.Schema.Value.Length) + "," + strconv.Itoa(*column.Value.Schema.Value.Precision) + ")"
				} else {
					columnType += "(" + strconv.Itoa(*column.Value.Schema.Value.Length) + ")"
				}
			}
		}
		var constraints = make([]ConstraintExpr, 0, len(column.Value.Constraints)+1)
		if !isCustom && column.Value.Schema.Value.NotNull {
			constraints = append(
				constraints,
				&UnnamedConstraintExpr{
					Constraint: &ConstraintNullableExpr{Nullable: Nullable(!column.Value.Schema.Value.NotNull)},
				},
			)
		}
		columnsAndConstraintsDefinition = append(columnsAndConstraintsDefinition, &ColumnDefinitionExpr{
			Name:        column.Value.Name,
			DataType:    columnType,
			Collation:   nil,
			Constraints: append(constraints, makeConstraintsExpr(true, column.Value.Constraints)...),
		})
	}
	for _, constraint := range tableStruct.Constraints {
		constraintInterface := makeConstraintInterface(false, constraint.Constraint)
		var constraintExpr ConstraintExpr
		if constraint.Constraint.Name != "" {
			constraintExpr = &NamedConstraintExpr{
				Name:       &Literal{constraint.Constraint.Name},
				Constraint: constraintInterface,
			}
		} else {
			constraintExpr = &UnnamedConstraintExpr{
				Constraint: constraintInterface,
			}
		}
		columnsAndConstraintsDefinition = append(columnsAndConstraintsDefinition, &ConstraintWithColumns{
			Columns:    constraint.Columns,
			Constraint: constraintExpr,
		})
	}
	return &CreateStmt{
		Target: TargetTable,
		Name: &Selector{
			Name:      tableName,
			Container: schemaName,
		},
		Create: &BracketBlock{
			Expr: columnsAndConstraintsDefinition,
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

/* CONSTRAINTS */

func (c *NamedConstraintExpr) ConstraintString() string {
	return fmt.Sprintf("%s %s", c.Name.GetName(), c.Constraint.ConstraintString())
}

func (c *NamedConstraintExpr) ConstraintParams() string {
	return c.Constraint.ConstraintParams()
}

func (c *NamedConstraintExpr) Expression() string {
	return fmt.Sprintf("%s %s", c.ConstraintString(), c.ConstraintParams())
}

func (c *UnnamedConstraintExpr) ConstraintString() string {
	return c.Constraint.ConstraintString()
}

func (c *UnnamedConstraintExpr) ConstraintParams() string {
	return c.Constraint.ConstraintParams()
}

func (c *UnnamedConstraintExpr) Expression() string {
	return fmt.Sprintf("%s %s", c.Constraint.ConstraintString(), c.Constraint.ConstraintParams())
}

func (c *ConstraintWithColumns) ConstraintString() string {
	return fmt.Sprintf("%s (%s)", c.Constraint.ConstraintString(), strings.Join(c.Columns, ", "))
}

func (c *ConstraintWithColumns) ConstraintParams() string {
	return c.Constraint.ConstraintParams()
}

func (c *ConstraintWithColumns) Expression() string {
	return fmt.Sprintf("%s %s", c.ConstraintString(), c.ConstraintParams())
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
	return fmt.Sprintf("default %s", c.Expression.Expression())
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
	collation := ""
	constraints := ""
	if c.Collation != nil {
		collation += " " + *c.Collation
	}
	for _, constraint := range c.Constraints {
		constraints += fmt.Sprintf(" %s %s", constraint.ConstraintString(), constraint.ConstraintParams())
	}
	return fmt.Sprintf("\n\t%s %s%s%s", c.Name, c.DataType, collation, constraints)
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
		return fmt.Sprintf("(\n/* begin */%s\n/* end */\n)", strings.Join(exprs, ", "))
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

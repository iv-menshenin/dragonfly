package dragonfly

import (
	"fmt"
	"go/token"
	"strconv"
	"strings"
)

var sqlReservedWords = []string{"all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "authorization", "binary", "both", "case", "cast", "check", "collate", "column", "concurrently", "constraint", "create", "cross", "current_catalog", "current_date", "current_role", "current_schema", "current_time", "current_timestamp", "current_user", "default", "deferrable", "desc", "distinct", "do", "else", "end", "except", "false", "fetch", "for", "foreign", "freeze", "from", "full", "grant", "group", "having", "ilike", "in", "initially", "inner", "intersect", "into", "is", "isnull", "join", "leading", "left", "like", "limit", "localtime", "localtimestamp", "natural", "not", "notnull", "null", "offset", "on", "only", "or", "order", "outer", "over", "overlaps", "placing", "primary", "references", "returning", "right", "select", "session_user", "similar", "some", "symmetric", "table", "then", "to", "trailing", "true", "union", "unique", "user", "using", "variadic", "verbose", "when", "where", "window", "with"}

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
	if len(c.Constraints) > 0 {
		var constraintsClause = make([]string, 0, len(c.Constraints))
		for _, constraint := range c.Constraints {
			constraintsClause = append(constraintsClause, constraint.Expression())
		}
		return fmt.Sprintf("%s %s %s", c.Name.GetName(), c.Describer, strings.Join(constraintsClause, " "))
	}
	return fmt.Sprintf("%s %s", c.Name.GetName(), c.Describer)
}

func (c *ShortTypeDesc) typeDescriber() string {
	return c.String()
}
func (c *ShortTypeDesc) String() string {
	if c.Collation != nil {
		return c.TypeName.Expression() + " " + *c.Collation
	}
	return c.TypeName.Expression()
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
	return fmt.Sprintf("%s %s %s", c.ShortTypeDesc.typeDescriber(), nullableClause, defaultClause)
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
		TargetType:   "type",
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
	if c.Create != nil {
		return fmt.Sprintf("create %s %s %s", c.Target, c.Name.GetName(), c.Create.Expression())
	} else {
		return fmt.Sprintf("create %s %s", c.Target, c.Name.GetName())
	}
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

func makeTypeSetSchema(domain string, rename NameComparator) SqlStmt {
	return &AlterStmt{
		Target: TargetType,
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

func makeTypeRename(schema string, rename NameComparator) SqlStmt {
	return &AlterStmt{
		Target: TargetType,
		Name: &Selector{
			Name:      rename.Actual,
			Container: schema,
		},
		Alter: &SqlRename{
			NewName: &Literal{Text: rename.New},
		},
	}
}

func makeTypeAlterAttributeDataType(schemaName, typeName, attrName string, typeSchema TypeBase) SqlStmt {
	return &AlterStmt{
		Target: TargetType,
		Name: &Selector{
			Name:      typeName,
			Container: schemaName,
		},
		Alter: makeAlterAttributeDataType(attrName, typeSchema),
	}

}

func makeAlterAttributeDataType(attrName string, typeSchema TypeBase) SqlExpr {
	return &AlterAttributeExpr{
		AttributeName: attrName,
		AlterExpr: &AlterDataTypeExpr{
			DataType:  typeSchema.Type,
			Length:    typeSchema.Length,
			Precision: typeSchema.Precision,
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
		Alter: makeSetDropExpr(notNull, &NotNullClause{}),
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

func makeType(schema, typeName string, typeSchema TypeSchema) SqlStmt {
	var create SqlExpr
	if strings.EqualFold(typeSchema.Type, "record") {
		fields := make([]SqlExpr, len(typeSchema.Fields))
		for i, f := range typeSchema.Fields {
			fieldTypeName := f.Value.Schema.Value.Type
			if typeSchema, typeName, ok := f.Value.Schema.makeCustomType(); ok {
				fieldTypeName = fmt.Sprintf("%s.%s", typeSchema, typeName)
			}
			fields[i] = &ColumnDefinitionExpr{
				Name:        &Literal{Text: f.Value.Name},
				DataType:    fieldTypeName,
				Collation:   nil, // TODO COLLATION
				Constraints: nil,
			}
		}
		create = &RecordDescription{
			Fields: fields,
		}
	} else if strings.EqualFold(typeSchema.Type, "enum") {
		values := make([]string, len(typeSchema.Enum))
		for i, f := range typeSchema.Enum {
			values[i] = f.Value
		}
		create = &EnumDescription{
			Values: values,
		}
	} else if strings.EqualFold(typeSchema.Type, "map") {
		values := make([]string, len(typeSchema.Enum))
		for i, f := range typeSchema.Enum {
			values[i] = f.Value
		}
		create = &RecordDescription{
			Fields: []SqlExpr{
				&ColumnDefinitionExpr{
					Name:     &Literal{Text: "data"},
					DataType: "json",
				},
			},
		}
	} else {
		panic(fmt.Sprintf("unknown type `%s`", typeSchema.Type))
	}
	return &CreateStmt{
		Target: TargetType,
		Name: &Selector{
			Name:      typeName,
			Container: schema,
		},
		Create: create,
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

func makeTypeDrop(schema, typeName string) SqlStmt {
	return &DropStmt{
		Target: TargetType,
		Name: &Selector{
			Name:      typeName,
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

func makeAlterColumnSetNotNull(schema, table, column string, notNull bool) SqlStmt {
	return &AlterStmt{
		Target: TargetTable,
		Name: &Selector{
			Name:      table,
			Container: schema,
		},
		Alter: &AlterExpr{
			Target: TargetColumn,
			Name:   &Literal{Text: column},
			Alter:  makeSetDropExpr(notNull, &NotNullClause{}),
		},
	}
}

func makeAlterColumnSetType(schema, table, column string, domainSchema DomainSchema) SqlStmt {
	return &AlterStmt{
		Target: TargetTable,
		Name: &Selector{
			Name:      table,
			Container: schema,
		},
		Alter: &AlterExpr{
			Target: TargetColumn,
			Name:   &Literal{Text: column},
			Alter: &TypeDescription{
				Type:      domainSchema.Type,
				Length:    domainSchema.Length,
				Precision: domainSchema.Precision,
			},
		},
	}
}

func makeAlterColumnSetDomain(schema, table, column, domainName string) SqlStmt {
	return &AlterStmt{
		Target: TargetTable,
		Name: &Selector{
			Name:      table,
			Container: schema,
		},
		Alter: &AlterExpr{
			Target: TargetColumn,
			Name:   &Literal{Text: column},
			Alter: &TypeDescription{
				Type: domainName,
			},
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

func makeTableCreate(schemaName, tableName string, tableStruct Table) SqlStmt {
	/*
		https://postgrespro.ru/docs/postgresql/9.6/sql-createtable
	*/
	var (
		fields      = make([]FieldDescriber, 0, len(tableStruct.Columns))
		constraints = make([]ConstraintExpr, 0, len(tableStruct.Constraints))
	)
	for _, column := range tableStruct.Columns {
		var (
			columnType SqlExpr
		)
		customSchema, customType, isCustom := column.Value.Schema.makeCustomType()
		if isCustom {
			columnType = &Selector{Name: customType, Container: customSchema}
		} else {
			if column.Value.Schema.Value.Length != nil {
				if column.Value.Schema.Value.Precision != nil {
					columnType = &FncCall{
						Name: &Literal{Text: column.Value.Schema.Value.Type},
						Args: []SqlExpr{
							&Integer{X: *column.Value.Schema.Value.Length},
							&Integer{X: *column.Value.Schema.Value.Precision},
						},
					}
				} else {
					columnType = &FncCall{
						Name: &Literal{Text: column.Value.Schema.Value.Type},
						Args: []SqlExpr{
							&Integer{X: *column.Value.Schema.Value.Length},
						},
					}
				}
			} else {
				columnType = &Literal{Text: column.Value.Schema.Value.Type}
			}
		}
		var columnConstraints = make([]ConstraintExpr, 0, len(column.Value.Constraints)+2)
		if !isCustom && column.Value.Schema.Value.NotNull {
			columnConstraints = append(
				columnConstraints,
				&UnnamedConstraintExpr{
					Constraint: &ConstraintNullableExpr{Nullable: Nullable(!column.Value.Schema.Value.NotNull)},
				},
			)
		}
		if !isCustom && column.Value.Schema.Value.Default != nil {
			columnConstraints = append(
				columnConstraints,
				&UnnamedConstraintExpr{
					Constraint: &ConstraintDefaultExpr{Expression: &Literal{Text: *column.Value.Schema.Value.Default}},
				},
			)
		}
		fields = append(fields, &SqlField{
			Name: &Literal{Text: column.Value.Name},
			Describer: &ShortTypeDesc{
				TypeName:  columnType,
				Collation: nil, // TODO COLLATION
			},
			Constraints: append(columnConstraints, makeConstraintsExpr(true, column.Value.Constraints)...),
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
		constraints = append(constraints, &ConstraintWithColumns{
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
		Create: &TableBodyDescriber{
			Fields:      fields,
			Constraints: constraints,
		},
	}
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
	if c.SetDrop == SetDropSet {
		return "set " + c.Expr.Expression()
	} else {
		return "drop " + c.Expr.Expression()
	}
}

func (c *AlterAttributeExpr) Expression() string {
	return fmt.Sprintf("alter attribute %s %s", c.AttributeName, c.AlterExpr.Expression())
}

func (c *AlterDataTypeExpr) Expression() string {
	if c.Length == nil && c.Precision == nil {
		return fmt.Sprintf("type %s", c.DataType)
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
	return fmt.Sprintf("constraint %s %s", c.Name.GetName(), c.Constraint.ConstraintString())
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
	return fmt.Sprintf("%s %s%s%s", c.Name.GetName(), c.DataType, collation, constraints)
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
		return fmt.Sprintf("(\n/* begin */\n\t%s\n/* end */\n)", strings.Join(exprs, ",\n\t"))
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
	if iArrayContains(sqlReservedWords, c.Text) {
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

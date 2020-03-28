package dragonfly

import (
	"fmt"
	sqt "github.com/iv-menshenin/dragonfly/sql_ast"
	"strings"
)

func makeSetDropExpr(setDrop bool, expr sqt.SqlExpr) sqt.SqlExpr {
	return &sqt.SetDropExpr{
		SetDrop: sqt.SetDrop(setDrop),
		Expr:    expr,
	}
}

func makeAddColumnExpr(column ColumnRef) sqt.SqlExpr {
	return &sqt.AddExpr{
		Target: sqt.TargetColumn,
		Name:   &sqt.Literal{Text: column.Value.Name},
		Definition: &sqt.DataTypeExpr{
			DataType:  column.Value.Schema.Value.Type,
			IsArray:   column.Value.Schema.Value.IsArray,
			Length:    column.Value.Schema.Value.Length,
			Precision: column.Value.Schema.Value.Precision,
			Collation: column.Value.Schema.Value.Collate,
		}, // TODO column constraints
	}
}

func makeDomainSetSchema(domain string, rename NameComparator) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetDomain,
		Name: &sqt.Selector{
			Name:      domain,
			Container: rename.Actual,
		},
		Alter: &sqt.SetMetadataExpr{
			Set: &sqt.SchemaExpr{
				SchemaName: rename.New,
			},
		},
	}
}

func makeTypeSetSchema(domain string, rename NameComparator) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetType,
		Name: &sqt.Selector{
			Name:      domain,
			Container: rename.Actual,
		},
		Alter: &sqt.SetMetadataExpr{
			Set: &sqt.SchemaExpr{
				SchemaName: rename.New,
			},
		},
	}
}

func makeDomainRename(schema string, rename NameComparator) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetDomain,
		Name: &sqt.Selector{
			Name:      rename.Actual,
			Container: schema,
		},
		Alter: &sqt.SqlRename{
			NewName: &sqt.Literal{Text: rename.New},
		},
	}
}

func makeTypeRename(schema string, rename NameComparator) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetType,
		Name: &sqt.Selector{
			Name:      rename.Actual,
			Container: schema,
		},
		Alter: &sqt.SqlRename{
			NewName: &sqt.Literal{Text: rename.New},
		},
	}
}

func makeTypeAlterAttributeDataType(schemaName, typeName, attrName string, typeSchema TypeBase) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetType,
		Name: &sqt.Selector{
			Name:      typeName,
			Container: schemaName,
		},
		Alter: makeAlterAttributeDataType(attrName, typeSchema),
	}

}

func makeAlterAttributeDataType(attrName string, typeSchema TypeBase) sqt.SqlExpr {
	return &sqt.AlterAttributeExpr{
		AttributeName: attrName,
		AlterExpr: &sqt.AlterDataTypeExpr{
			DataType:  typeSchema.Type,
			Length:    typeSchema.Length,
			Precision: typeSchema.Precision,
		},
	}
}

func makeDomainSetNotNull(schema, domain string, notNull bool) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetDomain,
		Name: &sqt.Selector{
			Name:      domain,
			Container: schema,
		},
		Alter: makeSetDropExpr(notNull, &sqt.NotNullClause{}),
	}
}

func makeDomainSetDefault(schema, domain string, defaultValue *string) sqt.SqlStmt {
	if defaultValue == nil {
		return &sqt.AlterStmt{
			Target: sqt.TargetDomain,
			Name: &sqt.Selector{
				Name:      domain,
				Container: schema,
			},
			Alter: makeSetDropExpr(false, &sqt.Literal{Text: "default"}),
		}
	} else {
		return &sqt.AlterStmt{
			Target: sqt.TargetDomain,
			Name: &sqt.Selector{
				Name:      domain,
				Container: schema,
			},
			Alter: makeSetDropExpr(true, &sqt.Default{Default: &sqt.Literal{Text: *defaultValue}}),
		}
	}
}

func makeDomain(schema, domain string, domainSchema DomainSchema) sqt.SqlStmt {
	return &sqt.CreateStmt{
		Target: sqt.TargetDomain,
		Name: &sqt.Selector{
			Name:      domain,
			Container: schema,
		},
		Create: &sqt.TypeDescription{
			Type:      domainSchema.Type,
			Length:    domainSchema.Length,
			Precision: domainSchema.Precision,
			Null:      sqt.Nullable(!domainSchema.NotNull),
			Default:   defaultToSQL(domainSchema.Default),
			Check:     domainSchema.Check,
		},
	}
}

func makeType(schema, typeName string, typeSchema TypeSchema) sqt.SqlStmt {
	var create sqt.SqlExpr
	if strings.EqualFold(typeSchema.Type, "record") {
		fields := make([]sqt.SqlExpr, len(typeSchema.Fields))
		for i, f := range typeSchema.Fields {
			fieldTypeName := f.Value.Schema.Value.Type
			if typeSchema, typeName, ok := f.Value.Schema.makeCustomType(); ok {
				fieldTypeName = fmt.Sprintf("%s.%s", typeSchema, typeName)
			}
			fields[i] = &sqt.ColumnDefinitionExpr{
				Name:        &sqt.Literal{Text: f.Value.Name},
				DataType:    fieldTypeName,
				Collation:   nil, // TODO COLLATION
				Constraints: nil,
			}
		}
		create = &sqt.RecordDescription{
			Fields: fields,
		}
	} else if strings.EqualFold(typeSchema.Type, "enum") {
		values := make([]string, len(typeSchema.Enum))
		for i, f := range typeSchema.Enum {
			values[i] = f.Value
		}
		create = &sqt.EnumDescription{
			Values: values,
		}
	} else if strings.EqualFold(typeSchema.Type, "map") {
		values := make([]string, len(typeSchema.Enum))
		for i, f := range typeSchema.Enum {
			values[i] = f.Value
		}
		create = &sqt.RecordDescription{
			Fields: []sqt.SqlExpr{
				&sqt.ColumnDefinitionExpr{
					Name:     &sqt.Literal{Text: "data"},
					DataType: "json",
				},
			},
		}
	} else {
		panic(fmt.Sprintf("unknown type `%s`", typeSchema.Type))
	}
	return &sqt.CreateStmt{
		Target: sqt.TargetType,
		Name: &sqt.Selector{
			Name:      typeName,
			Container: schema,
		},
		Create: create,
	}
}

func makeDomainDrop(schema, domain string) sqt.SqlStmt {
	return &sqt.DropStmt{
		Target: sqt.TargetDomain,
		Name: &sqt.Selector{
			Name:      domain,
			Container: schema,
		},
	}
}

func makeTypeDrop(schema, typeName string) sqt.SqlStmt {
	return &sqt.DropStmt{
		Target: sqt.TargetType,
		Name: &sqt.Selector{
			Name:      typeName,
			Container: schema,
		},
	}
}

/* COLUMNS */

func makeColumnRename(schema, table string, name NameComparator) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetTable,
		Name: &sqt.Selector{
			Name:      table,
			Container: schema,
		},
		Alter: &sqt.SqlRename{
			Target:  sqt.TargetColumn,
			OldName: &sqt.Literal{Text: name.Actual},
			NewName: &sqt.Literal{Text: name.New},
		},
	}
}

func makeColumnAdd(schema, table string, column ColumnRef) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetTable,
		Name: &sqt.Selector{
			Name:      table,
			Container: schema,
		},
		Alter: makeAddColumnExpr(column),
	}
}

func makeColumnDrop(schema, table, column string, ifExists, cascade bool) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetTable,
		Name: &sqt.Selector{
			Name:      table,
			Container: schema,
		},
		Alter: &sqt.DropExpr{
			Target:   sqt.TargetColumn,
			Name:     &sqt.Literal{Text: column},
			IfExists: ifExists,
			Cascade:  cascade,
		},
	}
}

func makeAlterColumnSetNotNull(schema, table, column string, notNull bool) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetTable,
		Name: &sqt.Selector{
			Name:      table,
			Container: schema,
		},
		Alter: &sqt.AlterExpr{
			Target: sqt.TargetColumn,
			Name:   &sqt.Literal{Text: column},
			Alter:  makeSetDropExpr(notNull, &sqt.NotNullClause{}),
		},
	}
}

func makeAlterColumnSetType(schema, table, column string, domainSchema DomainSchema) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetTable,
		Name: &sqt.Selector{
			Name:      table,
			Container: schema,
		},
		Alter: &sqt.AlterExpr{
			Target: sqt.TargetColumn,
			Name:   &sqt.Literal{Text: column},
			Alter: &sqt.TypeDescription{
				Type:      domainSchema.Type,
				Length:    domainSchema.Length,
				Precision: domainSchema.Precision,
			},
		},
	}
}

func makeAlterColumnSetDomain(schema, table, column, domainName string) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetTable,
		Name: &sqt.Selector{
			Name:      table,
			Container: schema,
		},
		Alter: &sqt.AlterExpr{
			Target: sqt.TargetColumn,
			Name:   &sqt.Literal{Text: column},
			Alter: &sqt.TypeDescription{
				Type: domainName,
			},
		},
	}
}

/* TABLE */

func makeTableSetSchema(table string, rename NameComparator) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetTable,
		Name: &sqt.Selector{
			Name:      table,
			Container: rename.Actual,
		},
		Alter: &sqt.SetMetadataExpr{
			Set: &sqt.SchemaExpr{
				SchemaName: rename.New,
			},
		},
	}
}

func makeTableRename(schema string, rename NameComparator) sqt.SqlStmt {
	return &sqt.AlterStmt{
		Target: sqt.TargetTable,
		Name: &sqt.Selector{
			Name:      rename.Actual,
			Container: schema,
		},
		Alter: &sqt.SqlRename{
			NewName: &sqt.Literal{Text: rename.New},
		},
	}
}

func makeTableDrop(schema, tableName string) sqt.SqlStmt {
	return &sqt.DropStmt{
		Target: sqt.TargetTable,
		Name: &sqt.Selector{
			Name:      tableName,
			Container: schema,
		},
	}
}

func stringToOnDeleteUpdateRule(s *string) sqt.OnDeleteUpdateRule {
	if s == nil {
		return sqt.RuleNoAction
	}
	switch strings.ToLower(*s) {
	case "cascade":
		return sqt.RuleCascade
	case "restrict":
		return sqt.RuleRestrict
	case "set null":
		return sqt.RuleSetNull
	case "default":
		return sqt.RuleSetDefault
	default:
		panic(fmt.Sprintf("cannot resolve update (delete) rule `%s`", *s))
	}
}

func makeConstraintInterface(inColumn bool, constraintDef Constraint) sqt.ConstraintInterface {
	var newConstraint sqt.ConstraintInterface
	switch constraintDef.Type {
	case ConstraintPrimaryKey:
		newConstraint = &sqt.ConstraintPrimaryKeyExpr{
			ConstraintCommon: sqt.ConstraintCommon{InColumn: inColumn},
		}
	case ConstraintCheck:
		if params, ok := constraintDef.Parameters.Parameter.(Check); ok {
			newConstraint = &sqt.ConstraintCheckExpr{
				ConstraintCommon: sqt.ConstraintCommon{InColumn: inColumn},
				Expression:       &sqt.Literal{Text: params.Expression},
				Where:            nil,
			}
		} else {
			panic("the check constraint should contains the check expression")
		}
	case ConstraintUniqueKey:
		if params, ok := constraintDef.Parameters.Parameter.(Where); ok {
			newConstraint = &sqt.ConstraintUniqueExpr{
				ConstraintCommon: sqt.ConstraintCommon{InColumn: inColumn},
				Where:            &sqt.Literal{Text: params.Where},
			}
		} else {
			newConstraint = &sqt.ConstraintUniqueExpr{
				ConstraintCommon: sqt.ConstraintCommon{InColumn: inColumn},
				Where:            nil,
			}
		}
	case ConstraintForeignKey:
		if params, ok := constraintDef.Parameters.Parameter.(ForeignKey); ok {
			newConstraint = &sqt.ConstraintForeignKeyExpr{
				ConstraintCommon: sqt.ConstraintCommon{InColumn: inColumn},
				ToTable:          &sqt.Literal{Text: params.ToTable},
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

func makeConstraintsExpr(inColumn bool, constraintSet []Constraint) []sqt.ConstraintExpr {
	var constraints = make([]sqt.ConstraintExpr, 0, len(constraintSet))
	for _, constraintDef := range constraintSet {
		if constraintDef.Name != "" {
			constraints = append(constraints, &sqt.NamedConstraintExpr{
				Name:       &sqt.Literal{constraintDef.Name},
				Constraint: makeConstraintInterface(inColumn, constraintDef),
			})
		} else {
			constraints = append(constraints, &sqt.UnnamedConstraintExpr{
				Constraint: makeConstraintInterface(inColumn, constraintDef),
			})
		}
	}
	return constraints
}

func makeTableCreate(schemaName, tableName string, tableStruct Table) sqt.SqlStmt {
	/*
		https://postgrespro.ru/docs/postgresql/9.6/sql-createtable
	*/
	var (
		fields      = make([]sqt.FieldDescriber, 0, len(tableStruct.Columns))
		constraints = make([]sqt.ConstraintExpr, 0, len(tableStruct.Constraints))
	)
	for _, column := range tableStruct.Columns {
		var (
			columnType sqt.SqlExpr
		)
		customSchema, customType, isCustom := column.Value.Schema.makeCustomType()
		if isCustom {
			columnType = &sqt.Selector{Name: customType, Container: customSchema}
		} else {
			columnType = &sqt.DataTypeExpr{
				DataType:  column.Value.Schema.Value.Type,
				IsArray:   column.Value.Schema.Value.IsArray,
				Length:    column.Value.Schema.Value.Length,
				Precision: column.Value.Schema.Value.Precision,
				Collation: column.Value.Schema.Value.Collate,
			}
		}
		var columnConstraints = make([]sqt.ConstraintExpr, 0, len(column.Value.Constraints)+2)
		if !isCustom && column.Value.Schema.Value.NotNull {
			columnConstraints = append(
				columnConstraints,
				&sqt.UnnamedConstraintExpr{
					Constraint: &sqt.ConstraintNullableExpr{Nullable: sqt.Nullable(!column.Value.Schema.Value.NotNull)},
				},
			)
		}
		def := defaultToSQL(column.Value.Schema.Value.Default)
		if !isCustom && def != nil {
			columnConstraints = append(
				columnConstraints,
				&sqt.UnnamedConstraintExpr{
					Constraint: &sqt.ConstraintDefaultExpr{Expression: &sqt.Literal{Text: *def}},
				},
			)
		}
		fields = append(fields, &sqt.SqlField{
			Name: &sqt.Literal{Text: column.Value.Name},
			Describer: &sqt.ShortTypeDesc{
				TypeName:  columnType,
				Collation: nil, // TODO COLLATION
			},
			Constraints: append(columnConstraints, makeConstraintsExpr(true, column.Value.Constraints)...),
		})
	}
	for _, constraint := range tableStruct.Constraints {
		constraintInterface := makeConstraintInterface(false, constraint.Constraint)
		var constraintExpr sqt.ConstraintExpr
		if constraint.Constraint.Name != "" {
			constraintExpr = &sqt.NamedConstraintExpr{
				Name:       &sqt.Literal{constraint.Constraint.Name},
				Constraint: constraintInterface,
			}
		} else {
			constraintExpr = &sqt.UnnamedConstraintExpr{
				Constraint: constraintInterface,
			}
		}
		constraints = append(constraints, &sqt.ConstraintWithColumns{
			Columns:    constraint.Columns,
			Constraint: constraintExpr,
		})
	}
	return &sqt.CreateStmt{
		Target: sqt.TargetTable,
		Name: &sqt.Selector{
			Name:      tableName,
			Container: schemaName,
		},
		Create: &sqt.TableBodyDescriber{
			Fields:      fields,
			Constraints: constraints,
		},
	}
}

package sql_ast

type (
	TableDesc struct {
		Table SqlIdent
		Alias string
	}
	NamedObject struct {
		Schema string
		Object string
		Field  string
	}
	Dependencies []NamedObject

	SqlTarget          int
	OnDeleteUpdateRule int
	Nullable           bool
	SetDrop            bool

	SqlStmt interface {
		String() string
		statement() int
		dependedOn() Dependencies
		solved() Dependencies
	}
	SqlIdent interface {
		GetName() string
	}
	SqlExpr interface {
		String() string
		expression() int
		dependedOn() Dependencies
	}
)

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

func concatDependencies(a, b Dependencies) Dependencies {
	return append(a, b...)
}

func dependedOn2(s, n string) Dependencies {
	return Dependencies{
		NamedObject{
			Schema: s,
			Object: n,
		},
	}
}

func dependedOn3(s, n, f string) Dependencies {
	return Dependencies{
		NamedObject{
			Schema: s,
			Object: n,
			Field:  f,
		},
	}
}

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

func (c SetDrop) String() string {
	if c {
		return "set"
	} else {
		return "drop"
	}
}

type (
	SqlNullable struct {
		Nullable Nullable
	}
)

func (c *SqlNullable) String() string {
	switch c.Nullable {
	case NullableNotNull:
		return "not null"
	case NullableNull:
		return "null"
	default:
		return ""
	}
}

func ExploreDependencies(stmt SqlStmt) Dependencies {
	return stmt.dependedOn()
}

func ExploreResolved(stmt SqlStmt) Dependencies {
	return stmt.solved()
}

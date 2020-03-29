package sql_ast

type (
	SqlStmt interface {
		String() string
		statement() int
	}
	SqlIdent interface {
		GetName() string
	}
	SqlExpr interface {
		Expression() string
	}
)

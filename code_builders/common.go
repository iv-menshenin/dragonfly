package builders

import (
	"go/ast"
	"go/token"
	"strings"
)

var (
	// 0
	Zero = IntegerConstant(0).Expr()
	// _
	Blank = ast.NewIdent("_")
	// nil
	Nil = ast.NewIdent("nil")
	// interface{}
	EmptyInterface = &ast.InterfaceType{
		Methods: &ast.FieldList{},
	}

	// uint
	UInt = ast.NewIdent("uint")
	// uint8
	UInt8 = ast.NewIdent("uint8")
	// uint16
	UInt16 = ast.NewIdent("uint16")
	// uint32
	UInt32 = ast.NewIdent("uint32")
	// uint64
	UInt64 = ast.NewIdent("uint64")

	// int
	Int = ast.NewIdent("int")
	// int8
	Int8 = ast.NewIdent("int8")
	// int16
	Int16 = ast.NewIdent("int16")
	// int32
	Int32 = ast.NewIdent("int32")
	// int64
	Int64 = ast.NewIdent("int64")

	// float32
	Float32 = ast.NewIdent("float32")
	// float64
	Float64 = ast.NewIdent("float64")

	// string
	String = ast.NewIdent("string")

	// context.Context
	ContextType = SimpleSelector("context", "Context")
)

// import declaration with token.IMPORT
func Import(lPos *token.Pos, imports map[string]string) ast.Decl {
	var (
		impSpec []ast.Spec
		lParen  = *lPos
	)
	*lPos++
	impSpec = makeImportSpec(lPos, imports)
	return &ast.GenDecl{
		Lparen: lParen,
		Tok:    token.IMPORT,
		Specs:  impSpec,
	}
}

// ast.CommentGroup. "nil" if arguments is omitted or empty
func CommentGroup(comment ...string) *ast.CommentGroup {
	if len(comment) == 0 {
		return nil
	} else {
		if len(comment) == 1 && strings.TrimSpace(comment[0]) == "" {
			return nil
		}
	}
	return &ast.CommentGroup{
		List: []*ast.Comment{
			{
				Slash: 1,
				Text:  " /* " + strings.Join(comment, "\n") + " */\n",
			},
		},
	}
}

// ast.Field constructor.
// docAndComments contains the first line as Docstring, all other lines turn into CommentGroup
func Field(name string, tag *ast.BasicLit, fieldType ast.Expr, docAndComments ...string) *ast.Field {
	if fieldType == nil {
		return nil
	}
	var (
		doc      = ""
		comments []string
		names    = make([]*ast.Ident, 0, 1)
	)
	if name != "" {
		names = []*ast.Ident{ast.NewIdent(name)}
	}
	if len(docAndComments) > 0 {
		doc = docAndComments[0]
		comments = docAndComments[1:]
	}
	return &ast.Field{
		Doc:     CommentGroup(doc),
		Names:   names,
		Type:    fieldType,
		Tag:     tag,
		Comment: CommentGroup(comments...),
	}
}

// creates ast.FieldList, any nil values will be excluded from list
func FieldList(fields ...*ast.Field) *ast.FieldList {
	var list = ast.FieldList{
		List: make([]*ast.Field, 0, len(fields)),
	}
	for i, field := range fields {
		if field != nil {
			list.List = append(list.List, fields[i])
		}
	}
	return &list
}

// creates ast.ValueSpec with Type field
func VariableType(name string, varType ast.Expr) *ast.ValueSpec {
	return &ast.ValueSpec{
		Names: []*ast.Ident{
			ast.NewIdent(name),
		},
		Type: varType,
	}
}

// creates ast.ValueSpec with Values field
func VariableValue(name string, varValue ast.Expr) *ast.ValueSpec {
	return &ast.ValueSpec{
		Names: []*ast.Ident{
			ast.NewIdent(name),
		},
		Values: []ast.Expr{varValue},
	}
}

type assignToken int

const (
	Assignment  assignToken = iota + 1 // =
	Incremental                        // +=
	Decremental                        // -=
	Definition                         // :=
)

func (t assignToken) token() token.Token {
	switch t {
	case Assignment:
		return token.ASSIGN
	case Incremental:
		return token.ADD_ASSIGN
	case Decremental:
		return token.SUB_ASSIGN
	case Definition:
		return token.DEFINE
	default:
		panic("unknown assignment token")
	}
}

type (
	// represents a list of variable names
	VarNames []string
)

func (c VarNames) expression() []ast.Expr {
	var varNames = make([]ast.Expr, 0, len(c))
	for _, varName := range c {
		varNames = append(varNames, ast.NewIdent(varName))
	}
	return varNames
}

// creates ast.AssignStmt which assigns a variable with a value
func Assign(varNames VarNames, tok assignToken, rhs ...ast.Expr) ast.Stmt {
	return &ast.AssignStmt{
		Lhs: varNames.expression(),
		Tok: tok.token(),
		Rhs: rhs,
	}
}

package builders

import (
	"github.com/iv-menshenin/dragonfly/utils"
	"go/ast"
)

const (
	TagTypeSQL   = "sql"
	TagTypeUnion = "union"
	TagTypeOp    = "operator"
)

// get a list of table columns and variable fields references for the output structure.
// column and field positions correspond to each other
func ExtractDestinationFieldRefsFromStruct(
	rowVariableName string,
	rowStructureFields []*ast.Field,
) (
	destinationStructureFields []ast.Expr,
	sourceTableColumnNames []string,
) {
	destinationStructureFields = make([]ast.Expr, 0, len(rowStructureFields))
	sourceTableColumnNames = make([]string, 0, len(rowStructureFields))
	for _, field := range rowStructureFields {
		if field.Tag != nil {
			tags := utils.FieldTagToMap(field.Tag.Value)
			if sqlTags, ok := tags[TagTypeSQL]; ok && len(sqlTags) > 0 && sqlTags[0] != "-" {
				for _, fName := range field.Names {
					destinationStructureFields = append(
						destinationStructureFields,
						MakeRef(MakeSelectorExpression(rowVariableName, fName.Name)),
					)
					sourceTableColumnNames = append(sourceTableColumnNames, sqlTags[0])
				}
			}
		}
	}
	return
}

func MakeDatabaseApiFunction(
	functionName string,
	resultExpr ast.Expr,
	functionBody []ast.Stmt,
	functionArgs ...*ast.Field,
) *ast.FuncDecl {
	return &ast.FuncDecl{
		Name: ast.NewIdent(functionName),
		Type: &ast.FuncType{
			Params: &ast.FieldList{
				List: append(
					[]*ast.Field{
						MakeField("ctx", nil, ContextType),
					},
					functionArgs...,
				),
			},
			Results: &ast.FieldList{
				List: []*ast.Field{
					MakeField("result", nil, resultExpr),
					MakeField("err", nil, ast.NewIdent("error")),
				},
			},
		},
		Body: &ast.BlockStmt{
			List: functionBody,
		},
	}
}

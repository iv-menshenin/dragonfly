package dragonfly

import (
	"fmt"
	"go/ast"
	"reflect"
	"testing"
)

func Test_mergeCodeBase(t *testing.T) {
	type args struct {
		main AstData
		next AstData
	}
	tests := []struct {
		name  string
		args  args
		Need  AstData
		Error bool
	}{
		{
			name: "simple",
			args: args{
				main: AstData{
					Types: map[string]*ast.TypeSpec{
						"Type1": {
							Name: &ast.Ident{Name: "Type1"},
							Type: &ast.SelectorExpr{
								X:   &ast.Ident{Name: "test1"},
								Sel: &ast.Ident{Name: "value"},
							},
						},
					},
					Constants:       nil,
					Implementations: nil,
				},
				next: AstData{
					Types: map[string]*ast.TypeSpec{
						"Type2": {
							Name: &ast.Ident{Name: "Type2"},
							Type: &ast.SelectorExpr{
								X:   &ast.Ident{Name: "test2"},
								Sel: &ast.Ident{Name: "value"},
							},
						},
					},
					Constants:       nil,
					Implementations: nil,
				},
			},
			Need: AstData{
				Types: map[string]*ast.TypeSpec{
					"Type1": {
						Name: &ast.Ident{Name: "Type1"},
						Type: &ast.SelectorExpr{
							X:   &ast.Ident{Name: "test1"},
							Sel: &ast.Ident{Name: "value"},
						},
					},
					"Type2": {
						Name: &ast.Ident{Name: "Type2"},
						Type: &ast.SelectorExpr{
							X:   &ast.Ident{Name: "test2"},
							Sel: &ast.Ident{Name: "value"},
						},
					},
				},
				Constants:       nil,
				Implementations: nil,
			},
		},
		{
			name: "error doubles",
			args: args{
				main: AstData{
					Types: map[string]*ast.TypeSpec{
						"Type1": {
							Name: &ast.Ident{Name: "Type1"},
							Type: &ast.SelectorExpr{
								X:   &ast.Ident{Name: "test1"},
								Sel: &ast.Ident{Name: "value"},
							},
						},
					},
					Constants:       nil,
					Implementations: nil,
				},
				next: AstData{
					Types: map[string]*ast.TypeSpec{
						"Type1": {
							Name: &ast.Ident{Name: "Type1"},
							Type: &ast.SelectorExpr{
								X:   &ast.Ident{Name: "test2"},
								Sel: &ast.Ident{Name: "value"},
							},
						},
					},
					Constants:       nil,
					Implementations: nil,
				},
			},
			Error: true,
		},
		{
			name: "same doubles",
			args: args{
				main: AstData{
					Types: map[string]*ast.TypeSpec{
						"Type1": {
							Name: &ast.Ident{Name: "Type1"},
							Type: &ast.SelectorExpr{
								X:   &ast.Ident{Name: "test1"},
								Sel: &ast.Ident{Name: "value"},
							},
						},
					},
					Constants:       nil,
					Implementations: nil,
				},
				next: AstData{
					Types: map[string]*ast.TypeSpec{
						"Type1": {
							Name: &ast.Ident{Name: "Type1"},
							Type: &ast.SelectorExpr{
								X:   &ast.Ident{Name: "test1"},
								Sel: &ast.Ident{Name: "value"},
							},
						},
					},
					Constants:       nil,
					Implementations: nil,
				},
			},
			Need: AstData{
				Types: map[string]*ast.TypeSpec{
					"Type1": {
						Name: &ast.Ident{Name: "Type1"},
						Type: &ast.SelectorExpr{
							X:   &ast.Ident{Name: "test1"},
							Sel: &ast.Ident{Name: "value"},
						},
					},
				},
				Constants:       nil,
				Implementations: nil,
			},
			Error: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := tt.args.main, tt.args.next
			err := mergeCodeBase(&a, &b)
			if (err != nil) != tt.Error {
				t.Error(fmt.Sprintf("need error: %v\nerror: %v", tt.Error, err))
			}
			if err == nil && !reflect.DeepEqual(a, tt.Need) {
				println(fmt.Sprintf("main: %+v\nnext: %+v\nneed: %+v", a, b, tt.Need))
				t.Error(fmt.Sprintf("Error in %s", tt.name))
			}
		})
	}
}

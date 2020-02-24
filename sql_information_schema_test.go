package dragonfly

import (
	"errors"
	"github.com/iv-menshenin/dragonfly/utils"
	"reflect"
	"testing"
)

func TestSchemas_getUnusedTableAndSetItAsUsed(t *testing.T) {
	type args struct {
		schemaName string
		tableName  string
	}
	tests := []struct {
		name string
		root Root
		args args
		want *Table
		test func(*Root) error
	}{
		{
			name: "simple test",
			root: Root{Schemas: []SchemaRef{
				{Value: Schema{
					Name: "schema-a",
					Tables: TablesContainer{
						"test1": {
							Inherits:    nil,
							Columns:     nil,
							Constraints: nil,
							Description: "",
							Api:         nil,
							used:        utils.RefBool(false),
						},
						"test2": {
							Inherits:    nil,
							Columns:     nil,
							Constraints: nil,
							Description: "Test Descr",
							Api:         nil,
							used:        utils.RefBool(false),
						},
					},
					Domains: nil,
				}},
			}},
			args: args{
				schemaName: "schema-a",
				tableName:  "test2",
			},
			want: &Table{
				Inherits:    nil,
				Columns:     nil,
				Constraints: nil,
				Description: "Test Descr",
				Api:         nil,
				used:        utils.RefBool(true),
			},
			test: func(f *Root) error {
				if s, ok := f.Schemas.tryToFind("schema-a"); ok {
					if *s.Value.Tables["test1"].used {
						return errors.New("field used contains TRUE value")
					}
				} else {
					return errors.New("schema not found")
				}
				if s, ok := f.Schemas.tryToFind("schema-a"); ok {
					if !*s.Value.Tables["test2"].used {
						return errors.New("field used contains FALSE value")
					}
				} else {
					return errors.New("schema not found")
				}
				return nil
			},
		},
		{
			name: "not found test",
			root: Root{Schemas: []SchemaRef{
				{Value: Schema{
					Name: "schema-a",
					Tables: TablesContainer{
						"test1": {
							Inherits:    nil,
							Columns:     nil,
							Constraints: nil,
							Description: "Test 1 bar",
							Api:         nil,
							used:        utils.RefBool(false),
						},
						"test2": {
							Inherits:    nil,
							Columns:     nil,
							Constraints: nil,
							Description: "Test 2 foo",
							Api:         nil,
							used:        utils.RefBool(false),
						},
					},
					Domains: nil,
				}},
			}},
			args: args{
				schemaName: "schema-a",
				tableName:  "test3",
			},
			want: nil,
			test: func(f *Root) error {
				if s, ok := f.Schemas.tryToFind("schema-a"); ok {
					if *s.Value.Tables["test1"].used {
						return errors.New("field used contains TRUE value")
					}
				} else {
					return errors.New("schema not found")
				}
				if s, ok := f.Schemas.tryToFind("schema-a"); ok {
					if *s.Value.Tables["test2"].used {
						return errors.New("field used contains TRUE value")
					}
				} else {
					return errors.New("schema not found")
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.root.getUnusedTableAndSetItAsUsed(tt.args.schemaName, tt.args.tableName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getUnusedTableAndSetItAsUsed() = %v, want %v", got, tt.want)
			}
			if tt.test != nil {
				if e := tt.test(&tt.root); e != nil {
					t.Error(e)
				}
			}
		})
	}
}

func TestRoot_getUnusedDomainAndSetItAsUsed(t *testing.T) {
	type fields struct {
		Schemas    Schemas
		Components Components
	}
	type args struct {
		schemaName string
		domainName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DomainSchema
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Root{
				Schemas:    tt.fields.Schemas,
				Components: tt.fields.Components,
			}
			if got := c.getUnusedDomainAndSetItAsUsed(tt.args.schemaName, tt.args.domainName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getUnusedDomainAndSetItAsUsed() = %v, want %v", got, tt.want)
			}
		})
	}
}

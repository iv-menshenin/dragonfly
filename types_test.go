package dragonfly

import (
	"errors"
	"testing"
	"time"
)

func Test_copyFromTo(t *testing.T) {
	type (
		testStruct struct {
			Field1 string
			Field2 time.Time
		}
	)
	var (
		testToString string
		testToStruct testStruct
		testToMap    map[string]int
	)
	tests := []struct {
		name  string
		to    interface{}
		from  func() interface{}
		check func() bool
	}{
		{
			name: "from var to ref",
			to:   &testToString,
			from: func() interface{} {
				return "TEST FOR PASS FROM STRING TO STRING"
			},
			check: func() bool {
				return testToString == "TEST FOR PASS FROM STRING TO STRING"
			},
		},
		{
			name: "from ref to ref",
			to:   &testToString,
			from: func() interface{} {
				fromStr := "TEST FOR PASS FROM *STRING TO *STRING"
				return &fromStr
			},
			check: func() bool {
				return testToString == "TEST FOR PASS FROM *STRING TO *STRING"
			},
		},
		{
			name: "from struct to struct",
			to:   &testToStruct,
			from: func() interface{} {
				var testStructFrom1 testStruct
				testStructFrom1.Field1 = "TEST STRUCT FIELD"
				testStructFrom1.Field2 = time.Time{}.Add(40000)
				return testStructFrom1
			},
			check: func() bool {
				return testToStruct.Field1 == "TEST STRUCT FIELD" && testToStruct.Field2 == time.Time{}.Add(40000)
			},
		},
		{
			name: "from map to map",
			to:   &testToMap,
			from: func() interface{} {
				return map[string]int{
					"one":     1,
					"two":     2,
					"hundred": 1000,
				}
			},
			check: func() bool {
				if i, ok := testToMap["one"]; !ok || i != 1 {
					return false
				}
				if i, ok := testToMap["two"]; !ok || i != 2 {
					return false
				}
				if i, ok := testToMap["hundred"]; !ok || i != 1000 {
					return false
				}
				return true
			},
		},
		{
			name: "from ref of struct to struct",
			to:   &testToStruct,
			from: func() interface{} {
				var testStructFrom2 testStruct
				testStructFrom2.Field1 = "TEST STRUCT FIELD FROM REF OF STRUCT"
				testStructFrom2.Field2 = time.Time{}.Add(20000)
				return &testStructFrom2
			},
			check: func() bool {
				return testToStruct.Field1 == "TEST STRUCT FIELD FROM REF OF STRUCT" && testToStruct.Field2 == time.Time{}.Add(20000)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copyFromTo(tt.from(), tt.to)
			if !tt.check() {
				t.Error("checking failed")
			}
		})
	}
}

func TestColumnRef_normalize(t *testing.T) {
	testRoot := Root{
		Schemas: []SchemaRef{
			{
				Value: Schema{
					Name: "test_schema_1",
					Tables: map[string]TableClass{
						"test_table_1": {
							Columns: []ColumnRef{
								{
									Value: Column{},
									Ref:   refString("#/components/columns/test_column_1"),
									used:  nil, // test it
								},
								{
									Value: Column{},
									Ref:   refString("#/components/columns/test_column_2"),
									used:  nil, // test it
								},
								{
									Value: Column{
										Name: "simple",
										Schema: ColumnSchemaRef{
											Value: DomainSchema{
												Type:    "int8",
												NotNull: false,
												used:    nil, // test it
											},
											Ref: nil,
										},
										Constraints: []Constraint{
											{
												Name: "Test3_{%Schema}_{%Table}_{%ColumnIndex}",
												Type: ConstraintUniqueKey,
											},
										},
									},
									Ref:  nil,
									used: nil, // test it
								},
							},
							used: refBool(false),
						},
						"test_table_2": {
							Columns: []ColumnRef{},
							used:    refBool(false),
						},
					},
				},
				Ref: nil,
			},
			{
				Value: Schema{
					Name: "test_schema_2",
					Tables: map[string]TableClass{
						"test_table_3": {
							Columns: []ColumnRef{},
							used:    refBool(false),
						},
					},
				},
				Ref: nil,
			},
		},
		Components: Components{
			Columns: map[string]Column{
				"test_column_1": {
					Name: "column_1",
					Schema: ColumnSchemaRef{
						Value: DomainSchema{
							Type:    "varchar",
							NotNull: true,
							used:    refBool(false),
						},
						Ref: nil,
					},
					Constraints: []Constraint{
						{
							Name: "Test1_{%Schema}_{%Table}_{%ColumnIndex}",
							Type: ConstraintPrimaryKey,
						},
					},
				},
				"test_column_2": {
					Name: "column_2",
					Schema: ColumnSchemaRef{
						Value: DomainSchema{
							Type:    "int8",
							NotNull: true,
							used:    refBool(false),
						},
						Ref: nil,
					},
					Constraints: []Constraint{
						{
							Name: "Test2_{%Table}_{%Schema}_{%ColumnIndex}",
							Type: ConstraintPrimaryKey,
						},
					},
				},
			},
			Classes: nil,
		},
	}
	type args struct {
		schema      *SchemaRef
		tableName   string
		columnIndex int
		db          *Root
	}
	tests := []struct {
		name   string
		column *ColumnRef
		args   args
		test   func(*ColumnRef) error
	}{
		{
			name:   "test simple: column.ref coping data checking",
			column: &testRoot.Schemas[0].Value.Tables["test_table_1"].Columns[0],
			args: args{
				schema:      &testRoot.Schemas[0],
				tableName:   "test_table_1",
				columnIndex: 0,
				db:          &testRoot,
			},
			test: func(col *ColumnRef) error {
				if col.Value.Name != "column_1" || col.Value.Schema.Value.Type != "varchar" {
					return errors.New("Column.Value: the object did not receive its contents by reference")
				}
				if col.Value.Constraints[0].Name != "Test1_test_schema_1_test_table_1_0" {
					return errors.New("Constraints[0].Name: did not complete the template conversion")
				}
				if testRoot.Components.Columns["test_column_1"].Constraints[0].Name != "Test1_{%Schema}_{%Table}_{%ColumnIndex}" {
					return errors.New("Column.Ref: the original data is distorted, the data must be copied but must not be changed")
				}
				if col.used == nil || *col.used {
					return errors.New("flag 'used' not initialized")
				}
				return nil
			},
		},
		{
			name:   "test another column",
			column: &testRoot.Schemas[0].Value.Tables["test_table_1"].Columns[1],
			args: args{
				schema:      &testRoot.Schemas[0],
				tableName:   "test_table_1",
				columnIndex: 1,
				db:          &testRoot,
			},
			test: func(col *ColumnRef) error {
				if col.Value.Name != "column_2" || col.Value.Schema.Value.Type != "int8" {
					return errors.New("Column.Value: the object did not receive its contents by reference")
				}
				if col.Value.Constraints[0].Name != "Test2_test_table_1_test_schema_1_1" {
					return errors.New("Constraints[0].Name: did not complete the template conversion")
				}
				if testRoot.Components.Columns["test_column_2"].Constraints[0].Name != "Test2_{%Table}_{%Schema}_{%ColumnIndex}" {
					return errors.New("Column.Ref: the original data is distorted, the data must be copied but must not be changed")
				}
				if col.used == nil || *col.used {
					return errors.New("flag 'used' not initialized")
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.column.normalize(tt.args.schema, tt.args.tableName, tt.args.columnIndex, tt.args.db)
			err := tt.test(tt.column)
			if err != nil {
				t.Error(err)
			}
		})
	}
}

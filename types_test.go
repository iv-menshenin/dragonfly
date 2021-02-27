package dragonfly

import (
	"errors"
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
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

// testing:
//  - processing 'ref'
//  - evaluating constraint name templates
//  - `used` flag
//  - column schemas normalization
func TestColumnRef_normalize(t *testing.T) {
	testRoot := Root{
		Schemas: []SchemaRef{
			{
				Value: Schema{
					Name: "test_schema_1",
					Tables: map[string]Table{
						"test_table_1": {
							Columns: []ColumnRef{
								{
									Value: Column{},
									Ref:   utils.RefString("#/components/columns/test_column_1"),
									used:  nil, // test it
								},
								{
									Value: Column{},
									Ref:   utils.RefString("#/components/columns/test_column_2"),
									used:  nil, // test it
								},
								{
									Value: Column{
										Name: "simple",
										Schema: ColumnSchemaRef{
											Value: DomainSchema{
												TypeBase: TypeBase{Type: "int8"},
												NotNull:  false,
												used:     nil, // test it
											},
											Ref: nil,
										},
										Constraints: []Constraint{
											{
												Name: "Test3_{%Schema}_{%Table}_{%Index}",
												Type: ConstraintUniqueKey,
											},
										},
									},
									Ref:  nil,
									used: nil, // test it
								},
							},
							used: utils.RefBool(false),
						},
						"test_table_2": {
							Columns: []ColumnRef{},
							used:    utils.RefBool(false),
						},
					},
				},
				Ref: nil,
			},
			{
				Value: Schema{
					Name: "test_schema_2",
					Tables: map[string]Table{
						"test_table_3": {
							Columns: []ColumnRef{},
							used:    utils.RefBool(false),
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
							TypeBase: TypeBase{Type: "varchar"},
							NotNull:  true,
							used:     utils.RefBool(false),
						},
						Ref: nil,
					},
					Constraints: []Constraint{
						{
							Name: "Test1_{%Schema}_{%Table}_{%Index}",
							Type: ConstraintPrimaryKey,
						},
					},
				},
				"test_column_2": {
					Name: "column_2",
					Schema: ColumnSchemaRef{
						Value: DomainSchema{
							TypeBase: TypeBase{Type: "int8"},
							NotNull:  true,
							used:     utils.RefBool(false),
						},
						Ref: nil,
					},
					Constraints: []Constraint{
						{
							Name: "Test2_{%Table}_{%Schema}_{%Index}",
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
				if testRoot.Components.Columns["test_column_1"].Schema.Value.used == nil || *testRoot.Components.Columns["test_column_1"].Schema.Value.used {
					return errors.New("TEST DATA INITIALIZATION ERROR")
				}
				if col.Value.Name != "column_1" || col.Value.Schema.Value.Type != "varchar" {
					return errors.New("Column.Value: the object did not receive its contents by reference")
				}
				if col.Value.Constraints[0].Name != "Test1_test_schema_1_test_table_1_0" {
					return errors.New("Constraints[0].Name: did not complete the template conversion")
				}
				if testRoot.Components.Columns["test_column_1"].Constraints[0].Name != "Test1_{%Schema}_{%Table}_{%Index}" {
					return errors.New("Column.Ref: the original data is distorted, the data must be copied but must not be changed")
				}
				if col.used == nil || *col.used {
					return errors.New("flag 'used' for column not initialized")
				}
				*col.used = true
				if col.Value.Schema.Value.used == nil || *col.Value.Schema.Value.used {
					return errors.New("flag 'used' for column schema not initialized")
				}
				*col.Value.Schema.Value.used = true
				if *testRoot.Components.Columns["test_column_1"].Schema.Value.used {
					return errors.New("REF instead of COPY: flag 'used' for column schema referenced to shared component 'column'")
				}
				return nil
			},
		},
		{
			name:   "test another referred column",
			column: &testRoot.Schemas[0].Value.Tables["test_table_1"].Columns[1],
			args: args{
				schema:      &testRoot.Schemas[0],
				tableName:   "test_table_1",
				columnIndex: 1,
				db:          &testRoot,
			},
			test: func(col *ColumnRef) error {
				if testRoot.Components.Columns["test_column_2"].Schema.Value.used == nil || *testRoot.Components.Columns["test_column_1"].Schema.Value.used {
					return errors.New("TEST DATA INITIALIZATION ERROR")
				}
				if col.Value.Name != "column_2" || col.Value.Schema.Value.Type != "int8" {
					return errors.New("Column.Value: the object did not receive its contents by reference")
				}
				if col.Value.Constraints[0].Name != "Test2_test_table_1_test_schema_1_0" {
					return errors.New("Constraints[0].Name: did not complete the template conversion")
				}
				if testRoot.Components.Columns["test_column_2"].Constraints[0].Name != "Test2_{%Table}_{%Schema}_{%Index}" {
					return errors.New("Column.Ref: the original data is distorted, the data must be copied but must not be changed")
				}
				if col.used == nil || *col.used {
					return errors.New("flag 'used' for column not initialized")
				}
				*col.used = true
				if col.Value.Schema.Value.used == nil || *col.Value.Schema.Value.used {
					return errors.New("flag 'used' for column schema not initialized")
				}
				*col.Value.Schema.Value.used = true
				if *testRoot.Components.Columns["test_column_2"].Schema.Value.used {
					return errors.New("REF instead of COPY: flag 'used' for column schema referenced to shared component 'column'")
				}
				return nil
			},
		},
		{
			name:   "test simple column",
			column: &testRoot.Schemas[0].Value.Tables["test_table_1"].Columns[2],
			args: args{
				schema:      &testRoot.Schemas[0],
				tableName:   "test_table_1",
				columnIndex: 2,
				db:          &testRoot,
			},
			test: func(col *ColumnRef) error {
				if col.Value.Name != "simple" || col.Value.Schema.Value.Type != "int8" {
					return errors.New("wrong column data")
				}
				if col.Value.Constraints[0].Name != "Test3_test_schema_1_test_table_1_0" {
					return errors.New("Constraints[0].Name: did not complete the template conversion")
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

func TestConstraintSchema_normalize(t *testing.T) {
	testRoot := Root{
		Schemas: []SchemaRef{
			{
				Value: Schema{
					Name: "test_schema_1",
					Tables: map[string]Table{
						"test_table_1": {
							Columns: []ColumnRef{
								{
									Value: Column{
										Name: "test_column",
										Schema: ColumnSchemaRef{
											Value: DomainSchema{
												TypeBase: TypeBase{Type: "int8"},
												used:     utils.RefBool(false),
											},
										},
									},
									used: utils.RefBool(false),
								},
							},
							Constraints: TableConstraints{
								{
									Columns: []string{"test_column_1"},
									Constraint: Constraint{
										Name:       "",
										Type:       ConstraintUniqueKey,
										Parameters: ConstraintParameters{Parameter: nil},
									},
								},
								{
									Columns: []string{"test_column_2"},
									Constraint: Constraint{
										Name: "",
										Type: ConstraintForeignKey,
										Parameters: ConstraintParameters{Parameter: ForeignKey{
											ToTable:  "schema1.table1",
											ToColumn: "fcolumn",
										}},
									},
								},
							},
							used: utils.RefBool(false),
						},
					},
				},
			},
		},
	}
	type args struct {
		schema          *SchemaRef
		tableName       string
		constraintIndex int
		db              *Root
	}
	tests := []struct {
		name       string
		constraint *ConstraintSchema
		args       args
		test       func(*ConstraintSchema) error
	}{
		{
			name:       "simple test",
			constraint: &testRoot.Schemas[0].Value.Tables["test_table_1"].Constraints[0],
			args: args{
				schema:          &testRoot.Schemas[0],
				tableName:       "test_table_1",
				constraintIndex: 0,
				db:              &testRoot,
			},
			test: func(constraint *ConstraintSchema) error {
				if constraint.Constraint.used == nil || *constraint.Constraint.used {
					return errors.New("field 'used' is not initialized")
				}
				if constraint.Constraint.Name != "ux_test_schema_1_test_table_1_0" {
					return errors.New("wrong generated constraint name")
				}
				return nil
			},
		},
		{
			name:       "test FK constraint",
			constraint: &testRoot.Schemas[0].Value.Tables["test_table_1"].Constraints[1],
			args: args{
				schema:          &testRoot.Schemas[0],
				tableName:       "test_table_1",
				constraintIndex: 1,
				db:              &testRoot,
			},
			test: func(constraint *ConstraintSchema) error {
				if constraint.Constraint.used == nil || *constraint.Constraint.used {
					return errors.New("field 'used' is not initialized")
				}
				if constraint.Constraint.Name != "fk_test_schema_1_test_table_1_schema1_table1" {
					return errors.New("wrong generated constraint name")
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.constraint.normalize(tt.args.schema, tt.args.tableName, tt.args.constraintIndex, tt.args.db)
			err := tt.test(tt.constraint)
			if err != nil {
				t.Error(err)
			}
		})
	}
}

/*  testing:
    - `used` flag
    - normalization for: columns, constraints, api
    - inheritance (with normalization)
*/
func TestTableClass_normalize(t *testing.T) {
	testRoot := Root{
		Schemas: []SchemaRef{
			{
				Value: Schema{
					Name: "test_schema_1",
					Tables: map[string]Table{
						"test_table_1": {
							Inherits: []string{"test_class_1"},
							Columns: []ColumnRef{
								{
									Value: Column{
										Name: "static_column",
										Schema: ColumnSchemaRef{
											Value: DomainSchema{
												TypeBase: TypeBase{Type: "int8"},
												used:     nil, // test it
											},
										},
									},
									used: nil, // test it
								},
							},
							used: nil, // test it
						},
						"test_table_2": {
							Inherits: []string{"test_class_1", "test_class_2"},
							Columns:  []ColumnRef{},
							Constraints: TableConstraints{
								{
									Columns: []string{"test_column_1"},
									Constraint: Constraint{
										Name:       "",
										Type:       ConstraintUniqueKey,
										Parameters: ConstraintParameters{Parameter: nil},
									},
								},
								{
									Columns: []string{"test_column_2"},
									Constraint: Constraint{
										Name: "",
										Type: ConstraintForeignKey,
										Parameters: ConstraintParameters{Parameter: ForeignKey{
											ToTable:  "schema1.table1",
											ToColumn: "fcolumn",
										}},
										used: utils.RefBool(false),
									},
								},
							},
							Api: []TableApi{
								{
									Type:          "api_type",
									Name:          "Api_Name_1",
									FindOptions:   nil,
									ModifyColumns: nil,
								},
							},
							used: nil, // test it
						},
						"test_table_3": {
							Inherits:    []string{"test_class_1", "test_class_2"},
							Columns:     []ColumnRef{},
							Constraints: TableConstraints{},
							used:        nil, // test it
						},
					},
				},
			},
		},
		Components: Components{
			Classes: map[string]TableClass{
				"test_class_1": {
					Columns: []ColumnRef{
						{
							Value: Column{
								Name: "inherited_column_1",
								Schema: ColumnSchemaRef{
									Value: DomainSchema{
										TypeBase: TypeBase{Type: "int4"},
										NotNull:  false,
										used:     utils.RefBool(false),
									},
								},
								Constraints: []Constraint{
									{
										Name:       "", // test
										Type:       ConstraintUniqueKey,
										Parameters: ConstraintParameters{Parameter: nil},
										used:       utils.RefBool(false),
									},
								},
							},
							used: utils.RefBool(false),
						},
						{
							Value: Column{
								Name: "inherited_column_2",
								Schema: ColumnSchemaRef{
									Value: DomainSchema{
										TypeBase: TypeBase{Type: "int8"},
										NotNull:  true,
										used:     utils.RefBool(false),
									},
								},
							},
							used: utils.RefBool(false),
						},
					},
					Constraints: nil,
					Api:         nil,
				},
				"test_class_2": {
					Columns: []ColumnRef{
						{
							Value: Column{
								Name: "inherited_column_3",
								Schema: ColumnSchemaRef{
									Value: DomainSchema{
										TypeBase: TypeBase{Type: "int4"},
										NotNull:  false,
										used:     utils.RefBool(false),
									},
								},
							},
							used: utils.RefBool(false),
						},
					},
					Constraints: TableConstraints{
						{
							Columns: []string{"inherited_column_1"},
							Constraint: Constraint{
								Name:       "Test_{%Schema}_{%Table}_{%Num}",
								Type:       ConstraintPrimaryKey,
								Parameters: ConstraintParameters{Parameter: nil},
								used:       utils.RefBool(false),
							},
						},
						{
							Columns: []string{"inherited_column_2"},
							Constraint: Constraint{
								Name:       "Test_{%Schema}_{%Table}_{%Num}",
								Type:       ConstraintUniqueKey,
								Parameters: ConstraintParameters{Parameter: nil},
								used:       utils.RefBool(false),
							},
						},
					},
					Api: ApiContainer{
						{
							Type: apiTypeInsertOne,
							Name: "InsertOneTest",
						},
						{
							Type: apiTypeInsertOne,
							Name: "", // test
						},
					},
				},
			},
		},
	}
	type args struct {
		schema    *SchemaRef
		tableName string
		db        *Root
	}
	table1 := testRoot.Schemas[0].Value.Tables["test_table_1"]
	table2 := testRoot.Schemas[0].Value.Tables["test_table_2"]
	table3 := testRoot.Schemas[0].Value.Tables["test_table_3"]
	tests := []struct {
		name  string
		table *Table
		args  args
		test  func(*Table) error
	}{
		{
			name:  "simple (column inherits)",
			table: &table1,
			args: args{
				schema:    &testRoot.Schemas[0],
				tableName: "test_table_1",
				db:        &testRoot,
			},
			test: func(table *Table) error {
				if f := testRoot.Components.Classes["test_class_1"].Columns.getColumn("inherited_column_1").Value.Schema.Value.used; f == nil || *f {
					return errors.New("TEST DATA INITIALIZATION ERROR")
				}
				if f := testRoot.Components.Classes["test_class_1"].Columns.getColumn("inherited_column_2").Value.Schema.Value.used; f == nil || *f {
					return errors.New("TEST DATA INITIALIZATION ERROR")
				}
				if table.used == nil || *table.used {
					return errors.New("flag 'used' for table not initialized")
				}
				for _, col := range table.Columns {
					if col.used == nil || *col.used {
						return errors.New(fmt.Sprintf("flag 'used' for column '%s' not initialized in table 'test_table_1'", col.Value.Name))
					}
					*col.used = true
					if col.Value.Schema.Value.used == nil || *col.Value.Schema.Value.used {
						return errors.New(fmt.Sprintf("flag 'used' for schema of column '%s' not initialized in table 'test_table_1'", col.Value.Name))
					}
					*col.Value.Schema.Value.used = true
				}
				if col, ok := table.Columns.tryToFind("inherited_column_1"); !ok {
					return errors.New("column 'inherited_column_1' not inherited")
				} else {
					if col.Value.Constraints[0].used == nil || *col.Value.Constraints[0].used {
						return errors.New("column 'inherited_column_1' not initialized")
					}
					if col.Value.Constraints[0].Name != "ux_test_schema_1_test_table_1_0" {
						return errors.New("column 'inherited_column_1' not initialized")
					}
				}
				if !table.Columns.exists("inherited_column_2") {
					return errors.New("column 'inherited_column_2' not inherited")
				}
				hCol := testRoot.Components.Classes["test_class_1"].Columns.getColumn("inherited_column_1")
				if *hCol.Value.Schema.Value.used {
					return errors.New("REF instead of COPY: flag 'used' for column schema referenced to shared component 'column'")
				}
				for _, constraint := range hCol.Value.Constraints {
					if constraint.Name != "" {
						return errors.New("constraints in the components container should not be normalized")
					}
					if *constraint.used {
						return errors.New("REF instead of COPY: flag 'used' for column constraint referenced to shared component 'column'")
					}
				}
				return nil
			},
		},
		{
			name:  "multiple inheritance (constraint and api inherits)",
			table: &table2,
			args: args{
				schema:    &testRoot.Schemas[0],
				tableName: "test_table_2",
				db:        &testRoot,
			},
			test: func(table *Table) error {
				if f := testRoot.Components.Classes["test_class_1"].Columns.getColumn("inherited_column_1").Value.Schema.Value.used; f == nil || *f {
					return errors.New("TEST DATA INITIALIZATION ERROR")
				}
				if f := testRoot.Components.Classes["test_class_1"].Columns.getColumn("inherited_column_2").Value.Schema.Value.used; f == nil || *f {
					return errors.New("TEST DATA INITIALIZATION ERROR")
				}
				if table.used == nil || *table.used {
					return errors.New("flag 'used' for table not initialized")
				}
				if col, ok := table.Columns.tryToFind("inherited_column_3"); ok {
					if col.used == nil || *col.used {
						return errors.New("flag 'used' for column 'inherited_column_3' not initialized")
					}
					*col.used = true
					if col.Value.Schema.Value.used == nil || *col.Value.Schema.Value.used {
						return errors.New("flag 'used' for schema of column 'inherited_column_3' not initialized")
					}
					*col.Value.Schema.Value.used = true
					hCol := testRoot.Components.Classes["test_class_2"].Columns.getColumn("inherited_column_3")
					if *hCol.Value.Schema.Value.used {
						return errors.New("REF instead of COPY: flag 'used' for column schema referenced to shared component 'column'")
					}
				} else {
					return errors.New("column 'inherited_column_3' not inherited")
				}
				if col, ok := table.Columns.tryToFind("inherited_column_1"); ok {
					if col.used == nil || *col.used {
						return errors.New("flag 'used' for column 'inherited_column_1' not initialized")
					}
					*col.used = true
					if col.Value.Schema.Value.used == nil || *col.Value.Schema.Value.used {
						return errors.New("flag 'used' for schema of column 'inherited_column_1' not initialized")
					}
					*col.Value.Schema.Value.used = true
					hCol := testRoot.Components.Classes["test_class_1"].Columns.getColumn("inherited_column_1")
					if *hCol.Value.Schema.Value.used {
						return errors.New("REF instead of COPY: flag 'used' for column schema referenced to shared component 'column'")
					}
				} else {
					return errors.New("column 'inherited_column_1' not inherited")
				}
				if len(table.Constraints) != 4 {
					return errors.New("failed to inherit all table constraints")
				}
				if len(table.Api) != 3 {
					return errors.New("failed to inherit all API")
				}
				if table.Api[1].Name != "test_schema_1_test_table_2_insertOne" {
					return errors.New("failed to check inherited API. wrong order or normalization")
				}
				return nil
			},
		},
		{
			name:  "empty table fill",
			table: &table3,
			args: args{
				schema:    &testRoot.Schemas[0],
				tableName: "test_table_3",
				db:        &testRoot,
			},
			test: func(table *Table) error {
				if len(table.Columns) != 3 {
					return errors.New("failed to inherit columns")
				}
				if len(table.Constraints) != 2 {
					return errors.New("failed to inherit constraints")
				}
				if len(table.Api) != 2 {
					return errors.New("failed to inherit APIs")
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run(tt.name, func(t *testing.T) {
				tt.table.normalize(tt.args.schema, tt.args.tableName, tt.args.db)
				err := tt.test(tt.table)
				if err != nil {
					t.Error(err)
				}
			})
		})
	}
}

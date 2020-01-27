package dragonfly

import (
	"fmt"
	"reflect"
	"testing"
)

func Test_getNextChain(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name      string
		args      args
		wantChain string
		wantLeft  string
		wantType  chainType
	}{
		{
			name:      "simple one",
			args:      args{s: "(test)"},
			wantChain: "test",
			wantLeft:  "",
			wantType:  ctArray,
		},
		{
			name:      "simple two",
			args:      args{s: "({VAR :test 12: case 3}{VAR:test 1:test 2})"},
			wantChain: "{VAR :test 12: case 3}{VAR:test 1:test 2}",
			wantLeft:  "",
			wantType:  ctArray,
		},
		{
			name:      "array parsing 1",
			args:      args{s: "{VAR :test 12: case 3} {VAR:test 1:test 2}"},
			wantChain: "VAR :test 12: case 3",
			wantLeft:  "{VAR:test 1:test 2}",
			wantType:  ctObject,
		},
		{
			name:      "array parsing 2",
			args:      args{s: "{VAR:test 1:test 2}"},
			wantChain: "VAR:test 1:test 2",
			wantLeft:  "",
			wantType:  ctObject,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotChain, gotLeft, gotType := getNextChain(tt.args.s)
			if gotChain != tt.wantChain {
				t.Errorf("getNextChain() gotChain = %v, want %v", gotChain, tt.wantChain)
			}
			if gotLeft != tt.wantLeft {
				t.Errorf("getNextChain() gotLeft = %v, want %v", gotLeft, tt.wantLeft)
			}
			if gotType != tt.wantType {
				t.Errorf("getNextChain() gotType = %v, want %v", gotType, tt.wantType)
			}
		})
	}
}

func Test_getNextField(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name      string
		args      args
		wantChain string
		wantLeft  string
	}{
		{
			name:      "simple",
			args:      args{s: "TYPE:field1 123:field2 1233"},
			wantChain: "TYPE",
			wantLeft:  "field1 123:field2 1233",
		},
		{
			name:      "last elem w/o terminator",
			args:      args{s: "field2 1233"},
			wantChain: "field2 1233",
			wantLeft:  "",
		},
		{
			name:      "last elem with terminator",
			args:      args{s: "field2 1233:"},
			wantChain: "field2 1233",
			wantLeft:  "",
		},
		{
			name:      "field with trim spaces",
			args:      args{s: " field1 123 : field2 1233 "},
			wantChain: "field1 123",
			wantLeft:  "field2 1233",
		},
		{
			name:      "data field",
			args:      args{s: "field1 [0, 5, 12, 0, 0] :field2 1233"},
			wantChain: "field1 [0, 5, 12, 0, 0]",
			wantLeft:  "field2 1233",
		},
		{
			name:      "object fields",
			args:      args{s: "field1 {VAR:ast: 00:type 00} :field2 {VAR:re 123}"},
			wantChain: "field1 {VAR:ast: 00:type 00}",
			wantLeft:  "field2 {VAR:re 123}",
		},
		{
			name:      "object fields nested",
			args:      args{s: "field1 {VAR:ast: 00:type 00:args ({VAR:var 1}{VAR:var 2})} :field2 {VAR:re 123}"},
			wantChain: "field1 {VAR:ast: 00:type 00:args ({VAR:var 1}{VAR:var 2})}",
			wantLeft:  "field2 {VAR:re 123}",
		},
		{
			name:      "array fields nested",
			args:      args{s: "field1 ({VAR:ast: 00:type 00:args ({VAR:var 1}{VAR:var 2})}) :field2 {VAR:re 123}"},
			wantChain: "field1 ({VAR:ast: 00:type 00:args ({VAR:var 1}{VAR:var 2})})",
			wantLeft:  "field2 {VAR:re 123}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotChain, gotLeft := getNextField(tt.args.s)
			if gotChain != tt.wantChain {
				t.Errorf("getNextField() gotChain = %v, want %v", gotChain, tt.wantChain)
			}
			if gotLeft != tt.wantLeft {
				t.Errorf("getNextField() gotLeft = %v, want %v", gotLeft, tt.wantLeft)
			}
		})
	}
}

// ({FUNCEXPR :funcid 870 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({RELABELTYPE :arg {VAR :varno 1 :varattno 6 :vartype 16410 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 1 :varoattno 6 :location 93} :resulttype 25 :resulttypmod -1 :resultcollid 100 :relabelformat 1 :location 101}) :location 87} {FUNCEXPR :funcid 870 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({FUNCEXPR :funcid 3060 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({RELABELTYPE :arg {VAR :varno 1 :varattno 9 :vartype 1043 :vartypmod 68 :varcollid 100 :varlevelsup 0 :varnoold 1 :varoattno 9 :location 121} :resulttype 25 :resulttypmod -1 :resultcollid 100 :relabelformat 1 :location 126} {CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 134 :constvalue 4 [ 5 0 0 0 0 0 0 0 ]}) :location 116}) :location 110})
func Test_parseObject(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want Node
	}{
		{
			name: "simple var",
			args: args{s: "VAR :varno 1 :varattno 9 :vartype 1043 :vartypmod 68 :varcollid 100 :varlevelsup 10 :varnoold 1 :varoattno 9 :location 121"},
			want: &VarNode{
				VarNo:       1,
				VarAttrNo:   9,
				VarType:     1043,
				VarTypeMod:  68,
				VarLevelSup: 10,
				VarNoOld:    1,
				VarOAttNo:   9,
			},
		},
		{
			name: "relabel with args",
			args: args{s: "RELABELTYPE :arg {VAR :varno 1 :varattno 6 :vartype 16410 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 1 :varoattno 6 :location 93} :resulttype 25 :resulttypmod -1 :resultcollid 100 :relabelformat 1 :location 101"},
			want: &ReLabelNode{
				Arg: &VarNode{
					VarNo:       1,
					VarAttrNo:   6,
					VarType:     16410,
					VarTypeMod:  -1,
					VarLevelSup: 0,
					VarNoOld:    1,
					VarOAttNo:   6,
				},
				ResultType:    25,
				ResultTypeMod: -1,
				ReLabelFormat: 1,
			},
		},
		{
			name: "hard type",
			args: args{s: "FUNCEXPR :funcid 870 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({FUNCEXPR :funcid 3060 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({RELABELTYPE :arg {VAR :varno 1 :varattno 9 :vartype 1043 :vartypmod 68 :varcollid 100 :varlevelsup 0 :varnoold 1 :varoattno 9 :location 121} :resulttype 25 :resulttypmod -1 :resultcollid 100 :relabelformat 1 :location 126} {CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 134 :constvalue 4 [ 5 0 0 0 0 0 0 0 ]}) :location 116}) :location 110"},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseObject(tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseObject() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_iNode_setFieldValue(t *testing.T) {
	type args struct {
		n Node
		s string
	}
	tests := []struct {
		name string
		args args
		want Node
	}{
		{
			name: "simple",
			args: args{
				n: &FuncNode{},
				s: "funcid 123",
			},
			want: &FuncNode{FuncId: 123},
		},
		{
			name: "bool value",
			args: args{
				n: &ConstNode{},
				s: "constbyval true",
			},
			want: &ConstNode{ConstByVal: true},
		},
		{
			name: "bool value",
			args: args{
				n: &ConstNode{},
				s: "constbyval true",
			},
			want: &ConstNode{ConstByVal: true},
		},
		{
			name: "data value",
			args: args{
				n: &ConstNode{},
				s: "constvalue [32 32 32]",
			},
			want: &ConstNode{ConstValue: []byte("   ")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var node = tt.args.n
			node.setFieldValue(tt.args.s)
			if !reflect.DeepEqual(tt.want, node) {
				t.Error(fmt.Sprintf("got: %+v, need: %+v", node, tt.want))
			}
		})
	}
}

func Test_parseBytes(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name:    "simple",
			args:    args{s: "[ 65 66 67 ]"},
			want:    []byte("ABC"),
			wantErr: false,
		},
		{
			name:    "malformed with spaces",
			args:    args{s: "  [65  66  67   68] "},
			want:    []byte("ABCD"),
			wantErr: false,
		},
		{
			name:    "error brackets",
			args:    args{s: "[65  66  67   68"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "error values",
			args:    args{s: "[65  66  67   68}]"},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBytes(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseBytes() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parsePgNodeTrees(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want []Node
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parsePgNodeTrees(tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parsePgNodeTrees() = %v, want %v", got, tt.want)
			}
		})
	}
}

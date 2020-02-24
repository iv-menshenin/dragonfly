package utils

import (
	"reflect"
	"testing"
)

func Test_tagToMap(t *testing.T) {
	type args struct {
		tag string
	}
	tests := []struct {
		name string
		args args
		want map[string][]string
	}{
		{
			name: "simple test",
			args: args{tag: "`some: \"one,two,three\"`"},
			want: map[string][]string{
				"some": {"one", "two", "three"},
			},
		},
		{
			name: "with ext spaces",
			args: args{tag: "` some: \"one, two , three\"`"},
			want: map[string][]string{
				"some": {"one", "two", "three"},
			},
		},
		{
			name: "double test",
			args: args{tag: "`some: \"one,two,three\" next:\"-,lit,nonce\"`"},
			want: map[string][]string{
				"some": {"one", "two", "three"},
				"next": {"-", "lit", "nonce"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FieldTagToMap(tt.args.tag); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FieldTagToMap() = %v, want %v", got, tt.want)
			}
		})
	}
}

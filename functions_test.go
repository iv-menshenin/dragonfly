package dragonfly

import (
	"reflect"
	"testing"
)

func Test_sortMap_getSorted(t *testing.T) {
	tests := []struct {
		name       string
		c          sortMap
		wantKeys   []string
		wantValues []int
	}{
		{
			name:       "nil map",
			c:          nil,
			wantKeys:   []string{},
			wantValues: []int{},
		},
		{
			name: "one value map",
			c: map[string]int{
				"ONE": 9,
			},
			wantKeys:   []string{"ONE"},
			wantValues: []int{9},
		},
		{
			name:       "empty map",
			c:          map[string]int{},
			wantKeys:   []string{},
			wantValues: []int{},
		},
		{
			name: "sort map",
			c: map[string]int{
				"ten":     10,
				"three":   3,
				"hundred": 100,
				"zero":    0,
			},
			wantKeys:   []string{"hundred", "ten", "three", "zero"},
			wantValues: []int{100, 10, 3, 0},
		},
		{
			name: "sort map with doubles",
			c: map[string]int{
				"ten":     10,
				"three1":  3,
				"three2":  3,
				"hundred": 100,
				"zero":    0,
			},
			wantKeys:   []string{"hundred", "ten", "three2", "three1", "zero"},
			wantValues: []int{100, 10, 3, 3, 0},
		},
		{
			name: "sort map with doubles much more",
			c: map[string]int{
				"ten":      10,
				"three1":   3,
				"three2":   3,
				"hundred1": 100,
				"hundred2": 100,
				"hundred3": 100,
				"zero":     0,
				"zero1":    0,
			},
			wantKeys:   []string{"hundred3", "hundred2", "hundred1", "ten", "three2", "three1", "zero1", "zero"},
			wantValues: []int{100, 100, 100, 10, 3, 3, 0, 0},
		},
		{
			name: "with same values",
			c: map[string]int{
				"abc": 1,
				"def": 1,
				"pqr": 1,
				"ghi": 1,
				"jkl": 1,
				"mno": 1,
				"vwx": 1,
				"stu": 1,
				"yz0": 1,
			},
			wantKeys:   []string{"yz0", "vwx", "stu", "pqr", "mno", "jkl", "ghi", "def", "abc"},
			wantValues: []int{1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKeys, gotValues := tt.c.getSortedKeysValues()
			if !reflect.DeepEqual(gotKeys, tt.wantKeys) {
				t.Errorf("getSortedKeysValues() gotKeys = %v, want %v", gotKeys, tt.wantKeys)
			}
			if !reflect.DeepEqual(gotValues, tt.wantValues) {
				t.Errorf("getSortedKeysValues() gotValues = %v, want %v", gotValues, tt.wantValues)
			}
		})
	}
}

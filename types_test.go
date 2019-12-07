package dragonfly

import (
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

package dragonfly

import (
	"fmt"
	"io"
	"strings"
)

type (
	sortMap map[string]int
)

func somethingMatched(compaMap map[string]int, matched func(string)) {
	keys, vals := sortMap(compaMap).getSortedKeysValues()
	if (len(keys) == 1 && vals[0] > 0) || (len(keys) > 1 && vals[0] > vals[1]*2) {
		matched(keys[0])
	}
}

// sorting in decreasing order map[string]int by their values. upper values are greater, lower values are less
func (c sortMap) getSortedKeysValues() (keys []string, values []int) {
	keys = make([]string, 0, len(c))
	values = make([]int, 0, len(c))
	for key, value := range c {
		var found = len(values)
		for i, val := range values {
			if val > value {
				continue
			}
			if val == value && keys[i] > key {
				continue
			}
			found = i
			break
		}
		keys = append(keys, "")
		values = append(values, 0)
		copy(keys[found+1:], keys[found:])
		copy(values[found+1:], values[found:])
		keys[found] = key
		values[found] = value
	}
	return
}

func mergeStringMap(a, b map[string]string) map[string]string {
	for key, val := range b {
		a[key] = val
	}
	return a
}

func writer(w io.Writer, format string, i ...interface{}) {
	if _, err := fmt.Fprintf(w, format, i...); err != nil {
		panic(err)
	}
}

func arrayContains(a []string, s string) bool {
	return arrayFind(a, s) > -1
}

func arrayConcat(a, b []string) []string {
	return append(a, b...)
}

func arrayRemove(a []string, s string) []string {
	if i := arrayFind(a, s); i > -1 {
		return arrayConcat(a[:i], a[i+1:])
	}
	return a
}

func arrayFind(a []string, s string) int {
	for i, elem := range a {
		if elem == s {
			return i
		}
	}
	return -1
}

func iArrayContains(a []string, s string) bool {
	return iArrayFind(a, s) > -1
}

func iArrayFind(a []string, s string) int {
	for i, elem := range a {
		if strings.EqualFold(elem, s) {
			return i
		}
	}
	return -1
}

func evalTemplateParameters(template string, parameters map[string]string) string {
	s := template
	for key, val := range parameters {
		s = strings.ReplaceAll(s, "{%"+key+"}", val)
	}
	return s
}

func refBool(b bool) *bool {
	return &b
}

func refString(s string) *string {
	return &s
}

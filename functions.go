package dragonfly

import (
	"fmt"
	"io"
	"strings"
)

type (
	sortMap map[string]int
)

// sorting in decreasing order map[string]int by their values. upper values are greater, lower values are less
func (c sortMap) getSortedKeysValues() (keys []string, values []int) {
	keys = make([]string, len(c), len(c))
	values = make([]int, len(c), len(c))
	for key, value := range c {
		var found = 0
		for i, val := range values {
			found = i
			if val > value {
				continue
			}
			if val == value && keys[i] > key {
				continue
			}
			break
		}
		copy(keys[found+1:], keys[found:])
		copy(values[found+1:], values[found:])
		keys[found] = key
		values[found] = value
	}
	return
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

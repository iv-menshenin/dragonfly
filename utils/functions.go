package utils

import (
	"fmt"
	"io"
	"strings"
)

type (
	SortMap map[string]int
)

func SomethingMatched(compaMap map[string]int, matched func(string)) {
	keys, vals := SortMap(compaMap).GetSortedKeysValues()
	if (len(keys) == 1 && vals[0] > 0) || (len(keys) > 1 && vals[0] > 0 && vals[0] > vals[1]*2) {
		matched(keys[0])
	}
}

// sorting in decreasing order map[string]int by their values. upper values are greater, lower values are less
func (c SortMap) GetSortedKeysValues() (keys []string, values []int) {
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

func MergeStringMap(a, b map[string]string) map[string]string {
	for key, val := range b {
		a[key] = val
	}
	return a
}

func WriteWrapper(w io.Writer, format string, i ...interface{}) {
	if _, err := fmt.Fprintf(w, format, i...); err != nil {
		panic(err)
	}
}

func ArrayContains(a []string, s string) bool {
	return ArrayFind(a, s) > -1
}

func ArrayConcat(a, b []string) []string {
	return append(a, b...)
}

func ArrayRemove(a []string, s string) []string {
	if i := ArrayFind(a, s); i > -1 {
		return ArrayConcat(a[:i], a[i+1:])
	}
	return a
}

func ArrayFind(a []string, s string) int {
	for i, elem := range a {
		if elem == s {
			return i
		}
	}
	return -1
}

func ArrayContainsCI(a []string, s string) bool {
	return ArrayFindCI(a, s) > -1
}

func ArrayFindCI(a []string, s string) int {
	for i, elem := range a {
		if strings.EqualFold(elem, s) {
			return i
		}
	}
	return -1
}

func EvalTemplateParameters(template string, parameters map[string]string) string {
	s := template
	for key, val := range parameters {
		s = strings.ReplaceAll(s, "{%"+key+"}", val)
	}
	return s
}

func RefBool(b bool) *bool {
	return &b
}

func RefString(s string) *string {
	return &s
}

func nonEmptyStrings(a ...interface{}) []string {
	result := make([]string, 0, len(a))
	for _, i := range a {
		var s string
		if i != nil {
			switch value := i.(type) {
			case *string:
				if value != nil {
					s = *value
				}
			case string:
				s = value
			case int, int16, int32, int64:
				s = fmt.Sprintf("%d", value)
			case float32, float64:
				s = fmt.Sprintf("%f", value)
			case bool:
				if value {
					s = "true"
				} else {
					s = "false"
				}
			case *int:
				if value != nil {
					s = fmt.Sprintf("%d", *value)
				}
			case *int16:
				if value != nil {
					s = fmt.Sprintf("%d", *value)
				}
			case *int32:
				if value != nil {
					s = fmt.Sprintf("%d", *value)
				}
			case *int64:
				if value != nil {
					s = fmt.Sprintf("%d", *value)
				}
			case *float32:
				if value != nil {
					s = fmt.Sprintf("%f", *value)
				}
			case *float64:
				if value != nil {
					s = fmt.Sprintf("%f", *value)
				}
			case *bool:
				if value != nil {
					if *value {
						s = "true"
					} else {
						s = "false"
					}
				}
			default:
				if sI, ok := value.(fmt.Stringer); ok {
					s = sI.String()
				} else if sI, ok := value.(*fmt.Stringer); ok {
					s = (*sI).String()
				} else {
					s = fmt.Sprintf("%v", value)
				}
			}
			if strings.TrimSpace(s) != "" {
				result = append(result, s)
			}
		}
	}
	return result
}

func NonEmptyStringsConcatSpaceSeparated(a ...interface{}) string {
	return strings.Join(nonEmptyStrings(a...), " ")
}

func NonEmptyStringsConcatCommaSeparated(a ...interface{}) string {
	return strings.Join(nonEmptyStrings(a...), ", ")
}

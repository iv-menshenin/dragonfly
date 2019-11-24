package main

import (
	"fmt"
	"strconv"
	"strings"
)

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

func evalRepeaterParameters(template string) string {
	for {
		// TODO: infinity loop?
		s := repeaterPattern.FindAllStringSubmatch(template, -1)
		if len(s) > 0 {
			count, err := strconv.Atoi(s[0][1])
			if err != nil {
				panic(err)
			}
			var (
				pattern   = s[0][2]
				inBracket = 0
			)
			for i := 0; i < len(pattern); i++ {
				if pattern[i] == '}' {
					if inBracket > 0 {
						inBracket--
					} else {
						pattern = pattern[:i]
						break
					}
				} else if pattern[i] == '{' {
					inBracket++
				}
			}
			template = strings.ReplaceAll(template, fmt.Sprintf("{%s*%s}", s[0][1], pattern), strings.Repeat(pattern, count))
		} else {
			break
		}
	}
	return template
}

func evalTemplateParameters(template string, parameters map[string]string) string {
	s := template
	for key, val := range parameters {
		s = strings.ReplaceAll(s, "{%"+key+"}", val)
	}
	return evalRepeaterParameters(s)
}

package utils

import (
	"regexp"
	"strings"
)

func StringToRef(s string) *string {
	if s == "" {
		return nil
	} else {
		return &s
	}
}

var (
	tagsPattern   = regexp.MustCompile("^`((\\s*[a-z]+\\s*:\\s*\"[^\"]*\"\\s*)*)`$")
	tagsExtractor = regexp.MustCompile("\\s*[a-z]+\\s*:\\s*\"[^\"]*\"\\s*")
	tagPattern    = regexp.MustCompile("\\s*([a-z]+)\\s*:\\s*\"([^\"]*)\"\\s*")
)

func FieldTagToMap(tag string) (result map[string][]string) {
	result = make(map[string][]string, 10)
	sub := tagsPattern.FindAllStringSubmatch(tag, -1)
	if len(sub) > 0 {
		tagsUnquoted := sub[0][1]
		extracted := tagsExtractor.FindAllString(tagsUnquoted, -1)
		for _, tagChain := range extracted {
			tagSmt := tagPattern.FindAllStringSubmatch(tagChain, -1)
			list := strings.Split(tagSmt[0][2], ",")
			for i, current := range list {
				list[i] = strings.TrimSpace(current)
			}
			result[tagSmt[0][1]] = list
		}
	}
	return
}

package dragonfly

import (
	"fmt"
	"strings"
)

func defaultToSQL(d interface{}) *string {
	if d == nil {
		return nil
	}
	var result = "null"
	switch val := d.(type) {
	case string:
		result = fmt.Sprintf("'%s'", strings.Replace(val, "'", "''", -1))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		result = fmt.Sprintf("%d", d)
	case float32, float64:
		result = fmt.Sprintf("%f", d)
	case bool:
		if val {
			result = "true"
		} else {
			result = "false"
		}
	default:
		panic(fmt.Sprintf("cannot resolve default value '%+v' <%T>", d, d))
	}
	if result == "null" {
		return nil
	} else {
		return &result
	}
}

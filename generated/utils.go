package generated

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"strings"
)

type (
	ContextDB      struct{}
	EnumValueError error
)

var (
	sqlEmptyResult     = errors.New("got empty result")
	SingletonViolation = errors.New("singleton violation")
	UUID               []byte
)

func makeTypeValueError(typeName, gotValue string) EnumValueError {
	return errors.New(fmt.Sprintf("invalid %s value: %s", typeName, gotValue))
}

func IsEmptyResult(err error) bool {
	return err == sqlEmptyResult
}

func EncryptField(input, salt string) string {
	return input + salt
}

func EqualEncryptedField(input, salt, needed string) bool {
	return EncryptField(input, salt) == needed
}

func getDatabase(ctx context.Context) (*sql.DB, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if db, ok := ctx.Value(ContextDB{}).(*sql.DB); ok {
		return db, nil
	}
	return nil, errors.New("cannot get connection from context")
}

type (
	SqlStringArray   []string
	SqlIntegerArray  []int64
	SqlUnsignedArray []uint64
	SqlFloatArray    []float64
)

func (c *SqlStringArray) Scan(i interface{}) error {
	if i == nil {
		return nil
	}
	if u, ok := i.([]uint8); ok {
		u[0] = '['
		u[len(u)-1] = ']'
		return yaml.Unmarshal(u, c)
	} else {
		return errors.New("unexpected data")
	}
}

func (c SqlStringArray) Value() (driver.Value, error) {
	if len(c) == 0 {
		return nil, nil
	}
	var serialized = make([]string, 0, len(c))
	for _, v := range c {
		serialized = append(serialized, fmt.Sprintf("'%s'", strings.Replace(v, "'", "''", -1)))
	}
	return fmt.Sprintf("{%s}", strings.Join(serialized, ",")), nil
}

func (c *SqlIntegerArray) Scan(i interface{}) error {
	if i == nil {
		return nil
	}
	if u, ok := i.([]uint8); ok {
		u[0] = '['
		u[len(u)-1] = ']'
		return yaml.Unmarshal(u, c)
	} else {
		return errors.New("unexpected data")
	}
}

func (c SqlIntegerArray) Value() (driver.Value, error) {
	if len(c) == 0 {
		return nil, nil
	}
	var serialized = make([]string, 0, len(c))
	for _, v := range c {
		serialized = append(serialized, fmt.Sprintf("%d", v))
	}
	return fmt.Sprintf("{%s}", strings.Join(serialized, ",")), nil
}

func (c *SqlUnsignedArray) Scan(i interface{}) error {
	if i == nil {
		return nil
	}
	if u, ok := i.([]uint8); ok {
		u[0] = '['
		u[len(u)-1] = ']'
		return yaml.Unmarshal(u, c)
	} else {
		return errors.New("unexpected data")
	}
}

func (c SqlUnsignedArray) Value() (driver.Value, error) {
	if len(c) == 0 {
		return nil, nil
	}
	var serialized = make([]string, 0, len(c))
	for _, v := range c {
		serialized = append(serialized, fmt.Sprintf("%d", v))
	}
	return fmt.Sprintf("{%s}", strings.Join(serialized, ",")), nil
}

func (c *SqlFloatArray) Scan(i interface{}) error {
	if i == nil {
		return nil
	}
	if u, ok := i.([]uint8); ok {
		u[0] = '['
		u[len(u)-1] = ']'
		return yaml.Unmarshal(u, c)
	} else {
		return errors.New("unexpected data")
	}
}

func (c SqlFloatArray) Value() (driver.Value, error) {
	if len(c) == 0 {
		return nil, nil
	}
	var serialized = make([]string, 0, len(c))
	for _, v := range c {
		serialized = append(serialized, fmt.Sprintf("%f", v))
	}
	return fmt.Sprintf("{%s}", strings.Join(serialized, ",")), nil
}

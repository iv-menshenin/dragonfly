package generated

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
)

type (
	ContextDB      struct{}
	EnumValueError error
)

var (
	EmptyResult        = errors.New("got empty result")
	SingletonViolation = errors.New("singleton violation")
)

func makeTypeValueError(typeName, gotValue string) EnumValueError {
	return errors.New(fmt.Sprintf("invalid %s value: %s", typeName, gotValue))
}

func IsEmptyResult(err error) bool {
	return err == EmptyResult
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
	if u, ok := i.([]uint8); ok {
		u[0] = '['
		u[len(u)-1] = ']'
		return yaml.Unmarshal(u, c)
	} else {
		return errors.New("unexpected data")
	}
}

func (c *SqlIntegerArray) Scan(i interface{}) error {
	if u, ok := i.([]uint8); ok {
		u[0] = '['
		u[len(u)-1] = ']'
		return yaml.Unmarshal(u, c)
	} else {
		return errors.New("unexpected data")
	}
}

func (c *SqlUnsignedArray) Scan(i interface{}) error {
	if u, ok := i.([]uint8); ok {
		u[0] = '['
		u[len(u)-1] = ']'
		return yaml.Unmarshal(u, c)
	} else {
		return errors.New("unexpected data")
	}
}

func (c *SqlFloatArray) Scan(i interface{}) error {
	if u, ok := i.([]uint8); ok {
		u[0] = '['
		u[len(u)-1] = ']'
		return yaml.Unmarshal(u, c)
	} else {
		return errors.New("unexpected data")
	}
}

package generated

import (
	"context"
	"database/sql"
	"errors"
)

type (
	ContextDB struct{}
)

var (
	EmptyResult = errors.New("got empty result")
)

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

package builders

import "go/ast"

var (
	registeredGenerators = map[string]CallFunctionDescriber{
		"now": TimeNowFn,
	}
)

func AddNewGenerator(name string, descr CallFunctionDescriber) {
	registeredGenerators[name] = descr
}

func RegisterSqlFieldEncryptFunction(encryptFn func(valueForEncrypt ast.Expr) *ast.CallExpr) {
	if makeEncryptPasswordCallCustom == nil {
		makeEncryptPasswordCallCustom = encryptFn
	} else {
		panic("custom function already registered")
	}
}

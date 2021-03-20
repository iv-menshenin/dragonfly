package dragonfly

import (
	"database/sql"
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	builders "github.com/iv-menshenin/go-ast"
	sqt "github.com/iv-menshenin/sql-ast"
	"go/ast"
	"go/printer"
	"go/token"
	"io"
	"net/url"
	"os"
	"strings"
)

func databaseWork(
	options ConnectionOptions,
	fn func(*sql.DB) error,
) error {
	dbConnectionString := options.ConnStr
	if dbConnectionString == "" {
		dbConnectionString = fmt.Sprintf(
			"%s://%s:%s@%s/%s",
			options.Driver,
			url.QueryEscape(options.UserName),
			url.QueryEscape(options.Password),
			options.Host,
			options.Database,
		)
	}
	if db, err := sql.Open(options.Driver, dbConnectionString); err != nil {
		return err
	} else {
		defer func() {
			if err := db.Close(); err != nil {
				if _, err := fmt.Fprintf(os.Stderr, "error on closing DB connection: %s", err.Error()); err != nil {
					panic(err)
				}
			}
		}()
		return fn(db)
	}
}

type (
	Diff struct {
		preInstall   []sqt.SqlStmt
		install      []sqt.SqlStmt
		afterInstall []sqt.SqlStmt
	}
)

func ResolveDependencies(d *Diff) {
	fixTheOrderOf(d.preInstall)
	fixTheOrderOf(d.install)
	fixTheOrderOf(d.afterInstall)
}

func MakeDatabaseDump(options ConnectionOptions) (dump Root, err error) {
	err = databaseWork(options, func(db *sql.DB) (e error) {
		dump, e = getAllDatabaseInformation(db, options.Database)
		return
	})
	return
}

func MakeDiff(current, new *Root) Diff {
	var (
		result = Diff{
			preInstall:   make([]sqt.SqlStmt, 0, 0),
			install:      make([]sqt.SqlStmt, 0, 0),
			afterInstall: make([]sqt.SqlStmt, 0, 0),
		}
		postponedSchemaObjects = make(map[string]postponedObjects, 0)
	)
	for _, schema := range new.Schemas {
		// process all
		pre, ins, after, postponed := schema.diffKnown(current, schema.Value.Name, new)
		postponedSchemaObjects[schema.Value.Name] = postponed
		// save needed
		result.preInstall = append(result.preInstall, pre...)
		result.install = append(result.install, ins...)
		result.afterInstall = append(result.afterInstall, after...)
	}
	for _, schema := range new.Schemas {
		postponedSchema, ok := postponedSchemaObjects[schema.Value.Name]
		if !ok {
			continue
		}
		pre, ins, after := schema.diffPostponed(postponedSchema, current, schema.Value.Name, new)
		// save needed
		result.preInstall = append(result.preInstall, pre...)
		result.install = append(result.install, ins...)
		result.afterInstall = append(result.afterInstall, after...)
	}
	for _, schema := range new.Schemas {
		pre, ins, after := schema.prepareDeleting(current, schema.Value.Name, new)
		// save needed
		result.preInstall = append(result.preInstall, pre...)
		result.install = append(result.install, ins...)
		result.afterInstall = append(result.afterInstall, after...)
	}
	return result
}

func MakeDataSQL(file *Root) []sqt.SqlStmt {
	var result = make([]sqt.SqlStmt, 0)
	for _, schema := range file.Schemas {
		for _, data := range schema.Value.Data {
			table, ok := schema.Value.Tables.tryToFind(data.Name)
			if !ok {
				panic(fmt.Sprintf("cannot find table `%s` in `%s`", data.Name, schema.Value.Name))
			}
			keys := table.extractPrimaryKeyColumns()
			if len(keys) == 0 {
				keys = table.extractUniqueKeyColumns()
			}
			var cause = make([]sqt.SqlExpr, 0)
			for _, k := range keys {
				cause = append(cause, &sqt.Literal{Text: k.Value.Name})
			}
			for _, row := range data.Data {
				var (
					onConflict = make([]sqt.SqlExpr, 0, len(row))
					setExps    = make(map[string]sqt.SqlExpr, len(row))
				)
				for f, i := range row {
					column, ok := table.Columns.tryToFind(f)
					if !ok {
						panic(fmt.Sprintf("cannot find column `%s` in `%s.%s`", f, data.Name, schema.Value.Name))
					}
					var value string
					if fm, ok := formatTypes[column.Value.Schema.Value.TypeBase.Type]; ok && fm == "%s" {
						value = fmt.Sprintf("'%v'", i)
					} else {
						value = fmt.Sprintf("%v", i)
					}
					setExps[f] = &sqt.Literal{Text: value}
					var found = false
					for _, k := range keys {
						if strings.EqualFold(k.Value.Name, f) {
							found = true
							break
						}
					}
					if found {
						continue
					}
					onConflict = append(onConflict, &sqt.BinaryExpr{
						Left:  &sqt.Literal{Text: f},
						Right: &sqt.Literal{Text: value},
						Op:    token.ASSIGN,
					})
				}
				result = append(result, &sqt.InsertStmt{
					Table: sqt.TableDesc{
						Table: &sqt.Selector{
							Name:      data.Name,
							Container: schema.Value.Name,
						},
					},
					Insert: setExps,
					OnConflict: &sqt.OnConflict{
						Cause: &sqt.BracketBlock{Expr: cause},
						Set:   onConflict,
					},
				})
			}
		}
	}
	return result
}

func MakeEmptyRoot() Root {
	return Root{
		Schemas: []SchemaRef{
			{
				Value: Schema{
					Name:    "public",
					Types:   nil,
					Domains: nil,
					Tables:  nil,
				},
				Ref: nil,
			},
		},
	}
}

func GenerateGO(db *Root, schemaName, packageName string, w io.Writer) {
	// we must allow to use type `schema.domain` as known type
	for _, schema := range db.Schemas {
		for domainName, domain := range schema.Value.Domains {
			if domainType, ok := knownTypes[domain.Type]; ok {
				knownTypes[fmt.Sprintf("%s.%s", schema.Value.Name, domainName)] = domainType
			}
		}
	}
	var astData AstData
	for _, schema := range db.Schemas {
		if schemaName == "" || schemaName == schema.Value.Name {
			schema.generateGO(schema.Value.Name, &astData)
		}
	}
	file, fset := astData.makeAstFile(packageName)
	filePrinter := printer.Config{
		Mode:     printer.UseSpaces | printer.TabIndent,
		Tabwidth: 8,
	}
	if err := filePrinter.Fprint(w, fset, file); err != nil {
		panic(err)
	}
}

func RegisterApiBuilder(typeName string, operation ApiDbOperation, builderFunc ApiFuncBuilder) {
	apiTypeIsOperation[ApiType(typeName)] = operation
	funcTemplates[ApiType(typeName)] = builderFunc
}

func RegisterFieldValueGenerator(alias, funcName string, minimumArgumentsCount int, isExtensible bool) {
	newFunction := builders.CallFunctionDescriber{
		FunctionName:                ast.NewIdent(funcName),
		MinimumNumberOfArguments:    minimumArgumentsCount,
		ExtensibleNumberOfArguments: isExtensible,
		MultipleReturnValues:        false,
	}
	addNewGenerator(alias, newFunction)
}

func RegisterFieldValueGeneratorWithChecking(alias, funcName string, minimumArgumentsCount int, isExtensible bool) {
	newFunction := builders.CallFunctionDescriber{
		FunctionName:                ast.NewIdent(funcName),
		MinimumNumberOfArguments:    minimumArgumentsCount,
		ExtensibleNumberOfArguments: isExtensible,
		MultipleReturnValues:        true,
	}
	addNewGenerator(alias, newFunction)
}

func RegisterSqlFieldEncryptFunction(encryptFn func(valueForEncrypt ast.Expr) *ast.CallExpr) {
	if makeEncryptPasswordCallCustom == nil {
		makeEncryptPasswordCallCustom = encryptFn
	} else {
		panic("custom function already registered")
	}
}

func (c *Diff) Print(w io.Writer) {
	utils.WriteWrapper(w, "\n/* SECTION BEFORE INSTALL %s */", strings.Repeat("=", 58))
	for _, stmt := range c.preInstall {
		utils.WriteWrapper(w, "\n%s;\n", stmt)
	}
	utils.WriteWrapper(w, "\n/* SECTION INSTALL %s */", strings.Repeat("=", 58))
	for _, stmt := range c.install {
		utils.WriteWrapper(w, "\n%s;\n", stmt)
	}
	utils.WriteWrapper(w, "\n/* SECTION AFTER INSTALL %s */", strings.Repeat("=", 52))
	for _, stmt := range c.afterInstall {
		utils.WriteWrapper(w, "\n%s;\n", stmt)
	}
	utils.WriteWrapper(w, "\n/* END OF UPDATE SCRIPT %s */", strings.Repeat("=", 53))
}

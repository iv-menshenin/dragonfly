package dragonfly

import (
	"database/sql"
	"fmt"
	"github.com/iv-menshenin/dragonfly/utils"
	"io"
	"net/url"
	"os"
	"strings"
)

func databaseWork(
	options ConnectionOptions,
	fn func(*sql.DB) error,
) error {
	dbConnectionString := fmt.Sprintf(
		"%s://%s:%s@%s/%s",
		options.Driver,
		url.QueryEscape(options.UserName),
		url.QueryEscape(options.Password),
		options.Host,
		options.Database,
	)
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
		preInstall   []SqlStmt
		install      []SqlStmt
		afterInstall []SqlStmt
	}
)

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
			preInstall:   make([]SqlStmt, 0, 0),
			install:      make([]SqlStmt, 0, 0),
			afterInstall: make([]SqlStmt, 0, 0),
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

func RegisterApiBuilder(typeName string, builderFunc ApiFuncBuilder) {
	funcTemplates[typeName] = builderFunc
}

func (c *Diff) Print(w io.Writer) {
	utils.WriteWrapper(w, "\n/* SECTION BEFORE INSTALL %s */", strings.Repeat("=", 58))
	for _, stmt := range c.preInstall {
		utils.WriteWrapper(w, "\n/* statement: %s */\n%s;\n", stmt.GetComment(), stmt.MakeStmt())
	}
	utils.WriteWrapper(w, "\n/* SECTION INSTALL %s */", strings.Repeat("=", 58))
	for _, stmt := range c.install {
		utils.WriteWrapper(w, "\n/* statement: %s */\n%s;\n", stmt.GetComment(), stmt.MakeStmt())
	}
	utils.WriteWrapper(w, "\n/* SECTION AFTER INSTALL %s */", strings.Repeat("=", 52))
	for _, stmt := range c.afterInstall {
		utils.WriteWrapper(w, "\n/* statement: %s */\n%s;\n", stmt.GetComment(), stmt.MakeStmt())
	}
	utils.WriteWrapper(w, "\n/* END OF UPDATE SCRIPT %s */", strings.Repeat("=", 53))
}

package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/iv-menshenin/dragonfly"
	"io"
	"os"
	"strings"
)

type (
	ToDo          string
	ProgramParams struct {
		ToDo         ToDo
		InputFile    *string
		OutputFile   *string
		OutputFormat *string
		Schema       *string
		PackageName  *string
		Connection   *string
		ShowHelp     *bool
	}
)

const (
	ToDoValidate ToDo = "validate"
	ToDoGenerate ToDo = "generate"
	ToDoDiff     ToDo = "diff"
	ToDoReverse  ToDo = "reverse"
	ToDoHelp     ToDo = "help"
)

func printWithoutOperationError() {
	raise(errors.New("you must select one of the valid operations: validate, generate or help"))
}

func raise(err error, args ...interface{}) {
	if len(args) > 0 {
		if argsFmt, ok := args[0].(string); ok {
			args = append(args, err.Error())
			if _, newError := fmt.Fprint(os.Stderr, fmt.Sprintf(argsFmt, args[1:]...)); newError != nil {
				panic(newError)
			}
		}
	} else {
		if _, newError := fmt.Fprint(os.Stderr, err.Error()); newError != nil {
			panic(newError)
		}
	}
	os.Exit(1)
}

func initFlags() ProgramParams {
	if len(os.Args) < 2 {
		printWithoutOperationError()
	}
	var (
		parameters = make(map[ToDo]ProgramParams, 3)
		flagSets   = make(map[ToDo]*flag.FlagSet, 3)
	)

	fsGenerate := flag.NewFlagSet(string(ToDoGenerate), flag.PanicOnError)
	parameters[ToDoGenerate] = ProgramParams{
		ToDo:         ToDoGenerate,
		InputFile:    fsGenerate.String("input", os.Stdin.Name(), "file to input"),
		OutputFile:   fsGenerate.String("output", os.Stdout.Name(), "file to output"),
		OutputFormat: fsGenerate.String("format", "sql", "go or sql"),
		PackageName:  fsGenerate.String("package", "generated", "go package name"),
		Schema:       fsGenerate.String("schema", "", "generate code for schema"),
		ShowHelp:     fsGenerate.Bool("help", false, "show this page"),
	}
	flagSets[ToDoGenerate] = fsGenerate

	fsValidate := flag.NewFlagSet(string(ToDoValidate), flag.PanicOnError)
	parameters[ToDoValidate] = ProgramParams{
		ToDo:      ToDoValidate,
		InputFile: fsValidate.String("input", os.Stdin.Name(), "file to input"),
		ShowHelp:  fsValidate.Bool("help", false, "show this page"),
	}
	flagSets[ToDoValidate] = fsValidate

	fsDiff := flag.NewFlagSet(string(ToDoDiff), flag.PanicOnError)
	parameters[ToDoDiff] = ProgramParams{
		ToDo:        ToDoDiff,
		InputFile:   fsDiff.String("input", os.Stdin.Name(), "file to input"),
		OutputFile:  fsDiff.String("output", os.Stdout.Name(), "file to output"),
		PackageName: fsDiff.String("package", "generated", "go package name"),
		Schema:      fsDiff.String("schema", "", "generate code for schema"),
		Connection:  fsDiff.String("connection", "", "connection string"),
	}
	flagSets[ToDoDiff] = fsDiff

	fsReverse := flag.NewFlagSet(string(ToDoReverse), flag.PanicOnError)
	parameters[ToDoReverse] = ProgramParams{
		ToDo:       ToDoReverse,
		OutputFile: fsReverse.String("output", os.Stdout.Name(), "file to output"),
		Connection: fsReverse.String("connection", os.Stdout.Name(), "connection string"),
	}
	flagSets[ToDoReverse] = fsReverse

	fsHelp := flag.NewFlagSet(string(ToDoHelp), flag.PanicOnError)
	parameters[ToDoHelp] = ProgramParams{
		ToDo: ToDoHelp,
	}
	flagSets[ToDoHelp] = fsHelp

	if flagSet, ok := flagSets[ToDo(os.Args[1])]; ok {
		if err := flagSet.Parse(os.Args[2:]); err != nil {
			raise(err)
		}
	} else {
		printWithoutOperationError()
	}
	return parameters[ToDo(os.Args[1])]
}

func openFileForWrite(fileName string, onOpened func(w io.Writer) error) error {
	var (
		f   *os.File
		err error
	)
	if fileName == os.Stdout.Name() {
		f = os.Stdout
	} else {
		if f, err = os.Create(fileName); err != nil {
			return err
		}
		defer func() {
			if err := f.Close(); err != nil {
				raise(err)
			}
		}()
	}
	return onOpened(f)
}

func main() {
	var root *dragonfly.Root
	state := initFlags()
	readAndParse := func() {
		root = dragonfly.ReadDatabaseProjectFile(*state.InputFile)
	}
	switch state.ToDo {
	case ToDoGenerate:

		err := openFileForWrite(*state.OutputFile, func(w io.Writer) error {
			readAndParse()
			switch strings.ToLower(*state.OutputFormat) {
			case "sql":
				var dump = dragonfly.MakeEmptyRoot()
				diff := dragonfly.MakeDiff(&dump, root)
				dragonfly.ResolveDependencies(&diff)
				diff.Print(w)
			case "go":
				dragonfly.GenerateGO(root, *state.Schema, *state.PackageName, w)
			}
			return nil
		})
		if err != nil {
			raise(err)
		}
	case ToDoValidate:
		readAndParse()
	case ToDoDiff:
		err := openFileForWrite(*state.OutputFile, func(w io.Writer) error {
			readAndParse()
			var (
				dump dragonfly.Root
				e    error
			)
			if dump, e = dragonfly.MakeDatabaseDump(dragonfly.ConnectionOptions{
				Driver:   "postgres",
				UserName: "postgres",
				Password: os.Getenv("DB_PASSWORD"),
				Host:     os.Getenv("DB_HOST"),
				Database: os.Getenv("DB_NAME"),
				ConnStr:  *state.Connection,
			}); e != nil {
				return e
			}
			diff := dragonfly.MakeDiff(&dump, root)
			diff.Print(w)
			return nil
		})
		if err != nil {
			raise(err)
		}
	case ToDoReverse:
		if err := openFileForWrite(*state.OutputFile, func(w io.Writer) error {
			var (
				dump dragonfly.Root
				data []byte
				e    error
			)
			if dump, e = dragonfly.MakeDatabaseDump(dragonfly.ConnectionOptions{
				Driver:   "postgres",
				UserName: "postgres",
				Password: os.Getenv("DB_PASSWORD"),
				Host:     os.Getenv("DB_HOST"),
				Database: os.Getenv("DB_NAME"),
				ConnStr:  *state.Connection,
			}); e != nil {
				return e
			}
			if data, e = yaml.Marshal(&dump); e != nil {
				return e
			}
			if _, e = w.Write(data); e != nil {
				return e
			}
			return nil
		}); err != nil {
			raise(err)
		}
	case ToDoHelp:
	}
}

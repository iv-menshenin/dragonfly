package main

import (
	"errors"
	"flag"
	"fmt"
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
		ShowHelp     *bool
	}
	arrayStringFinder []string
)

const (
	ToDoValidate ToDo = "validate"
	ToDoGenerate ToDo = "generate"
	ToDoHelp     ToDo = "help"
)

func (a arrayStringFinder) exists(s string) bool {
	for _, e := range a {
		if e == s {
			return true
		}
	}
	return false
}

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
				dragonfly.GenerateSql(root, *state.Schema, w)
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
	case ToDoHelp:
	}
}

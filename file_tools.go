package main

import (
	"encoding/json"
	"gopkg.in/yaml.v2"
	"io"
	"os"
	"path"
	"strings"
)

func jsonDecoder(r io.Reader) func(v interface{}) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	return decoder.Decode
}

func yamlDecoder(r io.Reader) func(v interface{}) error {
	decoder := yaml.NewDecoder(r)
	decoder.SetStrict(true)
	return decoder.Decode
}

func readAndParseFile(fileName string, i interface{}) {
	var (
		decoder func(r io.Reader) func(v interface{}) error
	)
	if file, err := os.Open(fileName); err != nil {
		raise(err)
	} else {
		switch strings.ToLower(path.Ext(fileName)) {
		case ".json":
			decoder = jsonDecoder
		case ".yaml", ".yml":
			decoder = yamlDecoder
		default:
			var b = make([]byte, 1, 1)
			if _, err := file.Read(b); err != nil {
				raise(err)
			} else {
				if _, err := file.Seek(0, 0); err != nil {
					raise(err)
				}
			}
			if b[0] == '{' {
				decoder = jsonDecoder
			} else {
				decoder = yamlDecoder
			}
		}
		if err := decoder(file)(i); err != nil {
			raise(err, "%s\nOn parsing: "+fileName)
		}
	}
}

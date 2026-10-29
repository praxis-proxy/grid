// Render hub templates with API fixtures without contacting a cluster.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"text/template"
)

func main() {
	var input struct {
		Template  string
		Cluster   string
		Resources map[string]any
		Managed   bool
	}
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		panic(err)
	}
	funcs := template.FuncMap{
		"lookup": func(api, kind, ns, name string) any {
			value, ok := input.Resources[api+"/"+kind+"/"+ns+"/"+name]
			if !ok {
				return map[string]any{}
			}
			return value
		},
		"fromSecret": func(ns, name, key string) string { return ns + "/" + name + "/" + key },
		"fail":       func(message string) (string, error) { return "", fmt.Errorf("%s", message) },
		"list":       func(values ...any) []any { return values },
	}
	left, right := "{{hub", "hub}}"
	if input.Managed {
		left, right = "{{", "}}"
	}
	t, err := template.New("hub").Delims(left, right).Funcs(funcs).Parse(input.Template)
	if err != nil {
		panic(err)
	}
	var output bytes.Buffer
	if err := t.Execute(&output, map[string]string{"ManagedClusterName": input.Cluster}); err != nil {
		panic(err)
	}
	fmt.Print(output.String())
}

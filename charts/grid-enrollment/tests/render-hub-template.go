// Render hub templates with API fixtures without contacting a cluster.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
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
			if value, ok := input.Resources[api+"/"+kind+"/"+ns+"/"+name]; ok {
				return value
			}
			if name != "" {
				return map[string]any{}
			}
			// An empty name lists every fixture of the kind, in every namespace when ns is empty.
			prefix := api + "/" + kind + "/"
			keys := make([]string, 0, len(input.Resources))
			for key := range input.Resources {
				if rest, ok := strings.CutPrefix(key, prefix); ok && (ns == "" || strings.HasPrefix(rest, ns+"/")) {
					keys = append(keys, key)
				}
			}
			sort.Strings(keys)
			items := make([]any, 0, len(keys))
			for _, key := range keys {
				items = append(items, input.Resources[key])
			}
			return map[string]any{"items": items}
		},
		"fromSecret": func(ns, name, key string) string { return ns + "/" + name + "/" + key },
		"fail":       func(message string) (string, error) { return "", fmt.Errorf("%s", message) },
		"list":       func(values ...any) []any { return values },
		// Sprig's default for the scalar values these templates pass.
		"default": func(fallback any, given ...any) any {
			if len(given) == 0 || given[0] == nil || given[0] == "" {
				return fallback
			}
			return given[0]
		},
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

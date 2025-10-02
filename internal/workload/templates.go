package workload

import (
	"embed"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

//go:embed templates/*.json
var templateFS embed.FS

var builtInTemplateNames []string
var builtInTemplates = map[string][]byte{}

func init() {
	entries, err := templateFS.ReadDir("templates")
	if err != nil {
		panic(fmt.Sprintf("load workload templates: %v", err))
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		data, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			panic(fmt.Sprintf("load template %s: %v", name, err))
		}
		key := strings.TrimSuffix(name, filepath.Ext(name))
		builtInTemplates[key] = data
		builtInTemplateNames = append(builtInTemplateNames, key)
	}
	sort.Strings(builtInTemplateNames)
}

// BuiltInTemplates returns the list of built-in workload template names.
func BuiltInTemplates() []string {
	out := make([]string, len(builtInTemplateNames))
	copy(out, builtInTemplateNames)
	return out
}

// LoadTemplate returns the JSON payload for the named built-in template.
func LoadTemplate(name string) ([]byte, error) {
	data, ok := builtInTemplates[name]
	if !ok {
		return nil, fmt.Errorf("unknown template %q", name)
	}
	copyData := make([]byte, len(data))
	copy(copyData, data)
	return copyData, nil
}

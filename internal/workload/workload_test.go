package workload

import "testing"

func TestBuiltInTemplates(t *testing.T) {
	names := BuiltInTemplates()
	if len(names) == 0 {
		t.Fatalf("expected built-in templates")
	}
	for _, name := range names {
		data, err := LoadTemplate(name)
		if err != nil {
			t.Fatalf("LoadTemplate(%s) error: %v", name, err)
		}
		selector, err := FromJSON(data)
		if err != nil {
			t.Fatalf("FromJSON(%s) error: %v", name, err)
		}
		if selector == nil {
			t.Fatalf("selector nil for template %s", name)
		}
	}
}

func TestFromJSONValidation(t *testing.T) {
	json := []byte(`{"operations": [{"operation": "put", "weight": 0}]}`)
	selector, err := FromJSON(json)
	if err != nil {
		t.Fatalf("FromJSON returned error: %v", err)
	}
	if selector.Pick().Operation != "" {
		t.Fatalf("expected no operations for zero-weight template")
	}
}

package streamerapi

import (
	"testing"
)

func compareSlices(a, b []string) bool {
	// If one is nil, the other must also be nil.
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func TestFlattenPassthrough(t *testing.T) {
	var tests = []struct {
		input    map[string]string
		expected string
	}{
		{map[string]string{"firstProperty": "1", "secondProperty": "2", "thirdProperty": "3"}, `["firstProperty":"1","secondProperty":"2","thirdProperty":"3"]`},
	}
	for _, test := range tests {
		flatInput := flattenPassthrough(test.input)
		if !(flatInput == test.expected) {
			t.Errorf("Expecting %s got %s", test.expected, flatInput)
		}

	}
}

package streamer

import (
	"fmt"
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
func TestRenameDuplicateColumns(t *testing.T) {
	var tests = []struct {
		input    []string
		expected []string
	}{
		{[]string{"header1", "header2", "header3", "header4"}, []string{"header1", "header2", "header3", "header4"}},
		{[]string{"header1", "header1", "header1", "header1"}, []string{"header1", "header1_1", "header1_2", "header1_3"}},
	}
	for _, test := range tests {
		input := RenameDuplicateColumns(test.input)
		if !compareSlices(input, test.expected) {
			fmt.Printf()
			t.Errorf("Expecting %s got %s", test.expected, input)
		}

	}
}

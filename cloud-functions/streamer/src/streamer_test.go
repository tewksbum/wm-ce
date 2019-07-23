package streamer

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
			t.Errorf("Expecting %s got %s", test.expected, input)
		}

	}
}
func TestUniqueElements(t *testing.T) {
	var tests = []struct {
		input    []string
		expected []string
	}{
		{[]string{"header1", "header2", "header3", "header4"}, []string{"header1", "header2", "header3", "header4"}},
		{[]string{"header1", "header1", "header1", "header1"}, []string{"header1"}},
	}
	for _, test := range tests {
		input := uniqueElements(test.input)
		if !compareSlices(input, test.expected) {
			t.Errorf("Expecting %s got %s", test.expected, input)
		}

	}
}
func TestContains(t *testing.T) {
	var tests = []struct {
		input    string
		slice    []string
		expected bool
	}{
		{"header1", []string{"header1", "header2", "header3", "header4"}, true},
		{"header2", []string{"header1", "header1_1", "header1_2", "header1_3"}, false},
	}
	for _, test := range tests {
		if !(contains(test.slice, test.input) == test.expected) {
			if test.expected {
				t.Errorf("%s contains %s but its returning false", test.slice, test.input)
			} else {
				t.Errorf("%s doesn't contain %s but its returning true", test.slice, test.input)
			}
		}

	}
}

func TestGetCsvMap(t *testing.T) {
	var tests = []struct {
		input             [][]string
		expectedNameSlice []string
		expectedYearSlice []string
	}{
		{[][]string{{"name", "year"}, {"john", "2020"}, {"smith", "2019"}}, []string{"john", "smith"}, []string{"2020", "2019"}},
	}
	for _, test := range tests {
		pivotedMap := getCsvMap(test.input)
		if !compareSlices(pivotedMap["name"], test.expectedNameSlice) && compareSlices(pivotedMap["year"], test.expectedYearSlice) {
			t.Error("Csv map wasn't created as expected")
		}
	}

}

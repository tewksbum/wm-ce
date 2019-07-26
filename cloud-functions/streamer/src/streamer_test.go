package streamer

import (
	"reflect"
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
		inputHeader       []string
		inputData         [][]string
		expectedNameSlice []string
		expectedYearSlice []string
	}{
		{[]string{"name", "year"}, [][]string{{"john", "2020"}, {"smith", "2019"}}, []string{"john", "smith"}, []string{"2020", "2019"}},
	}
	for _, test := range tests {
		pivotedMap := getCsvMap(test.inputHeader, test.inputData)
		if !compareSlices(pivotedMap["name"], test.expectedNameSlice) && compareSlices(pivotedMap["year"], test.expectedYearSlice) {
			t.Error("Csv map wasn't created as expected")
		}
	}
}

func TestGetColumnStats(t *testing.T) {
	var tests = []struct {
		input    []string
		expected map[string]string
	}{
		{[]string{"john", "smith", ""}, map[string]string{"max": "smith", "min": "john", "rows": "3", "unique": "2", "populated": "0.3333333333333333"}},
		{[]string{"test", "test", "test"}, map[string]string{"max": "test", "min": "test", "rows": "3", "unique": "1", "populated": "0"}},
	}
	for _, test := range tests {
		stats := getColumnStats(test.input)
		equal := reflect.DeepEqual(stats, test.expected)
		if !equal {
			t.Errorf("Column stats %v not equal to expected %v", stats, test.expected)
		}
	}
}

func TestFlattenStats(t *testing.T) {
	var tests = []struct {
		nameCol  []string
		yearCol  []string
		expected map[string]string
	}{
		{[]string{"john", "mike", "rob"}, []string{"1998", "2000", "2015"}, map[string]string{"name.min": "john", "year.min": "1998", "year.populated": "0", "name.rows": "3", "name.populated": "0", "name.unique": "3", "year.rows": "3", "year.unique": "3", "year.max": "2015", "name.max": "rob"}},
		{[]string{"test", "test", "test"}, []string{"1", "3", ""}, map[string]string{"year.rows": "3", "name.max": "test", "year.unique": "2", "year.max": "3", "year.populated": "0.3333333333333333", "name.min": "test", "name.populated": "0", "name.rows": "3", "name.unique": "1", "year.min": "1"}},
	}
	for _, test := range tests {
		colStats := make(map[string]map[string]string)
		colStats["name"] = getColumnStats(test.nameCol)
		colStats["year"] = getColumnStats(test.yearCol)
		flattened := flattenStats(colStats)
		equal := reflect.DeepEqual(flattened, test.expected)
		if !equal {
			t.Errorf("Column stats %v not equal to expected %v", flattened, test.expected)
		}
	}
}
func TestGetProfilerStats(t *testing.T) {
	var tests = []struct {
		colStats map[string]map[string]string
		headers  []string
		expected Profiler
	}{
		{
			map[string]map[string]string{"name": {"max": "smith", "min": "john", "rows": "3", "unique": "2", "populated": "0.3333333333333333"}},
			[]string{"name", "year"},
			Profiler{"name.rows": "3", "name.unique": "2", "file": "test/test", "request": "test", "owner": "test/", "columnHeaders": "name,year", "name.max": "smith", "name.min": "john", "columns": 2, "name.populated": "0.3333333333333333"},
		},
	}

	for _, test := range tests {
		profiler := getProfilerStats("test/test", len(test.headers), test.headers, test.colStats)
		equal := reflect.DeepEqual(profiler, test.expected)
		if !equal {
			t.Errorf("Profiler %v not equal to expected %v", profiler, test.expected)
		}
	}
}

func TestGetNERentry(t *testing.T) {
	var tests = []struct {
		input    NERresponse
		expected NERentry
	}{
		{
			NERresponse{
				Columns: []NERcolumns{
					{
						ColumnName:  "email",
						NEREntities: map[string]float64{"ORG": 0.5},
					},
				},
				ElapsedTime: 16.339344,
				Owner:       "OWNER1",
				Source:      "SOURCE1",
				TimeStamp:   "2019-07-25 19:51:05.252920",
			},
			NERentry{
				"TimeStamp":         "2019-07-25 19:51:05.252920",
				"ElapsedTime":       16.339344,
				"Owner":             "OWNER1",
				"Source":            "SOURCE1",
				"columns.email.ORG": 0.5,
			},
		},
	}

	for _, test := range tests {
		nerentry := getNERentry(test.input)
		equal := reflect.DeepEqual(nerentry, test.expected)
		if !equal {
			t.Errorf("Profiler %v not equal to expected %v", nerentry, test.expected)
		}
	}
}

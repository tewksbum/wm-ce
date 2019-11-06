package streamerapi

import (
	"encoding/json"
	"fmt"
	"strings"
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
		expected []string
	}{
		{map[string]string{"A": "1", "B": "2"},
			[]string{`{"A":"1","B":"2"}`,
				`{"B":"2","A":"1"}`}},
		{map[string]string{"firstProperty": "1"},
			[]string{`{"firstProperty":"1"}`}},
	}
	for _, test := range tests {
		flatInput := flattenMap(test.input)
		pass := false

		for _, exp := range test.expected {
			if flatInput == exp {
				pass = true
			}
		}
		if !pass {
			t.Errorf("Expecting %s got %s", test.expected, flatInput)
		}
	}
}

//Basic json unmarshalling tests.
func TestUnmarshalToString(t *testing.T) {
	var d struct {
		AccessKey    string `json:"accessKey"`
		FileURL      string `json:"fileUrl"`
		Organization string `json:"organization"`
		Source       string `json:"source"`
		Owner        string `json:"owner"`
		Passthrough  string `json:"passthrough"`
		Attributes   string `json:"attributes"`
	}

	jsonEx := `{
		"passthrough":[],
		"attributes":{
			"key":"value"
			}
		}
		`
	if err := json.NewDecoder(strings.NewReader(jsonEx)).Decode(&d); err != nil {
		fmt.Println(err)
	}
}

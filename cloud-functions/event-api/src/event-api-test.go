package eventapi

import (
	"encoding/json"
	"fmt"
	"log"
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

func ToJson(v *map[string]string) string {
	jsonString, err := json.Marshal(v)
	if err == nil {
		return string(jsonString)
	} else {
		log.Fatalf("%v Could not convert map %v to json: %v", v, err)
		return ""
	}

}

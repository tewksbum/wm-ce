package utils

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

// StructToMap converts a Record struct to a map[string]interface{} and removes
// entries on the blacklist
func StructToMap(i interface{}, blacklist []string) map[string]interface{} {
	j := make(map[string]interface{})
	r := make(map[string]interface{})
	jj, _ := json.Marshal(i)
	json.Unmarshal(jj, &j)
	for k, v := range j {
		dont := false
		sk := ToSnakeCase(k)
		for _, blp := range blacklist {
			dont = blp == sk
			if dont {
				break
			}
		}
		if !dont {
			r[sk] = v
		}
	}
	return r
}

// ToSnakeCase converts camelcase str to snake_case
func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// FlattenMap Takes a map and flattens it
func FlattenMap(mstring map[string]string) string {
	flat := "{"
	for key, value := range mstring {
		flat = flat + `"` + key + `":"` + value + `",`
	}
	//remove extra comma if there's one
	if flat[:len(flat)-1] == "," {
		flat = flat[:len(flat)-1]
	}
	flat += "}"
	if flat == "{}" {
		return ""
	}
	return flat
}

// I64toa converts int64 to string
func I64toa(n int64) string {
	return strconv.FormatInt(n, 10)
}

// Itoa converts int64 to string
func Itoa(n int) string {
	return I64toa(int64(n))
}

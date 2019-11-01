package utils

import (
	"strconv"
)

// FlattenMap Takes a map and flattens it
func FlattenMap(mstring map[string]string) string {
	flat := "{"
	for key, value := range mstring {
		flat = flat + `"` + key + `":"` + value + `",`
	}
	//remove extra comma if there's one
	if flat[:len(flat)-1] == "," {
		flat = flat[:len(flat)-1] + "}"
	}
	return flat
}

// I64toa converts int64 to string
func I64toa(n int64) string {
	return strconv.FormatInt(n, 10)
}

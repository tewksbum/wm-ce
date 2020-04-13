package cpfile

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"unicode"
)

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, item) {
			return true
		}
	}
	return false
}

func GetSignatureField(v *Signature, field string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(string)
}

func GetSignatureSliceValues(source []Signature, field string) []string {
	slice := []string{}
	for _, s := range source {
		slice = append(slice, GetSignatureField(&s, field))
	}
	return slice
}

func GetRecordIDNormalizedSliceValues(source []Signature, field string) []string {
	slice := []string{}
	for _, s := range source {
		slice = append(slice, Left(GetSignatureField(&s, field), 36))
	}
	return slice
}

func Left(str string, num int) string {
	if num <= 0 {
		return ``
	}
	if num > len(str) {
		num = len(str)
	}
	return str[:num]
}

func ToAsciiArray(s string) []int {
	runes := []rune(s)

	var result []int

	for i := 0; i < len(runes); i++ {
		result = append(result, int(runes[i]))
	}
	return result
}

func IsInt(s string) bool {
	for _, c := range s {
		if !unicode.IsDigit(c) {
			return false
		}
	}
	return true
}

func GetKVPValue(v []KVP, key string) string {
	for _, kvp := range v {
		if kvp.Key == key {
			return kvp.Value
		}
	}
	return ""
}

func schoolYearFormatter(schoolYear, classStanding string) string {
	LogDev(fmt.Sprintf("schoolYear: %v classStanding: %v", schoolYear, classStanding))
	if classStanding == "Freshman" {
		return "FY" + schoolYear
	}
	return schoolYear
}

func listTypeFormatter(listType string) string {
	LogDev(fmt.Sprintf("listType: %v", listType))
	if len(listType) > 0 {
		return listType[0:1]
	}
	return listType
}

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
}

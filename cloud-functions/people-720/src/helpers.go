package people720

import (
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"unicode"

	"github.com/fatih/structs"
)

func GetSmallestYear(values []string) string {
	if len(values) == 0 {
		return ""
	}
	eligible := []string{}
	for _, v := range values {
		if strings.HasPrefix(v, "20") && len(v) == 4 && IsInt(v) {
			eligible = append(eligible, v)
		}
	}
	if len(eligible) > 0 {
		sort.Strings(eligible)
		return eligible[0]
	} else {
		return ""
	}
}

func GetAdValid(values []string) string {
	if len(values) == 0 {
		return "FALSE"
	}
	for _, v := range values {
		if v == "TRUE" {
			return "TRUE"
		}
	}
	return "FALSE"
}

func GetMatchKeyFieldFromStruct(v *PeopleOutput, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func GetMatchKeyFieldFromDSFiber(v *PeopleFiberDS, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func GetMatchKey360ByName(v []MatchKey360, key string) *MatchKey360 {
	for _, m := range v {
		if m.Key == key {
			return &m
		}
	}
	return &MatchKey360{}
}

func GetPeopleFiberSearchFields(v *PeopleFiberDS) []string {
	var searchFields []string
	searchFields = append(searchFields, fmt.Sprintf("RECORDID=%v", v.RecordID))
	if len(v.EMAIL.Value) > 0 {
		searchFields = append(searchFields, fmt.Sprintf("EMAIL=%v", strings.ToUpper(v.EMAIL.Value)))
	}
	if len(v.PHONE.Value) > 0 && len(v.FINITIAL.Value) > 0 {
		searchFields = append(searchFields, fmt.Sprintf("PHONE=%v&FINITIAL=%v", strings.ToUpper(v.PHONE.Value), strings.ToUpper(v.FINITIAL.Value)))
	}
	if len(v.CITY.Value) > 0 && len(v.STATE.Value) > 0 && len(v.LNAME.Value) > 0 && len(v.FNAME.Value) > 0 && len(v.AD1.Value) > 0 {
		searchFields = append(searchFields, fmt.Sprintf("FNAME=%v&LNAME=%v&AD1=%v&CITY=%v&STATE=%v", strings.ToUpper(v.FNAME.Value), strings.ToUpper(v.LNAME.Value), strings.ToUpper(v.AD1.Value), strings.ToUpper(v.CITY.Value), strings.ToUpper(v.STATE.Value)))
	}
	return searchFields
}

func GetPeopleGoldenSearchFields(v *PeopleGoldenDS) []string {
	log.Printf("golden record: %+v", v)
	var searchFields []string
	if len(v.EMAIL) > 0 {
		searchFields = append(searchFields, fmt.Sprintf("EMAIL=%v", strings.ToUpper(v.EMAIL)))
	}
	if len(v.PHONE) > 0 && len(v.FINITIAL) > 0 {
		searchFields = append(searchFields, fmt.Sprintf("PHONE=%v&FINITIAL=%v", strings.ToUpper(v.PHONE), strings.ToUpper(v.FINITIAL)))
	}
	if len(v.CITY) > 0 && len(v.STATE) > 0 && len(v.LNAME) > 0 && len(v.FNAME) > 0 && len(v.AD1) > 0 {
		searchFields = append(searchFields, fmt.Sprintf("FNAME=%v&LNAME=%v&AD1=%v&CITY=%v&STATE=%v", strings.ToUpper(v.FNAME), strings.ToUpper(v.LNAME), strings.ToUpper(v.AD1), strings.ToUpper(v.CITY), strings.ToUpper(v.STATE)))
	}
	return searchFields
}

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, item) {
			return true
		}
	}
	return false
}

func ConvertPassthrough(v map[string]string) []Passthrough360 {
	var result []Passthrough360
	if len(v) > 0 {
		for mapKey, mapValue := range v {
			pt := Passthrough360{
				Name:  mapKey,
				Value: mapValue,
			}
			result = append(result, pt)
		}
	}
	return result
}

func GetFiberDS(v *PeopleFiber) PeopleFiberDS {
	p := PeopleFiberDS{
		OwnerID:     v.Signature.OwnerID,
		Source:      v.Signature.Source,
		EventType:   v.Signature.EventType,
		EventID:     v.Signature.EventID,
		RecordID:    v.Signature.RecordID,
		FiberType:   v.Signature.FiberType,
		Passthrough: v.Passthrough,
		CreatedAt:   v.CreatedAt,
	}
	PopulateFiberMatchKeys(&p, &(v.MatchKeys))
	return p
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

func SetPeople360SetOutputFieldValues(v *PeopleSetDS, field string, value []string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	// LogDev(fmt.Sprintf("SetPeople360SetOutputFieldValues: %v %v", field, value))
}

func SetPeople360GoldenOutputFieldValue(v *PeopleGoldenDS, field string, value string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	// LogDev(fmt.Sprintf("SetPeople360GoldenOutputFieldValue: %v %v", field, value))
}

func SetPeopleFiberMatchKeyField(v *PeopleFiberDS, field string, value MatchKeyField) {
	LogDev(fmt.Sprintf("SetPeopleFiberMatchKeyField: %v %v", field, value))
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))

}

func PopulateSetOutputSignatures(target *PeopleSetDS, values []Signature) {
	KeyList := structs.Names(&Signature{})
	for _, key := range KeyList {
		SetPeople360SetOutputFieldValues(target, key, GetSignatureSliceValues(values, key))
		if key == "RecordID" {
			SetPeople360SetOutputFieldValues(target, key+"Normalized", GetRecordIDNormalizedSliceValues(values, key))
		}
	}
}

func PopulateFiberMatchKeys(target *PeopleFiberDS, source *PeopleOutput) {
	KeyList := structs.Names(&PeopleOutput{})
	for _, key := range KeyList {
		SetPeopleFiberMatchKeyField(target, key, GetMatchKeyFieldFromStruct(source, key))
	}
}

func PopulateSetOutputMatchKeys(target *PeopleSetDS, values []MatchKey360) {
	KeyList := structs.Names(&PeopleOutput{})
	for _, key := range KeyList {
		SetPeople360SetOutputFieldValues(target, key, GetSetValuesFromMatchKeys(values, key))
		SetPeople360SetOutputFieldValues(target, key+"Normalized", GetSetValuesFromMatchKeysNormalized(values, key))
	}
}

func PopulateGoldenOutputMatchKeys(target *PeopleGoldenDS, values []MatchKey360) {
	KeyList := structs.Names(&PeopleOutput{})
	for _, key := range KeyList {
		SetPeople360GoldenOutputFieldValue(target, key, GetGoldenValueFromMatchKeys(values, key))
	}
}

func GetGoldenValueFromMatchKeys(values []MatchKey360, key string) string {
	for _, m := range values {
		if m.Key == key {
			return m.Value
		}
	}
	return ""
}

func GetSetValuesFromMatchKeys(values []MatchKey360, key string) []string {
	for _, m := range values {
		if m.Key == key {
			return m.Values
		}
	}
	return []string{}
}

func GetSetValuesFromMatchKeysNormalized(values []MatchKey360, key string) []string {
	result := []string{}
	for _, m := range values {
		if m.Key == key {
			for _, v := range m.Values {
				result = append(result, strings.ToUpper(v))
			}
			return result
		}
	}
	return []string{}
}

func LogDev(s string) {
	if dev || true {
		log.Printf(s)
	}
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

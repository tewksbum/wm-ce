package cpfile

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
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
	// LogDev(fmt.Sprintf("schoolYear: %v classStanding: %v", schoolYear, classStanding))
	if classStanding == "Freshman" {
		return "FY" + schoolYear
	}
	return schoolYear
}

func roleFormatter(role string) string {
	// LogDev(fmt.Sprintf("listType: %v", listType))
	if len(role) > 0 {
		return role[0:1]
	}
	return role
}

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
}

func validateRole(role string) string {
	// LogDev(fmt.Sprintf("role: %v", role))
	if role == "" {
		return "Student"
	}
	return role
}

func copyFileToBucket(ctx context.Context, event Event, records [][]string, bucket string) (filename string) {
	//Bucket
	sb := cs.Bucket(bucket)
	// store it in bucket
	var buf bytes.Buffer
	csv := csv.NewWriter(&buf)
	csv.WriteAll(records)
	csv.Flush()

	csvBytes := buf.Bytes()

	file := sb.Object(GetKVPValue(event.Passthrough, "sponsorCode") + "." + GetKVPValue(event.Passthrough, "masterProgramCode") + strings.ToUpper(GetKVPValue(event.Passthrough, "seasonUpload")) + "." + GetKVPValue(event.Passthrough, "schoolYear") + "." + event.EventID + "." + strconv.Itoa(len(records)-1) + ".csv")

	writer := file.NewWriter(ctx)
	if _, err := io.Copy(writer, bytes.NewReader(csvBytes)); err != nil {
		log.Printf("File cannot be copied to bucket %v", err)
		return ""
	}
	if err := writer.Close(); err != nil {
		log.Printf("Failed to close bucket write stream %v", err)
		return ""
	}
	return file.ObjectName()
}

func getEnvFloat(key string) float64 {
	s := os.Getenv(key)
	if s == "" {
		s = "0"
	}

	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

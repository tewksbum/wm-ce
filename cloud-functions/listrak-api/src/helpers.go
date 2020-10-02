package listrakapi

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
)

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, item) {
			return true
		}
	}
	return false
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

func validateRole(role string) string {
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

	file := sb.Object(GetKVPValue(event.Passthrough, "masterProgramCode") + "." + GetKVPValue(event.Passthrough, "sponsorCode") + "." + GetKVPValue(event.Passthrough, "schoolYear") + "." + event.EventID + "." + strconv.Itoa(len(records)-1) + ".csv")

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

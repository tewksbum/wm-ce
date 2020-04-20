package listrakapi

import (
	"fmt"
	"log"
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
	LogDev(fmt.Sprintf("role: %v", role))
	if role == "" {
		return "Student"
	}
	return role
}

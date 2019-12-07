package purgeutil

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestProcessRequest run purge locally
// complete go.testEnvVars in vscode settings before running
// "go.testEnvVars": {
// 	"GOOGLE_APPLICATION_CREDENTIALS": "P:\\work\\wemade\\gcp\\wemade-core-192394ec89ff.json",
// 	"PROJECTID": "wemade-core",
// 	"ENVIRONMENT": "dev",
// 	"CLIENTID": "wemade",
// 	"CLIENTSECRET": "cool_works"
// },
func TestProcessRequest1(t *testing.T) {
	json := `
	{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "namespace",
		"operation": "delete",
		"targetSelection": "",
		"targetSubSelection": ""
	}`

	req := httptest.NewRequest("POST", "/", strings.NewReader(json))
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	ProcessRequest(rr, req)

	got := rr.Body.String()

	fmt.Println(got)
}

func TestProcessRequest2(t *testing.T) {
	json := `
	{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "[default]",
		"targetSubSelection": ""
	}`

	req := httptest.NewRequest("POST", "/", strings.NewReader(json))
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	ProcessRequest(rr, req)

	got := rr.Body.String()

	fmt.Println(got)
}

package filereport

import (
	"context"
	"fmt"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"cloud.google.com/go/pubsub"
)

func TestStep1(t *testing.T) {
	json := `{
		"id": "9cfdc586-6a7c-4347-9a15-25fa9cf57907",
		"requestedAt": "2020-04-24T13:13:02Z",
		"attributes": [{"k":"campaignName","v":"mlr-hrp-RHL-2021"},{"k":"classStanding","v":"Freshman"},{"k":"organization","v":"MLR"},{"v":"true","k":"PermM"},{"k":"PermS","v":"true"},{"k":"listType","v":"Student"},{"k":"schoolYear","v":"2021"},{"k":"uploadType","v":"P"},{"v":"true","k":"PermE"},{"k":"campaignId","v":"22957"}],
		"passthroghs": [{"k":"listType","v":"Student"},{"k":"orderByDate","v":"2020-08-11"},{"k":"salutation","v":"To The Parent(s) of:"},{"k":"schoolCode","v":"MLR"},{"k":"schoolName","v":"Millersville University"},{"k":"inputType","v":"P"},{"k":"masterProgramCode","v":"RHL"},{"k":"schoolColor","v":"#D59F0F"},{"v":"2021","k":"schoolYear"},{"k":"sponsorCode","v":"mlr-hrp"},{"v":"MLR1R0LQQQ","k":"ADCODE"}],
		"customerId": "mlr-hrp",
		"inputFilePath": "https://storage.googleapis.com/qa-ocm-portal/uploads/student-lists/mlr-hrp/1586986821-1585317841-MLRRHLPartial2021%20%281%29.xlsx?Expires=1902346821&GoogleAccessId=portal-gql%40ocm-core-qa.iam.gserviceaccount.com&Signature=AjF9Ju1ZQxZtugWZRmuRZrBaNo1JE%2Bwq2GBRPni8EcxLSNpyEeTcIJryP%2B9TzWpD2Af1vl5UXLQxGw9KzbFPempM61v6yCnIHBFveE5zkHJnMv7cneySdrT46eZ1CNovSZ5NFrdYICishnu8AYTpY2VUwcBoApf2YonGy19NJ4EcP1peqnQjUzLpXkx58PvLt4cwPSpy7Ss9m8XyYv%2BxAb6m4qHv%2Ft5nG38%2Ft8ZOvk7cUc0nIbOPWrD1U%2BkRUIstjhd5iYFga",
		"inputFileName": "1586986821-1585317841-MLRRHLPartial2021 (1).xlsx",
		"owner": "mlr-hrp",
		"statusLabel": "Request Received",
		"statusBy": "wm-file-api-dev"
	}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-api-dev",
	}
	ProcessUpdate(context.Background(), &message)
}

func TestCounter1(t *testing.T) {
	json := `{
		"id": "9cfdc586-6a7c-4347-9a15-25fa9cf57907",
		"counters": [
			{"type": "Record", "name": "Total", "inc": false, "count": 100}
		]
	}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-processor-dev",
	}
	ProcessUpdate(context.Background(), &message)
}

func TestCounter2(t *testing.T) {
	json := `{
		"id": "9cfdc586-6a7c-4347-9a15-25fa9cf57907",
		"counters": [
			{"type": "Record", "name": "Deleted", "inc": true, "count": 1}
		]
	}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-processor-dev",
	}
	ProcessUpdate(context.Background(), &message)
}

func TestColumns(t *testing.T) {
	json := `{
		"id": "9cfdc586-6a7c-4347-9a15-25fa9cf57907",
		"columns": [
			"First Name","Last Name", "Address 1"
		]
	}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-processor-dev",
	}
	ProcessUpdate(context.Background(), &message)
}

func TestColumnMap1(t *testing.T) {
	json := `{
		"id": "9cfdc586-6a7c-4347-9a15-25fa9cf57907",
		"map": [
			{"k": "First Name", "v": "FNAME"},
			{"k": "Last Name", "v": "LNAME"}, 
			{"k": "Address 1", "v": "AD1"},
			{"k": "Address 2", "v": "AD2"}
		]
	}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-processor-dev",
	}
	ProcessUpdate(context.Background(), &message)
}
func TestColumnMap2(t *testing.T) {
	json := `{
		"id": "9cfdc586-6a7c-4347-9a15-25fa9cf57907",
		"map": [
			{"k": "First Name", "v": "LNAME"},
			{"k": "Last Name", "v": "FNAME"}, 
			{"k": "Address 1", "v": "AD1"},
			{"k": "Address 1", "v": "AD2"}
		]
	}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-processor-dev",
	}
	ProcessUpdate(context.Background(), &message)
}

func TestColumnStat(t *testing.T) {
	json := `{
		"id": "9cfdc586-6a7c-4347-9a15-25fa9cf57907",
		"inputStats": {
			"First Name": {
				"name": "First Name",
				"min": "Abbey",
				"max": "Williams",
				"sparsity": 99
			}
		}
	}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-processor-dev",
	}
	ProcessUpdate(context.Background(), &message)
}

func TestMatchKeyStats(t *testing.T) {
	json := `{
		"id": "9cfdc586-6a7c-4347-9a15-25fa9cf57907",
		"matchKeyStats": {
			"AD1": 1,
			"AD2": 1
		}
	}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-processor-dev",
	}
	ProcessUpdate(context.Background(), &message)
}

func TestRecord1(t *testing.T) {
	json := `{
		"id": "9cfdc586-6a7c-4347-9a15-25fa9cf57907",
		"recordList": [
			{
				"id": "d55738bf-07ce-49ca-8c8d-b5052d53b2e1",
				"row": 1,
				"createdOn": "2020-05-06T05:06:12Z"
			},
			{
				"id": "b878dba5-4561-4a45-a7c8-b35b59b2ea46",
				"row": 2,
				"createdOn": "2020-05-06T05:06:12Z"
			}			
		]
	}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-processor-dev",
	}
	ProcessUpdate(context.Background(), &message)
}

func TestRecord2(t *testing.T) {
	json := `{
		"id": "9cfdc586-6a7c-4347-9a15-25fa9cf57907",
		"recordList": [
			{
				"id": "d55738bf-07ce-49ca-8c8d-b5052d53b2e1",
				"disposition": "new",
				"fibers": [
					"ef0c3403-c5ac-455d-b920-cf67f5e1ebfb",
					"390cd601-1db4-4426-a0a0-ec0610d9baff"
				]
			}
		],
		"fiberList": [
			{
				"id": "ef0c3403-c5ac-455d-b920-cf67f5e1ebfb",
				"type": "default"
			},
			{
				"id": "390cd601-1db4-4426-a0a0-ec0610d9baff",
				"type": "mar"
			}
		]
	}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-processor-dev",
	}
	ProcessUpdate(context.Background(), &message)
}

func TestRecord3(t *testing.T) {
	json := `{"id":"b34f2060-eb40-4ee8-a8d9-ffb7842fbc73","processingBegin":"0001-01-01T00:00:00Z","statusTime":"0001-01-01T00:00:00Z","errors":null,"warnings":null,"audits":null,"counters":null,"inputStats":null,"matchKeyStats":null,"recordList":[{"id":"2d394ea7-6266-44d1-9c72-5777c3d3bec7","row":3,"createdOn":"2020-05-13T01:09:22.072473942Z","fibers":null,"sets":null}]}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-processor-dev",
	}
	ProcessUpdate(context.Background(), &message)
}

func TestReport(t *testing.T) {
	json := `{"bypass": "@U1Q6TAy^QH,98y", "eventId": "9cfdc586-6a7c-4347-9a15-25fa9cf57907"}`
	req := httptest.NewRequest("POST", "/", strings.NewReader(json))
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	GetReport(rr, req)
	got := rr.Body.String()
	fmt.Println(got)
}

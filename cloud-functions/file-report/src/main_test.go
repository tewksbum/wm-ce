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
		"passthroughs": [{"k":"listType","v":"Student"},{"k":"salutation","v":"To The Parent(s) of:"},{"k":"schoolCode","v":"MLR"},{"k":"schoolName","v":"Millersville University"},{"k":"inputType","v":"P"},{"k":"masterProgramCode","v":"RHL"},{"k":"schoolColor","v":"#D59F0F"},{"v":"2021","k":"schoolYear"},{"k":"sponsorCode","v":"mlr-hrp"},{"v":"MLR1R0LQQQ","k":"ADCODE"}],
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

func TestPost(t *testing.T) {
	json := `{"id":"c560c153-1595-47e4-866e-4bd03de7c0fd","requestedAt":"2020-05-13T16:44:14.374025317Z","processingBegin":"0001-01-01T00:00:00Z","processingEnd":"0001-01-01T00:00:00Z","attributes":[{"k":"campaignName","v":"atu-rha-RHL-2021"},{"k":"classStanding","v":""},{"k":"listType","v":"Student"},{"k":"PermE","v":"true"},{"k":"PermS","v":"true"},{"k":"schoolYear","v":"2021"},{"k":"campaignId","v":"25196"},{"k":"uploadType","v":"New"},{"k":"organization","v":"ATU"},{"k":"PermM","v":"true"}],"passthroughs":[{"k":"salutation","v":"To The Parent(s) of:"},{"k":"schoolCode","v":"ATU"},{"k":"masterProgramCode","v":"RHL"},{"k":"orderByDate","v":"2020-08-03"},{"k":"schoolYear","v":"2021"},{"k":"sponsorCode","v":"atu-rha"},{"k":"ADCODE","v":"ATU1R0LQQQ"},{"k":"inputType","v":"New"},{"k":"listType","v":"Student"},{"k":"schoolName","v":"Arkansas Tech"}],"customerId":"abi-abim","inputFilePath":"https://00e9e64bacd4f20719cf31cfe902043c76b3d2cc229eaf00c4-apidata.googleusercontent.com/download/storage/v1/b/ocm_school_raw_files/o/drop%2Fjy%2Fuga1.xlsx?qk=AD5uMEsh_9NdEJrF5D8Fk-y-P59ISRqqZZWvTmU19yLVGQoOZeiv98vtWEoc4I1XXnj9fxs2jaCM23BaWKrjKkIt08bpfCUL5WZkXLH8FuxmYFpS9TYQ5uVmTpzv59a0I0SMVYWi0TiSynvpcYeJ85AnQHFuw6rsM9VkkVYvyZlFryeXbvTZ5ZloSZaFafx8VVV7WOqnpR9E7RiWF6Rs-aFrvVodf9XmN-WPk7kIRG7mpTkWjpqpDsvjPlibUh9gj5mCFtJQL0bi4FAKaGCuG8o5y3e2TRNmrUnObXEAk5U0ULDmXmbu38U8hbgMd_ee-fZpmY37It-uv1xMxqyEBWUx7DE6FLR2tB5dXhO7Avzg0eJuFf-R7t9jCDYetepy1rRd89Wn3_odiQ1ExpoOuxy7DjpJh7wJ6jEVhZ__XtHQub1f3-SBVerKKnIuoGc6zPzoo235pusq74yx2wjESV8o_gllL-baL4QDje8oDuC0MTIs3eYS7D6lY1Kpky4N10b7yAcHNJ_psRgKSFPtYdMHMO03BANM6-TMsaN8rPNGBdWob-fDaxl8Mz_SxcG7sMvnwFJR5LKS81aSsn9rnnG-ZCYFCYwnRhpncCWX6_UhROiALjwv2mmbOU05s14tEfrCilbjGiMJbV6AASsdTp2AjmohnOO-sVbcXxKU2X-1mowQmp3beDP3Uz7EM_yVDBIn5YdtcOorl1D0WhwipGs8Tg7fb8hpeCbn_gIFQQ_cXZMNjZs6kBkYoViTNYCCjADgiGK4pLO7F1sSc6gGKo7kY7Ez7azLYu1EVgBSPrJWadnO68Ms5KWnIqsr8yr_rGuvwhMzgUAE\u0026isca=1","inputFileName":"uga1.xlsx","owner":"abi-abim","statusLabel":"request received","statusBy":"wm-dev-file-api","statusTime":"2020-05-13T16:44:14.374055205Z"}`
	re := regexp.MustCompile(`\r?\n`)
	var message pubsub.Message
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{
		"source": "wm-file-api-dev",
	}
	ProcessUpdate(context.Background(), &message)
}

func Test360(t *testing.T) {
	// json = `{"id":"47c45992-434f-45d3-908f-6974e8007e23","processingBegin":"0001-01-01T00:00:00Z","processingEnd":"0001-01-01T00:00:00Z","statusTime":"0001-01-01T00:00:00Z","errors":null,"warnings":null,"counters":[{"type":"Golden","name":"Created","count":1,"inc":true},{"type":"Set","name":"Created","count":1,"inc":true},{"type":"Set","name":"Expired","count":2,"inc":true},{"type":"Golden","name":"Expired","count":2,"inc":true}],"fiberList":[{"createdOn":"0001-01-01T00:00:00Z","sets":["32c47a2e-78a6-43b8-92ef-03906dc3279c"]}],"setList":[{"id":"32c47a2e-78a6-43b8-92ef-03906dc3279c","fiberCount":1,"createdOn":"2020-05-13T18:04:42.966266042Z","deletedOn":"0001-01-01T00:00:00Z"},{"id":"f916ba5d-0563-4331-a03e-bc60c3554f00","createdOn":"0001-01-01T00:00:00Z","deletedOn":"2020-05-13T18:04:43.059789248Z","isDeleted":true,"replacedBy":"32c47a2e-78a6-43b8-92ef-03906dc3279c"},{"id":"f8d2e31c-35f2-4256-9cf0-a154799b0e28","createdOn":"0001-01-01T00:00:00Z","deletedOn":"2020-05-13T18:04:43.059790774Z","isDeleted":true,"replacedBy":"32c47a2e-78a6-43b8-92ef-03906dc3279c"}]}`
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
	json := `{"bypass": "@U1Q6TAy^QH,98y", "eventId": "337c3353-0a20-4329-93ec-4ce90c431cc9"}`
	req := httptest.NewRequest("POST", "/", strings.NewReader(json))
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	GetReport(rr, req)
	got := rr.Body.String()
	fmt.Println(got)
}

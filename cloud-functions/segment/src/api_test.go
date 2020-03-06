package segment

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"segment/models"
	"testing"
)

func createReqRes(verb string, addr string, body io.Reader) (http.ResponseWriter, *http.Request) {
	// handler := func(w http.ResponseWriter, r *http.Request) {

	// }

	req := httptest.NewRequest(verb, addr, body)
	w := httptest.NewRecorder()
	// handler(w, req)

	// resp := w.Result()
	// b, _ := ioutil.ReadAll(resp.Body)

	// fmt.Println(resp.StatusCode)
	// fmt.Println(resp.Header.Get("Content-Type"))
	// fmt.Println(string(b))

	return w, req
}

func TestUpsert(t *testing.T) {
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	data := models.DecodeRecord{ // map[string]interface{}{
		// URL:     "https://wemade.io",
		// Browser: "faek browser 1.0.8",
		// "":       "",,
	}
	data.OwnerID = 5648073946562560

	input, _ := json.Marshal(map[string]interface{}{
		"accessKey":    "6a30ed702e8a6614c7fba7e7e24eb1bd8807a2d9",
		"entityType":   "people",
		"age":          "108",
		"writeToOwner": false,     //true
		"peopleId":     "poopppp", //"c3d142cd-327b-4280-a7d0-4eae98471679", //"3afb8d06-56e3-46c2-bc85-ec15708cf540",
		"householdId":  "6a30ed702e8a6614c7fba7e7e24eb1bd8807a2d9",
		"firstName":    "KAKA", "lastName": "Ali",
		"attributes": map[string]string{"organization": "mracu"},
		"emails": []map[string]string{
			{"email": "email@ocm.com", "type": "work"},
			{"email": "email@gmail.com", "type": "persona"},
		},
		"phones": []map[string]string{
			{"phone": "+123456789", "type": "work"},
			{"phone": "+1595981566234", "type": "persona4"},
			{"phone": "+2595981566234", "type": "persona5"},
			{"phone": "+3595981566234", "type": "personaSE"},
		},
		"passthough": []models.Passthrough360{
			models.Passthrough360{
				Name:  "pepito1",
				Value: "sorongo1",
			},
			models.Passthrough360{
				Name:  "pepito2",
				Value: "sorongo2",
			},
		},
		"signature":   "2be16825-5dc4-4c9d-aa0a-6b851ef16ff7",
		"signatures":  []string{"d99f8fe3-bdb4-49dd-b45c-e145bf58cd63", "1a86cae5-4495-4e9b-81af-c5f24c90972a", "05f633bb-4192-47b7-beb4-dccefe363a13"},
		"expiredSets": []string{"6cfc2a31-79f3-4a63-a113-58500f5026c4", "c67f2e2a-9507-4154-8863-2e70d89e9519"},
		// "accessKey":  "05c8da151b6281c92ad9c6971a7786ab",
		// "entityType": "event",
		"source":   "test",
		"type":     "jajaja",
		"browser":  "Duck OS 1.0.8.0",
		"eventIds": []string{"ec86f654-ebac-4f90-8cb2-1eb083feebfb", "aa3798d3-6a7e-4817-a738-432ce7f80266"},
		"id":       "ca1a173b-6cf9-4cff-a7c3-8241df12a487",
		// "signature": map[string]interface{}{"ownerId": 2, "source": "Testing", "eventId": "20344429-d7b5-4456-b657-b3237effecf3", "eventType": "UPLOAD"},
		// "signatures": []map[string]interface{}{{"ownerId": 2, "source": "Testing", "eventId": "20344429-d7b5-4456-b657-b3237effecf3", "eventType": "UPLOAD",
		// 	"recordId": "a46d0e9d-fa20-49ab-bce0-e47cbbd531c9"}},
		// "createdAt": "2019-11-12T22:29:49.181287005Z", "timestamp": "2019-11-12T22:29:49.181285984Z",
		// "fibers":       []string{"a46d0e9d-fa20-49ab-bce0-e47cbbd531c9"},
		// "passthrough": nil,
	})
	// logger.DebugFmt("input: %s", input)
	w2, r2 := createReqRes("OPTIONS", "https://wemade.io/foo", nil)
	w1, r1 := createReqRes("POST", "https://wemade.io/foo", bytes.NewReader(input))
	tests := []struct {
		name string
		args args
	}{
		{
			name: "API OK 200",
			args: args{
				w: w1, r: r1,
			},
		},
		{
			name: "API Options",
			args: args{
				w: w2, r: r2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Upsert(tt.args.w, tt.args.r)
		})
	}
}

func TestRead(t *testing.T) {
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	// filters := models.QueryFilter{
	// 	field:  "",
	// 	op:     "",
	// 	opType: models.OperationTypeFilter,
	// 	// linkOperation: models.OperationLinkAnd,
	// 	// Value: w,
	// 	// Values: poop,
	// }
	// "eventIds": []string{"ec86f654-ebac-4f90-8cb2-1eb083feebfb", "aa3798d3-6a7e-4817-a738-432ce7f80266"},
	input, _ := json.Marshal(map[string]interface{}{
		"accessKey":   "6a30ed702e8a6614c7fba7e7e24eb1bd8807a2d9",
		"entityType":  "people",
		"doReadCount": true,
		// "columns":    []string{"*"},
		// "columns": []string{"record"},
		// "source":     "test",
		"filters": []map[string]interface{}{
			{
				"field": "JSON_SEARCH(eventIds, 'one', 'ec86f654-ebac-4f90-8cb2-1eb083feebfb')",
				"op":    models.OperationIsNotNull,
			},
			// {
			// 	"linkOperation": "AND",
			// 	"field":  "signature",
			// 	"op":     models.OperationIsNotNull,
			// 	// "values": []int{108, 108, 110},
			// },
			// {
			// 	"field":  "record.peopleid",
			// 	"op":     models.OperationNotIn,
			// 	"linkOperation": "and",
			// 	// "value":  "91de1279-46c2-4fdc-9566-2b2506415fdb",
			// 	"values": []int{108, 108, 110},
			// },
			// {
			// 	"opType": "order",
			// 	"field":  "created_at", //"timestamp",
			// 	"op":     "desc",
			// 	// "value":  "91de1279-46c2-4fdc-9566-2b2506415fdb",
			// 	// "values": []int{108, 108, 110},
			// },
			// {
			// 	"opType": "order",
			// 	"field":  "signature",
			// 	"op":     "asc",
			// 	// "value":  "91de1279-46c2-4fdc-9566-2b2506415fdb",
			// 	// "values": []int{108, 108, 110},
			// },
		},
	})
	// logger.DebugFmt("input: %s", input)
	w1, r1 := createReqRes("POST", "https://wemade.io/foo", bytes.NewReader(input))
	tests := []struct {
		name string
		args args
	}{
		{
			name: "API READ OK 200",
			args: args{
				w: w1, r: r1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Read(tt.args.w, tt.args.r)
		})
	}
}

// // func TestDelete(t *testing.T) {
// // 	type args struct {
// // 		w http.ResponseWriter
// // 		r *http.Request
// // 	}
// // 	tests := []struct {
// // 		name string
// // 		args args
// // 	}{
// // 		// TODO: Add test cases.
// // 	}
// // 	for _, tt := range tests {
// // 		t.Run(tt.name, func(t *testing.T) {
// // 			Delete(tt.args.w, tt.args.r)
// // 		})
// // 	}
// // }

func TestSweepExpiredSets(t *testing.T) {
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	data := models.DecodeRecord{ // map[string]interface{}{
		// URL:     "https://wemade.io",
		// Browser: "faek browser 1.0.8",
		// "":       "",,
	}
	data.OwnerID = 5648073946562560

	input, _ := json.Marshal(map[string]interface{}{
		"accessKey":       "6a30ed702e8a6614c7fba7e7e24eb1bd8807a2d9",
		"entityType":      "people",
		"entityBlacklist": []string{"57"},
	})
	// logger.DebugFmt("input: %s", input)
	// w2, r2 := createReqRes("OPTIONS", "https://wemade.io/foo", nil)
	w1, r1 := createReqRes("POST", "https://wemade.io/foo", bytes.NewReader(input))
	tests := []struct {
		name string
		args args
	}{
		{
			name: "API SWEEPER OK 200",
			args: args{
				w: w1, r: r1,
			},
		},
		// {
		// 	name: "API Options",
		// 	args: args{
		// 		w: w2, r: r2,
		// 	},
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SweepExpiredSets(tt.args.w, tt.args.r)
		})
	}
}

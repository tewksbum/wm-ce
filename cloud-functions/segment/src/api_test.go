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
		"accessKey":  "0f26ca1527621312720ec57ca17be4d8",
		"entityType": "people",
		"peopleId":   "c3d142cd-327b-4280-a7d0-4eae98471679", //"3afb8d06-56e3-46c2-bc85-ec15708cf540",
		"firstName":  "Tembolin", "lastName": "Maswenya",
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
		"signature":  "2be16825-5dc4-4c9d-aa0a-6b851ef16ff7",
		"signatures": []string{"1a86cae5-4495-4e9b-81af-c5f24c90972a", "05f633bb-4192-47b7-beb4-dccefe363a13"},
		// "accessKey":  "05c8da151b6281c92ad9c6971a7786ab",
		// "entityType": "event",
		"source":  "test",
		"type":    "jajaja",
		"browser": "Duck OS 1.0.8.0",
		"eventId": "ec86f654-ebac-4f90-8cb2-1eb083feebfb",
		"id":      "ca1a173b-6cf9-4cff-a7c3-8241df12a487",

		// "signature": map[string]interface{}{"ownerId": 2, "source": "Testing", "eventId": "20344429-d7b5-4456-b657-b3237effecf3", "eventType": "UPLOAD"},
		// "signatures": []map[string]interface{}{{"ownerId": 2, "source": "Testing", "eventId": "20344429-d7b5-4456-b657-b3237effecf3", "eventType": "UPLOAD",
		// 	"recordId": "a46d0e9d-fa20-49ab-bce0-e47cbbd531c9"}},
		// "createdAt": "2019-11-12T22:29:49.181287005Z", "timestamp": "2019-11-12T22:29:49.181285984Z",
		// "fibers":       []string{"a46d0e9d-fa20-49ab-bce0-e47cbbd531c9"},
		// "passthroughs": nil,
	})
	// logger.InfoFmt("input: %s", input)
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
	// 	// opLink: models.OperationLinkAnd,
	// 	// Value: w,
	// 	// Values: poop,
	// }
	input, _ := json.Marshal(map[string]interface{}{
		"accessKey":  "3e92de26224fec73e70cc62c108864d1",
		"entityType": "decode",
		"source":     "test",
		"filters": []map[string]interface{}{
			{
				"field": "peopleId",
				"op":    models.OperationEquals,
				"value": "91de1279-46c2-4fdc-9566-2b2506415fdb",
				// "values": []int{108, 108, 110},
			},
			{
				"opLink": "AND",
				"field":  "signature",
				"op":     models.OperationIsNotNull,
				// "values": []int{108, 108, 110},
			},
			// {
			// 	"field":  "record.peopleid",
			// 	"op":     models.OperationNotIn,
			// 	"opLink": "and",
			// 	// "value":  "91de1279-46c2-4fdc-9566-2b2506415fdb",
			// 	"values": []int{108, 108, 110},
			// },
			{
				"opType": "order",
				"field":  "created_at", //"timestamp",
				"op":     "desc",
				// "value":  "91de1279-46c2-4fdc-9566-2b2506415fdb",
				// "values": []int{108, 108, 110},
			},
			{
				"opType": "order",
				"field":  "signature",
				"op":     "asc",
				// "value":  "91de1279-46c2-4fdc-9566-2b2506415fdb",
				// "values": []int{108, 108, 110},
			},
		},
	})
	// logger.InfoFmt("input: %s", input)
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

// func TestDelete(t *testing.T) {
// 	type args struct {
// 		w http.ResponseWriter
// 		r *http.Request
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			Delete(tt.args.w, tt.args.r)
// 		})
// 	}
// }

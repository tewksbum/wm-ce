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
		"accessKey":  "05c8da151b6281c92ad9c6971a7786ab",
		"entityType": "event",
		"source":     "test",
		"type":       "crazyloco",
		"browser":    "Duck OS 1.0.8.0",
		"eventId":    "ec86f654-ebac-4f90-8cb2-1eb083feebfb",
	})
	// logger.InfoFmt("input: %s", input)
	w2, r2 := createReqRes("OPTIONS", "https://wemade.io/foo", nil)
	w1, r1 := createReqRes("POSTO", "https://wemade.io/foo", bytes.NewReader(input))
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
	// 	Field:  "",
	// 	Op:     "",
	// 	OpType: models.OperationTypeFilter,
	// 	// OpLink: models.OperationLinkAnd,
	// 	// Value: w,
	// 	// Values: poop,
	// }
	input, _ := json.Marshal(map[string]interface{}{
		"accessKey":  "05c8da151b6281c92ad9c6971a7786ab",
		"entityType": "event",
		"source":     "test",
		"filters": []map[string]interface{}{
			// {
			// 	"field": "record.eventid",
			// 	"op":    models.OperationEquals,
			// 	"value": "91de1279-46c2-4fdc-9566-2b2506415fdb",
			// 	// "values": []int{108, 108, 110},
			// },
			{
				"field":  "record.eventid",
				"op":     models.OperationNotIn,
				"opLink": "and",
				// "value":  "91de1279-46c2-4fdc-9566-2b2506415fdb",
				"values": []int{108, 108, 110},
			},
			{
				"opType": "order",
				"field":  "timestamp",
				"op":     "asc",
				// "value":  "91de1279-46c2-4fdc-9566-2b2506415fdb",
				// "values": []int{108, 108, 110},
			},
		},
	})
	// logger.InfoFmt("input: %s", input)
	w1, r1 := createReqRes("GET", "https://wemade.io/foo", bytes.NewReader(input))
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

func TestDelete(t *testing.T) {
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Delete(tt.args.w, tt.args.r)
		})
	}
}

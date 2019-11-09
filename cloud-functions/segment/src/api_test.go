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
		"entityType": "decode",
		"source":     "test",
		"owner":      "OCM",
		"signature":  "f462a513-6af2-4252-81be-51d1e5bc8bb6",
		"peopleID":   "91de1279-46c2-4fdc-9566-2b2506415fdb",
	})
	// logger.InfoFmt("input: %s", input)
	w1, r1 := createReqRes("POST", "https://wemade.io/foo", bytes.NewReader(input))
	w2, r2 := createReqRes("OPTIONS", "https://wemade.io/foo", nil)
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

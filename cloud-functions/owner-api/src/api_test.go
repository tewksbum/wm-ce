package segment

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
	input, _ := json.Marshal(map[string]interface{}{
		// "accessKey": "0f26ca1527621312720ec57ca17be4d8",
		"accessKey": "4ZFGVumXw9043yH1SKFd9vubWHxMBAt3",
		// "accessKey": "3bb44f5242738d99820cd6a6b9588ae426749b88",
		"customer": map[string]interface{}{
			"id":         109109109,
			"externalId": "108108108",
			"owner":      "popo x externalid",
			"name":       "poo123",
			"enabled":    true,
			// "permissions": []string{"Customer:create"},
		},
	})
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Upsert(tt.args.w, tt.args.r)
		})
	}
}

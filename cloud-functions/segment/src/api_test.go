package segment

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"segment/wemade"
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

func TestAPI(t *testing.T) {
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	input, _ := json.Marshal(wemade.APIInput{
		AccessKey:  "81efed5f-57e8-4076-9506-6527d6532b00",
		EntityType: "event",
		Source:     "test",
		Owner:      "OCM",
		Data: wemade.Event{ // map[string]interface{}{
			URL:     "https://wemade.io",
			Browser: "faek browser 1.0.8",
			// "":       "",,
		},
	})
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
			API(tt.args.w, tt.args.r)
		})
	}
}

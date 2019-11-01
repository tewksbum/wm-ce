package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"segment/wemade"
	"testing"
	"time"
)

func createReqRes(verb string, addr string, body io.Reader) (http.ResponseWriter, *http.Request) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "<html><body>Hello World!</body></html>")
	}

	req := httptest.NewRequest(verb, addr, body)
	w := httptest.NewRecorder()
	handler(w, req)

	resp := w.Result()
	b, _ := ioutil.ReadAll(resp.Body)

	fmt.Println(resp.StatusCode)
	fmt.Println(resp.Header.Get("Content-Type"))
	fmt.Println(string(b))

	return w, req
}

func TestAPI(t *testing.T) {
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	input, _ := json.Marshal(wemade.APIInput{
		AccessKey:  "",
		EntityType: "orderHeader",
		Source:     "test",
		Owner:      "OCM",
		Data: wemade.OrderHeader{
			OrderID:   "7803aee4-717e-4a4c-80cc-4a08d63c4d73",
			SubTotal:  "108.92",
			OrderDate: time.Now(),
			// URL:     "https://foo.bar",
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
		// {
		// 	name: "API err != nil",
		// 	args: args{
		// 		w: w1, r: r1,
		// 	},
		// },
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

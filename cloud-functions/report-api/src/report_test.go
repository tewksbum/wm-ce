package reportapi

import (
	"net/http"
	"testing"
)

func TestLocalServer(t *testing.T) {
	http.HandleFunc("/wm-dev-report-api", ProcessRequest)
	http.HandleFunc("/wm-prod-report-api", ProcessRequest)
	http.ListenAndServe(":8090", nil)
}

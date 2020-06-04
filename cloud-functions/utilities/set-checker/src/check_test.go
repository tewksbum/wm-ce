package checkutil

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProcessRequestALL(t *testing.T) {
	json := `
	{
		"namespace": "prod-wne-sa"
	}`

	req := httptest.NewRequest("POST", "/", strings.NewReader(json))
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	ProcessRequest(rr, req)

	got := rr.Body.String()

	fmt.Println(got)
}

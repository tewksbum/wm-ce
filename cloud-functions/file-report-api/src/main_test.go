package filereport

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReport(t *testing.T) {
	json := `{"bypass": "@U1Q6TAy^QH,98y", "eventId": "d34f3149-2e30-4c8b-9161-0c9374c1c3ce"}`
	req := httptest.NewRequest("POST", "/", strings.NewReader(json))
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	GetReport(rr, req)
	got := rr.Body.String()
	fmt.Println(got)
}

func TestReportDetail(t *testing.T) {
	json := `{"bypass": "@U1Q6TAy^QH,98y", "eventId": "38b0e3f8-f556-4d84-b1b0-7f5083764159"}`
	req := httptest.NewRequest("POST", "/?detail=1", strings.NewReader(json))
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	GetReport(rr, req)
	got := rr.Body.String()
	fmt.Println(got)
}

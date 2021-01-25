package dsdumper

import (
	"fmt"
	"net/http/httptest"
	"testing"
)

func TestC(t *testing.T) {
	req := httptest.NewRequest("POST", "/", nil)
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	ProcessRequest(rr, req)

	got := rr.Body.String()

	fmt.Println(got)
}

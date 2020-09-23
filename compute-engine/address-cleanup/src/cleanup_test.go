package cleanup

import (
	"testing"
)

func TestRequest(t *testing.T) {
	RunPackages()
}

func TestRequest2(t *testing.T) {
	RunFedexPackages()
}

func TestRequest3(t *testing.T) {
	RunFedexHistoical2()
}

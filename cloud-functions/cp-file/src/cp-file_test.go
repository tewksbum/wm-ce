package cpfile

import (
	"context"
	"regexp"
	"testing"
)

func TestC(t *testing.T) {
	json := `{"eventId":"9fb1391a-0f16-4ffd-ac72-ddfd95607c38", "ownerId":"wiu-saa"}`
	re := regexp.MustCompile(`\r?\n`)
	var message PubSubMessage
	message.Data = []byte(re.ReplaceAllString(json, ""))
	GenerateCP(context.Background(), message)

}

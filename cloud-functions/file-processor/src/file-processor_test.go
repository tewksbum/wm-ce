package fileprocessor

import (
	"context"
	"regexp"
	"testing"
)

func Test720(t *testing.T) {
	json := `{"eventId":"3e7fe07f-68bc-4dc3-b17b-c4741a21ad8d", "ownerId":"wiu-saa"}`
	re := regexp.MustCompile(`\r?\n`)
	var message PubSubMessage
	message.Data = []byte(re.ReplaceAllString(json, ""))
	ProcessFile(context.Background(), message)

}

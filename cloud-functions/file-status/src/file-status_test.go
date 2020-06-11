package filestatus

import (
	"context"
	"regexp"
	"testing"
)

func TestCheckStatus(t *testing.T) {
	json := `{"signature":{"ownerId":"fla-men","source":"","eventId":"7068cfe7-a930-4e19-865c-88026d93773f","eventType":"","fiberType":"","recordId":""},"passthrough":null,"attributes":null,"eventData":{"bad-addresses":6,"certified-addresses":9,"message":"CP file generated successfully fla-men.RHL.2021.7068cfe7-a930-4e19-865c-88026d93773f.15.csv","parent-emails":0,"row-count":15,"status":"File Generated","student-emails":0}}`
	re := regexp.MustCompile(`\r?\n`)
	var message PubSubMessage
	message.Data = []byte(re.ReplaceAllString(json, ""))
	CheckStatus(context.Background(), message)
}

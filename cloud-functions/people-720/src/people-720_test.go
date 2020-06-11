package people720

import (
	"context"
	"regexp"
	"testing"
)

func Test720(t *testing.T) {
	json := `{"eventId":"cb95696b-cbea-4e20-98f2-bb4b69a19a03","ownerId":"aam-sata"}`
	re := regexp.MustCompile(`\r?\n`)
	var message PubSubMessage
	message.Data = []byte(re.ReplaceAllString(json, ""))
	People720(context.Background(), message)

}

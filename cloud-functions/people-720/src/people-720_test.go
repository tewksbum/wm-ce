package people720

import (
	"context"
	"regexp"
	"testing"
)

func Test720(t *testing.T) {
	json := `{"eventId":"f1673c8b-05ad-497f-9e60-5e80c5bf1b37", "ownerId":"aam-sata"}`
	re := regexp.MustCompile(`\r?\n`)
	var message PubSubMessage
	message.Data = []byte(re.ReplaceAllString(json, ""))
	People720(context.Background(), message)

}

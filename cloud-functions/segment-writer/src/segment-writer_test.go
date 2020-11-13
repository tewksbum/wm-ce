package segmentwriter

import (
	"context"
	"testing"
)

func TestRequest(t *testing.T) {
	var message PubSubMessage
	message.Data = []byte("")
	ProcessFiles(context.Background(), message)
}

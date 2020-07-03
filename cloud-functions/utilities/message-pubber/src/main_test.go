package pubber

import (
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
)

func TestMessageDropper(t *testing.T) {
	for i := 0; i < 5; i++ {
		var message pubsub.Message
		message.Data = []byte(`{"orderNumber": "W12345","amount": 123.45}`)
		topicR.Publish(ctx, &message)
		time.Sleep(1 * time.Second)
	}
}

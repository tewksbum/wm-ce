package dsrorderfetch

import (
	"context"
	"testing"

	"cloud.google.com/go/pubsub"
)

func TestRun(t *testing.T) {
	var message pubsub.Message
	message.Data = []byte(`["4119709"]`)
	Run(context.Background(), &message)
}

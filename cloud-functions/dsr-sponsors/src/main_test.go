package dsrsponsor

import (
	"context"
	"testing"

	"cloud.google.com/go/pubsub"
)

func TestRun(t *testing.T) {
	var message pubsub.Message
	Run(context.Background(), &message)
}

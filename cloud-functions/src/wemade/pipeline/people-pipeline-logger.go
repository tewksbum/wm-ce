// Package pipeline contains a Pub/Sub Cloud Function.
package pipeline

import (
	"context"
	"log"
)

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// LogPubSub logs the pubsub message
func LogPubSub(ctx context.Context, m PubSubMessage) error {
	log.Println(string(m.Data))
	return nil
}

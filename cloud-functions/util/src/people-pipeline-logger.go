// Package wmPsLogger contains a Pub/Sub Cloud Function.
package wmPsLogger

import (
	"context"
	"log"
)

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// HelloPubSub logs the pubsub message
func LogPubSub(ctx context.Context, m PubSubMessage) error {
	log.Println(string(m.Data))
	return nil
}

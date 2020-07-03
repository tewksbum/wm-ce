package pubber

import (
	"context"
	"os"

	"cloud.google.com/go/pubsub"
)

var ProjectID = os.Getenv("PROJECTID")
var ctx context.Context
var ps *pubsub.Client
var topicR *pubsub.Topic

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topicR = ps.Topic("wm-order-intake")
}

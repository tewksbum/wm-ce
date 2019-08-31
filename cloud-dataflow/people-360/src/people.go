package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/daidokoro/beam/sdks/go/pkg/beam/x/debug"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
)

var PubsubTopic = "pipeline-output-dev"
var OutputTopic = "people360-output-dev"
var ProjectID = "wemade-core"
func main() {
	ctx := context.Background()

	// If beamx or Go flags are used, flags must be parsed first.
	flag.Parse()

	log.Infof(ctx, "Starting pipeline")

	log.Infof(ctx, "input project %v pubsub topic %v", ProjectID, PubsubTopic)
	log.Infof(ctx, "output project %v pubsub topic %v", ProjectID, OutputTopic)
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	buildPipeline(ctx, s)

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}

func buildPipeline(ctx context.Context, s beam.Scope) {
	// nothing -> PCollection<Painting>
	pubsubOptions := pubsubio.ReadOptions{WithAttributes: true}
	//pubsubOptions := pubsubio.ReadOptions{}
	messages := pubsubio.Read(s, ProjectID, PubsubTopic, &pubsubOptions)

	// step 2: use matchkey to do lookups
	records := beam.ParDo(s, func(message *pb.PubsubMessage, emit func(pb.PubsubMessage)) {
		emit(message)
	}, messages)

	// step 3: store the sets?
	sets := beam.ParDo(s, func(message *pb.PubsubMessage, emit func(pb.PubsubMessage)) {
		emit(message)
	}, messages)

	// step 4: datastore IO to store the sets

	// step 5: PubSubIO to pub for next step

}

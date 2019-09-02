package main

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/daidokoro/beam/sdks/go/pkg/beam/x/debug"
)

var PubsubTopic = "streamer-output-dev"
var OutputTopic = "pipeline-output-dev"
var ProjectID = "wemade-core"
var BucketData = "wemade-ai-platform"

type InputRecord struct {
	Columns []struct {
		ERR struct {
			Address1        int `json:"Address1"`
			Address2        int `json:"Address2"`
			Age             int `json:"Age"`
			Birthday        int `json:"Birthday"`
			City            int `json:"City"`
			Country         int `json:"Country"`
			County          int `json:"County"`
			Email           int `json:"Email"`
			FirstName       int `json:"FirstName"`
			FullName        int `json:"FullName"`
			Gender          int `json:"Gender"`
			LastName        int `json:"LastName"`
			MiddleName      int `json:"MiddleName"`
			ParentEmail     int `json:"ParentEmail"`
			ParentFirstName int `json:"ParentFirstName"`
			ParentLastName  int `json:"ParentLastName"`
			ParentName      int `json:"ParentName"`
			Phone           int `json:"Phone"`
			State           int `json:"State"`
			Suffix          int `json:"Suffix"`
			ZipCode         int `json:"ZipCode"`
			TrustedID       int `json:"TrustedID"`
			Title           int `json:"Title"`
		} `json:"ERR"`
		NER struct {
			FAC       float64 `json:"FAC"`
			GPE       float64 `json:"GPE"`
			LOC       float64 `json:"LOC"`
			NORP      float64 `json:"NORP"`
			ORG       float64 `json:"ORG"`
			PERSON    float64 `json:"PERSON"`
			PRODUCT   float64 `json:"PRODUCT"`
			EVENT     float64 `json:"EVENT"`
			WORKOFART float64 `json:"WORK_OF_ART"`
			LAW       float64 `json:"LAW"`
			LANGUAGE  float64 `json:"LANGUAGE"`
			DATE      float64 `json:"DATE"`
			TIME      float64 `json:"TIME"`
			PERCENT   float64 `json:"PERCENT"`
			MONEY     float64 `json:"MONEY"`
			QUANTITY  float64 `json:"QUANTITY"`
			ORDINAL   float64 `json:"ORDINAL"`
			CARDINAL  float64 `json:"CARDINAL"`
		} `json:"NER"`
		Name     string `json:"Name"`
		Value    string `json:"Value"`
		MatchKey string `json:"MK"`
	} `json:"Columns"`
	Owner     int    `json:"Owner"`
	Request   string `json:"Request"`
	Row       int    `json:"Row"`
	Source    string `json:"Source"`
	TimeStamp string `json:"TimeStamp"`
}

func main() {
	ctx := context.Background()

	// If beamx or Go flags are used, flags must be parsed first.
	flag.Parse()

	// beam.Init() is an initialization hook that must called on startup. On
	// distributed runners, it is used to intercept control.
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

	debug.Print(s, messages)
	//log.Infof(ctx, "PCollection size is %v", stats.Count(s, records))

	//pubsubio.Write(s, ProjectID, OutputTopic, records)

}

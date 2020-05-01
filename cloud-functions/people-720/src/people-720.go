package people720

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"github.com/gomodule/redigo/redis"
)

var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"
var DSKindSet = os.Getenv("DSKINDSET")
var DSKindGolden = os.Getenv("DSKINDGOLDEN")
var DSKindFiber = os.Getenv("DSKINDFIBER")

var cfName = os.Getenv("FUNCTION_NAME")

var ctx context.Context
var ds *datastore.Client
var fs *datastore.Client
var ps *pubsub.Client
var msp *redis.Pool
var topic *pubsub.Topic
var ready *pubsub.Topic

var topicR *pubsub.Topic

func init() {
	ctx = context.Background()
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
	msp = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", os.Getenv("MEMSTORE")) },
	}
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(os.Getenv("PSOUTPUT"))
	ready = ps.Topic(os.Getenv("PSREADY"))
	topicR = ps.Topic(os.Getenv("PSREPORT"))
	ready.PublishSettings.DelayThreshold = 120 * time.Second
}

func People720(ctx context.Context, m PubSubMessage) error {
	var input FileComplete
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	cleanupKey := []string{input.EventID, "cleanup"}
	if GetRedisIntValue(cleanupKey) == 1 { // already processed
		return nil
	}
	SetRedisTempKey(cleanupKey)

	report0 := FileReport{
		ID:          input.EventID,
		StatusLabel: "set verification started",
		StatusBy:    cfName,
		StatusTime:  time.Now(),
	}
	publishReport(&report0, cfName)

	var sets []PeopleSetDS
	ownerNS := strings.ToLower(fmt.Sprintf("%v-%v", Env, input.OwnerID))
	if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKindSet).Namespace(ownerNS).Filter("eventid =", input.EventID), &sets); err != nil {
		log.Fatalf("Error querying sets: %v", err)
		return nil
	}

	var reprocessFibers []string
	for _, set := range sets {
		for _, search := range set.Search {
			msKey := []string{input.EventID, "cleanup", search}
			setKeys := GetRedisStringsValue(msKey)
			if !Contains(setKeys, set.ID.Name) {
				setKeys = append(setKeys, set.ID.Name)
				SetRedisTempKeyWithValue(msKey, strings.Join(setKeys, ","))
			}
			if len(setKeys) > 1 {
				// same search mapped to more than 1 swet
				reprocessFibers = append(reprocessFibers, set.Fibers...)
			}
		}

	}
	sets = nil
	report := FileReport{
		ID: input.EventID,
		Counters: []ReportCounter{
			ReportCounter{
				Type:      "Fiber",
				Name:      "Reprocess",
				Count:     len(reprocessFibers),
				Increment: true,
			},
		},
	}
	publishReport(&report, cfName)
	var fiberKeys []*datastore.Key
	var fibers []PeopleFiberDS
	for _, fiber := range reprocessFibers {
		dsFiberGetKey := datastore.NameKey(DSKindFiber, fiber, nil)
		dsFiberGetKey.Namespace = ownerNS
		fiberKeys = append(fiberKeys, dsFiberGetKey)
		fibers = append(fibers, PeopleFiberDS{})
	}
	if len(fiberKeys) > 0 {
		if err := fs.GetMulti(ctx, fiberKeys, fibers); err != nil && err != datastore.ErrNoSuchEntity {
			log.Fatalf("Error fetching fibers ns %v kind %v, keys %v: %v,", ownerNS, DSKindFiber, fiberKeys, err)
		}
	}
	var outputFibers []PeopleFiberDS
	for _, fiber := range fibers {
		if fiber.EventID == input.EventID {
			outputFibers = append(outputFibers, fiber)
		}
	}

	log.Printf("total reprocess fiber count %v", len(outputFibers))

	for _, fiber := range outputFibers {
		var pubs []People360Input
		var output People360Input

		fiberType := fiber.FiberType
		if fiberType == "mar" { // force fiber type to avoid the wait logic in 360
			fiberType = "default"
		}
		output.Signature = Signature{
			OwnerID:   fiber.OwnerID,
			Source:    fiber.Source,
			EventID:   input.EventID,
			EventType: fiber.EventType,
			FiberType: fiberType,
			RecordID:  fiber.RecordID,
		}
		output.Passthrough = ConvertPassthrough360SliceToMap(fiber.Passthrough)
		output.MatchKeys = GetPeopleOutputFromFiber(&fiber)
		pubs = append(pubs, output)

		outputJSON, _ := json.Marshal(pubs)
		psresult := topic.Publish(ctx, &pubsub.Message{
			Data: outputJSON,
			Attributes: map[string]string{
				"type":   "people",
				"source": "cleanup",
			},
		})
		psid, err := psresult.Get(ctx)
		if err != nil {
			log.Fatalf("%v Could not pub to pubsub: %v", input.EventID, err)
		} else {
			log.Printf("%v pubbed fiber rerun as message id %v: %v", input.EventID, psid, string(outputJSON))
		}
	}

	prresult := ready.Publish(ctx, &pubsub.Message{
		Data: m.Data,
		Attributes: map[string]string{
			"type":   "people",
			"source": "ready",
		},
	})
	prid, err := prresult.Get(ctx)
	if err != nil {
		log.Fatalf("%v Could not pub ready to pubsub: %v", input.EventID, err)
	} else {
		log.Printf("%v pubbed ready as message id %v: %v", input.EventID, prid, string(m.Data))
	}

	return nil
}

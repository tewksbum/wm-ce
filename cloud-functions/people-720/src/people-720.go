package people720

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/datastore"
)

var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"
var DSKindSet = os.Getenv("DSKINDSET")
var DSKindGolden = os.Getenv("DSKINDGOLDEN")
var DSKindFiber = os.Getenv("DSKINDFIBER")

var ds *datastore.Client
var fs *datastore.Client

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
}

func People720(ctx context.Context, m PubSubMessage) error {
	var input FileComplete
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	var sets []PeopleSetDS
	OwnerNamespace := strings.ToLower(fmt.Sprintf("%v-%v", Env, input.OwnerID))
	if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKindSet).Namespace(OwnerNamespace).Filter("eventid =", input.EventID), &sets); err != nil {
		log.Fatalf("Error querying sets: %v", err)
		return nil
	}

	return nil
}

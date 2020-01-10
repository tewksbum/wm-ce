package segment

import (
	"context"
	"io/ioutil"
	"net/http"
	"os"

	"segment/db"
	"segment/utils"
	"segment/utils/logger"
	"segment/wemade"
	// "cloud.google.com/go/pubsub"
)

// Environment variables
var (
	projectID         = os.Getenv("PROJECTID")
	namespace         = os.Getenv("NAMESPACE")
	csqlRegion        = os.Getenv("CSQL_REGION")
	csqlInstanceID    = os.Getenv("CSQL_INSTANCEID")
	csqlUser          = os.Getenv("CSQL_USER")
	csqlPass          = os.Getenv("CSQL_PASS")
	csqlSchema        = os.Getenv("CSQL_SCHEMA")
	csqlCnn           = os.Getenv("CSQL_CNN")
	csqlDSN           = utils.TruncatingSprintf(csqlCnn, csqlUser, csqlPass, projectID, csqlRegion, csqlInstanceID, csqlSchema)
	PubSubTopicInput  = os.Getenv("PS_SWEEPER_INPUT")
	PubSubTopicOutput = os.Getenv("PS_SWEEEER_OUTPUT")
	// ps                *pubsub.Client
	// topicInput        *pubsub.Topic
	// topicOutput       *pubsub.Topic
)

// Return messages
const (
	successMsg       = "Record successfully processed"
	successReadMsg   = "Query successfully processed"
	successDeleteMsg = "Record successfully deleted"
)

// func init() {
// 	ctx := context.Background()
// 	ps, _ = pubsub.NewClient(ctx, projectID)
// 	topicInput = ps.Topic(PubSubTopicInput)
// 	topicOutput = ps.Topic(PubSubTopicOutput)
// 	logger.InfoFmt("Sweeper sub topic name: %v, ", topicInput)
// 	logger.InfoFmt("Sweeper pub topic name: %v, ", topicOutput)
// }

// Upsert api entry point for upserting (create|update) a resource
func Upsert(w http.ResponseWriter, r *http.Request) {
	// check if the method of the request is a POST
	if err := CheckAllowedMethod(w, r, "POST"); err != nil {
		errToHTTP(w, r, err)
		return
	}
	// Get and parse the object
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errToHTTP(w, r, err)
	}
	logger.DebugFmt("[API.Upsert] Start Build Record From Input")
	rec, err := wemade.BuildRecordFromInput(projectID, namespace, data, false)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	logger.DebugFmt("[API.Upsert] Finished Build Record From Input")
	// Set the CSQL env vars to the record's db options
	rec.SetCSQLConnStr(csqlCnn)
	rec.SetCSQLSchemaName(csqlSchema)
	// Write to db
	logger.DebugFmt("[API.Upsert] Start DB writing")
	updated, err := db.Write(projectID, csqlDSN, rec)
	logger.DebugFmt("[API.Upsert] Finished DB writing")
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// If all goes well...
	if !updated {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	logger.DebugFmt("[API.Upsert.OwnerDWH] Start Build Record From Input")
	rec, err = wemade.BuildRecordFromInput(projectID, namespace, data, true)
	logger.DebugFmt("[API.Upsert.OwnerDWH] Finished Build Record From Input")
	logger.DebugFmt("[API.Upsert.OwnerDWH] Start DB writing")
	db.Write(projectID, csqlDSN, rec)
	if err != nil {
		logger.ErrFmt("[API.Upsert.OwnerDWH.Error]: %v", err)
	}
	logger.DebugFmt("[API.Upsert.OwnerDWH] Finished DB writing")

	HTTPWriteOutput(w, apiOutput(true, successMsg))
}

// Read api entry point for getting a (list of) resource(s)
func Read(w http.ResponseWriter, r *http.Request) {
	// check if the method of the request is a POST
	if err := CheckAllowedMethod(w, r, "POST"); err != nil {
		errToHTTP(w, r, err)
		return
	}

	// Get and parse the object
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	rec, err := wemade.BuildRecordFromInput(projectID, namespace, data, false)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	logger.DebugFmt("record(s): %+v", rec)

	// Write to db
	or, err := db.Read(projectID, csqlDSN, rec)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// If all goes well...
	w.WriteHeader(http.StatusOK)
	HTTPWriteOutput(w, apiOutputWithRecords(true, successReadMsg, &or))
}

// SweepEntry the database
func SweepEntry(ctx context.Context, m wemade.PubSubMessage) error {
	// Does nothing right now
	return nil
}

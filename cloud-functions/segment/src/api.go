package segment

import (
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
	projectID       = os.Getenv("PROJECTID")
	namespace       = os.Getenv("NAMESPACE")
	csqlRegion      = os.Getenv("CSQL_REGION")
	csqlInstanceID  = os.Getenv("CSQL_INSTANCEID")
	csqlUser        = os.Getenv("CSQL_USER")
	csqlPass        = os.Getenv("CSQL_PASS")
	csqlSchema      = os.Getenv("CSQL_SCHEMA")
	csqlCnn         = os.Getenv("CSQL_CNN")
	csqlDSN         = utils.TruncatingSprintf(csqlCnn, csqlUser, csqlPass, projectID, csqlRegion, csqlInstanceID, csqlSchema)
	sweeperEndpoint = os.Getenv("SWEEPER_API_ENDPOINT")
	// PubSubTopicInput  = os.Getenv("SWEEPER_PS_INPUT")
	// PubSubTopicOutput = os.Getenv("SWEEEER_PS_OUTPUT")
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
	logger.DebugFmt("[Upsert] Start Build Record From Input")
	rec, err := wemade.BuildRecordFromInput(projectID, namespace, data, false)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	logger.DebugFmt("[Upsert] Finished Build Record From Input")
	// Set the CSQL env vars to the record's db options
	rec.SetCSQLConnStr(csqlCnn)
	rec.SetCSQLSchemaName(csqlSchema)
	// Write to db
	logger.DebugFmt("[Upsert] Start DB writing")
	updated, err := db.Write(projectID, csqlDSN, rec)
	logger.DebugFmt("[Upsert] Finished DB writing")
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

	if rec.GetWriteToOwner() {
		logger.DebugFmt("[Upsert.OwnerDWH] Start Build Record From Input")
		rec, err = wemade.BuildRecordFromInput(projectID, namespace, data, true)
		logger.DebugFmt("[Upsert.OwnerDWH] Finished Build Record From Input")
		logger.DebugFmt("[Upsert.OwnerDWH] Start DB writing")
		db.Write(projectID, csqlDSN, rec)
		if err != nil {
			logger.ErrFmt("[Upsert.OwnerDWH.Error]: %v", err)
		}
		logger.DebugFmt("[Upsert.OwnerDWH] Finished DB writing")
	}

	HTTPWriteOutput(w, apiOutput(true, successMsg))

	// input := map[string]string{
	// 	"accessKey":  rec.GetAccessKey(),
	// 	"entityType": rec.GetEntityType(),
	// }
	// jInput, err := json.Marshal(input)
	// if err != nil {
	// 	logger.ErrFmt("[Upsert.Sweeper.json.Marshall.Error]: %v", err)
	// }
	// req, err := http.NewRequest("POST", sweeperEndpoint, bytes.NewBuffer(jInput))
	// req.Header.Set("Content-Type", "application/json")
	// client := &http.Client{}
	// resp, err := client.Do(req)
	// if err != nil {
	// 	logger.ErrFmt("[Upsert.Sweeper.client.Do.Error]: %v", err)
	// }
	// defer resp.Body.Close()

	// decoder := json.NewDecoder(resp.Body)
	// var sr wemade.APIOutput
	// err = decoder.Decode(&sr)
	// if err != nil {
	// 	logger.ErrFmt("[Upsert.Sweeper.decode.Decode.Error]: %v", err)
	// }
	// if sr.Success != true {
	// 	logger.ErrFmt("[Upsert.Sweeper.not_successful.Error]: %v", err)
	// } else {
	// 	logger.DebugFmt("[Upsert.Sweeper.POST] success!")
	// }

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

	// Read from db
	or, err := db.Read(projectID, csqlDSN, rec)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// If all goes well...
	w.WriteHeader(http.StatusOK)
	HTTPWriteOutput(w, apiOutputWithRecords(true, successReadMsg, &or))
}

// Delete api entry point for deleting a resource
func Delete(w http.ResponseWriter, r *http.Request) {
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
	rec, err := wemade.BuildRecordFromInput(projectID, namespace, data, false)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	logger.DebugFmt("record(s): %+v", rec)

	// Delete from db
	err = db.Delete(projectID, csqlDSN, rec)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// If all goes well...
	w.WriteHeader(http.StatusOK)
	HTTPWriteOutput(w, apiOutput(true, successDeleteMsg))
}

// SweepExpiredSets api entry point for getting a (list of) resource(s)
func SweepExpiredSets(w http.ResponseWriter, r *http.Request) {
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

	input, err := wemade.BuildSweeperInputFromData(data)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}

	logger.DebugFmt("[SweepExpiredSets] Start DB works")
	if db.SweepExpiredSets(projectID, csqlDSN, input.EntityType, input.EntityBlacklist) != nil {
		errToHTTP(w, r, err)
		return
	}

	logger.DebugFmt("[SweepExpiredSets] finished DB works")

	// // If all goes well...
	w.WriteHeader(http.StatusOK)
	HTTPWriteOutput(w, apiOutput(true, successMsg))
}

// // SweepEntry the database
// func SweepEntry(ctx context.Context, m wemade.PubSubMessage) error {
// 	// Does nothing right now
// 	return
// }

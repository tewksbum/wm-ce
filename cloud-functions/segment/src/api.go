package segment

import (
	"io/ioutil"
	"net/http"
	"os"
	"segment/utils/logger"

	"segment/db"
	"segment/utils"
	"segment/wemade"
)

// Environment variables
var (
	projectID      = os.Getenv("PROJECTID")
	namespace      = os.Getenv("NAMESPACE")
	csqlRegion     = os.Getenv("CSQL_REGION")
	csqlInstanceID = os.Getenv("CSQL_INSTANCEID")
	csqlUser       = os.Getenv("CSQL_USER")
	csqlPass       = os.Getenv("CSQL_PASS")
	csqlSchema     = os.Getenv("CSQL_SCHEMA")
	csqlCnn        = os.Getenv("CSQL_CNN")
	csqlDSN        = utils.TruncatingSprintf(csqlCnn, csqlUser, csqlPass, projectID, csqlRegion, csqlInstanceID, csqlSchema)
)

// Return messages
const (
	successMsg       = "Record successfully processed"
	successReadMsg   = "Query successfully processed"
	successDeleteMsg = "Record successfully deleted"
)

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
	rec, err := wemade.BuildRecordFromInput(projectID, namespace, data, false)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// Set the CSQL env vars to the record's db options
	rec.SetCSQLConnStr(csqlCnn)
	rec.SetCSQLSchemaName(csqlSchema)
	// Write to db
	updated, err := db.Write(projectID, csqlDSN, rec)
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

	rec, err = wemade.BuildRecordFromInput(projectID, namespace, data, true)
	db.Write(projectID, csqlDSN, rec)
	if err != nil {
		logger.ErrFmt("[API.Upsert.MainOwnerCopy.Error]: %v", err)
	}

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

	// Write to db
	err = db.Delete(projectID, csqlDSN, rec)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// If all goes well...
	w.WriteHeader(http.StatusOK)
	HTTPWriteOutput(w, apiOutput(true, successDeleteMsg))
}

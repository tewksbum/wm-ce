package segment

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"segment/db"
	"segment/wemade"
)

var (
	projectID        string = os.Getenv("PROJECTID")
	namespace        string = os.Getenv("NAMESPACE")
	csqlRegion       string = os.Getenv("CSQL_REGION")
	csqlInstanceID   string = os.Getenv("CSQL_INSTANCEID")
	csqlUser         string = os.Getenv("CSQL_USER")
	csqlPass         string = os.Getenv("CSQL_PASS")
	csqlSchema       string = os.Getenv("CSQL_SCHEMA")
	csqlCnn          string = os.Getenv("CSQL_CNN")
	csqlDSN          string = fmt.Sprintf(csqlCnn, csqlUser, csqlPass, projectID, csqlRegion, csqlInstanceID, csqlSchema)
	successMsg       string = "Record successfully processed"
	successReadMsg   string = "Query successfully processed"
	successDeleteMsg string = "Record successfully deleted"
)

// Upsert api entry point for upserting (create|update) a resource
func Upsert(w http.ResponseWriter, r *http.Request) {
	// Local test variables
	// projectID = "wemade-core"
	// namespace = "wemade-dev"
	// csqlRegion = "us-central1"
	// csqlInstanceID = "wemade"
	// csqlSchema = "segment_dev"
	// csqlDSN := "segment:RLWOrYOntAINtRatioNtURaI@tcp(localhost:3307)/segment_dev?charset=utf8mb4,utf8&parseTime=true"

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
	rec, err := wemade.BuildRecordFromInput(projectID, namespace, data)
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
	fmt.Fprint(w, apiOutput(true, successMsg))
}

// Read api entry point for getting a (list of) resource(s)
func Read(w http.ResponseWriter, r *http.Request) {
	// // Local test variables
	// projectID = "wemade-core"
	// namespace = "wemade-dev"
	// csqlRegion = "us-central1"
	// csqlInstanceID = "wemade"
	// csqlSchema = "segment_dev"
	// csqlDSN := "segment:RLWOrYOntAINtRatioNtURaI@tcp(localhost:3307)/segment_dev?charset=utf8mb4,utf8&parseTime=true"

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
	rec, err := wemade.BuildRecordFromInput(projectID, namespace, data)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// logger.InfoFmt("record(s): %+v", rec)

	// Write to db
	or, err := db.Read(projectID, csqlDSN, rec)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// If all goes well...
	w.WriteHeader(http.StatusOK)
	// foo := apiOutputWithRecords(true, successMsg, &or)
	// logger.InfoFmt("POO!: %#v", foo)
	fmt.Fprint(w, apiOutputWithRecords(true, successReadMsg, &or))
}

// Delete api entry point for deleting a resource
func Delete(w http.ResponseWriter, r *http.Request) {
	// // Local test variables
	// projectID = "wemade-core"
	// namespace = "wemade-dev"
	// csqlRegion = "us-central1"
	// csqlInstanceID = "wemade"
	// csqlSchema = "segment_dev"
	// csqlDSN := "segment:RLWOrYOntAINtRatioNtURaI@tcp(localhost:3307)/segment_dev?charset=utf8mb4,utf8&parseTime=true"

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
	rec, err := wemade.BuildRecordFromInput(projectID, namespace, data)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// logger.InfoFmt("record(s): %+v", rec)

	// Write to db
	err = db.Delete(projectID, csqlDSN, rec)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// If all goes well...
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, apiOutput(true, successDeleteMsg))
}

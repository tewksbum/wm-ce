package segment

import (
	"io/ioutil"
	"net/http"
	"os"
	"segment/utils/logger"

	"segment/wemade"
)

// Environment variables
var (
	projectID = os.Getenv("PROJECTID")
	namespace = os.Getenv("NAMESPACE")
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
	rec, err := wemade.UpsertCustomer(projectID, namespace, data)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	logger.InfoFmt("rec: %#v", rec)
	// If all goes well...
	if !rec.Updated {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	HTTPWriteOutput(w, apiOutputWithResult(true, successMsg, rec.AccessKey))
}

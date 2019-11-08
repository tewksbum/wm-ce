package segment

import (
	"fmt"
	"net/http"
	"os"

	"segment/db"
	"segment/wemade"
)

var (
	projectID      string = os.Getenv("PROJECTID")
	namespace      string = os.Getenv("NAMESPACE")
	csqlRegion     string = os.Getenv("CSQL_REGION")
	csqlInstanceID string = os.Getenv("CSQL_INSTANCEID")
	csqlCnn        string = os.Getenv("CSQLCNN")
	csqlDSN        string = fmt.Sprintf(csqlCnn, projectID, csqlRegion, csqlInstanceID)
	successMsg     string = "Record successfully processed"
)

// Upsert the api entrypoint main func
func Upsert(w http.ResponseWriter, r *http.Request) {
	// TODO: remove these assignments before merging to dev
	// projectID = "wemade-core"
	// namespace = "wemade.streamer-api.dev"
	// csqlRegion = "us-central1"
	// csqlInstanceID = "wemade"
	// // csqlCnn = "segment:RLWOrYOntAINtRatioNtURaI@unix(/cloudsql/%s:%s:%s)/segment?charset=utf8mb4,utf8&parseTime=true"
	// csqlDSN := "segment:RLWOrYOntAINtRatioNtURaI@tcp(localhost:3307)/segment?charset=utf8mb4,utf8&parseTime=true"

	// Set returning headers
	if err := setHeaders(w, r); err != nil {
		errToHTTP(w, r, err)
		return
	}
	// Get and parse the object
	o, err := wemade.DecodeAPIInput(projectID, namespace, r.Body)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// logger.InfoFmt("output: %+v", o)
	// Write to db
	err = db.Write(projectID, csqlDSN, o)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// If all goes well...
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, apiOutput(true, successMsg))
}

// Read the api entrypoint main func
func Read(w http.ResponseWriter, r *http.Request) {
	// TODO: remove these assignments before merging to dev
	// projectID = "wemade-core"
	// namespace = "wemade.streamer-api.dev"
	// csqlRegion = "us-central1"
	// csqlInstanceID = "wemade"
	// // csqlCnn = "segment:RLWOrYOntAINtRatioNtURaI@unix(/cloudsql/%s:%s:%s)/segment?charset=utf8mb4,utf8&parseTime=true"
	// csqlDSN := "segment:RLWOrYOntAINtRatioNtURaI@tcp(localhost:3307)/segment?charset=utf8mb4,utf8&parseTime=true"

	// Set returning headers
	if err := setHeaders(w, r); err != nil {
		errToHTTP(w, r, err)
		return
	}
	// Get and parse the object
	o, err := wemade.DecodeAPIQuery(projectID, namespace, r.Body)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// logger.InfoFmt("output: %+v", o)
	// Write to db
	err = db.Read(projectID, csqlDSN, o)
	if err != nil {
		errToHTTP(w, r, err)
		return
	}
	// If all goes well...
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, apiOutput(true, successMsg))
}

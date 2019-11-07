package segment

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"segment/db"
	"segment/utils/logger"
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
	projectID = "wemade-core"
	namespace = "wemade.streamer-api.dev"
	csqlRegion = "us-central1"
	csqlInstanceID = "wemade"
	// csqlCnn = "segment:RLWOrYOntAINtRatioNtURaI@unix(/cloudsql/%s:%s:%s)/segment?charset=utf8mb4,utf8&parseTime=true"
	csqlDSN := "segment:RLWOrYOntAINtRatioNtURaI@tcp(localhost:3307)/segment?charset=utf8mb4,utf8&parseTime=true"

	if err := setHeaders(w, r); err != nil {
		// pack these lines into a API err func
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, apiOutput(false, wemade.ErrStatusNoContent))
		return
	}
	o, err := wemade.DecodeAPIInput(projectID, namespace, r.Body)
	if err != nil {
		strErr := err.Error()
		switch strErr {
		case wemade.ErrAccountNotEnabled:
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, apiOutput(false, fmt.Sprintf("%s, -11", strErr)))
		case wemade.ErrInvalidAccessKey:
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, apiOutput(false, fmt.Sprintf("%s, -10", strErr)))
		default:
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, apiOutput(false, fmt.Sprintf("%s, -2", strErr)))
		}
		return
	}
	logger.InfoFmt("output: %+v", o)
	err = db.Write(projectID, csqlDSN, o)
	// err = bq.Write(projectID, o.GetStrOwnerID(), o.GetEntityType(), o.GetBQOptions(), o)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, apiOutput(false, fmt.Sprintf("%s, -3", err.Error())))
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, apiOutput(true, successMsg))
}

// funcs
func apiOutput(success bool, msg string, args ...interface{}) []byte {
	fmsg := fmt.Sprintf(msg, args...)
	o, _ := json.Marshal(wemade.APIOutput{Success: success, Message: fmsg})
	return o
}

// SetHeaders sets the headers for the response
func setHeaders(w http.ResponseWriter, r *http.Request) error {
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "3600")
		w.WriteHeader(http.StatusNoContent)
		return logger.ErrStr(wemade.ErrStatusNoContent)
	}
	// Set CORS headers for the main request.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	return nil
}

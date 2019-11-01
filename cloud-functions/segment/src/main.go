package main

import (
	"fmt"
	"net/http"
	"os"
	"segment/bq"
	"segment/utils/logger"
	"segment/wemade"
	"strings"
)

// projectID - the project ID
var (
	projectID = os.Getenv("PROJECTID")
	namespace = os.Getenv("WMDSACCESS")
)

// API the api entrypoint main func
func API(w http.ResponseWriter, r *http.Request) {
	// TODO: remove these assignments before mergeging to dev
	projectID = "wemade-core"
	namespace = "wemade.streamer-api.dev"

	if err := wemade.SetHeaders(w, r); err != nil {
		// pack these lines into a API err func
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -1\"}")
		return
	}
	o, err := wemade.DecodeAPIInput(projectID, namespace, r.Body)
	if err != nil {
		if strings.Contains(err.Error(), "Invalid access key") {
			w.WriteHeader(http.StatusUnauthorized)
		}
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -2\"}")
		return
	}
	// logger.InfoFmt("output: %+v", o)
	err = bq.Write(projectID, o.GetStrCustomerID(), o.GetEntityType(), o)
	if err != nil {
		// logger.Err(err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -3\"}")
		return
	}
	fmt.Fprint(w, `{success: true, message: "Record inserted"}`)
}

// func apiOutput(success bool, message string) wemade.APIOutput {
// 	wemade.APIOutput
// }

func main() {
	logger.Info("Load this as a cloud function and use the API func as entry point")
}

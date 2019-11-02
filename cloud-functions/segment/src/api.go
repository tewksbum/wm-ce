package segment

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"segment/bq"
	"segment/utils/logger"
	"segment/wemade"
)

// projectID - the project ID
var (
	projectID  string = os.Getenv("PROJECTID")
	namespace  string = os.Getenv("NAMESPACE")
	successMsg string = "Record successfully processed"
)

// API the api entrypoint main func
func API(w http.ResponseWriter, r *http.Request) {
	// TODO: remove these assignments before mergeging to dev
	// projectID = "wemade-core"
	// namespace = "wemade.streamer-api.dev"

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
	// logger.InfoFmt("output: %+v", o)
	err = bq.Write(projectID, o.GetStrCustomerID(), o.GetEntityType(), o.GetBQOptions(), o)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, apiOutput(false, fmt.Sprintf("%s, -3", err.Error())))
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, apiOutput(true, successMsg))
}

func apiOutput(success bool, msg string, args ...interface{}) string {
	fmsg := fmt.Sprintf(msg, args...)
	o, _ := json.Marshal(wemade.APIOutput{Success: success, Message: fmsg})
	return string(o)
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

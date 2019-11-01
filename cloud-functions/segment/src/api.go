package segment

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"segment/bq"
	"segment/wemade"
)

// projectID - the project ID
var (
	projectID = os.Getenv("PROJECTID")
	namespace = os.Getenv("NAMESPACE")
)

// API the api entrypoint main func
func API(w http.ResponseWriter, r *http.Request) {
	// TODO: remove these assignments before mergeging to dev
	// projectID = "wemade-core"
	// namespace = "wemade.streamer-api.dev"

	if err := wemade.SetHeaders(w, r); err != nil {
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
	err = bq.Write(projectID, o.GetStrCustomerID(), o.GetEntityType(), o)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, apiOutput(false, fmt.Sprintf("%s, -3", err.Error())))
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, apiOutput(true, "Record successfully processed"))
}

func apiOutput(success bool, msg string, args ...interface{}) []byte {
	fmsg := fmt.Sprintf(msg, args...)
	o, _ := json.Marshal(wemade.APIOutput{Success: success, Message: fmsg})
	return o
}

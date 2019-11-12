package segment

import (
	"encoding/json"
	"fmt"
	"net/http"

	"segment/utils/logger"
	"segment/wemade"
)

// ErrToHTTP sets the thrown err into the http response
func errToHTTP(w http.ResponseWriter, r *http.Request, err error) error {
	strErr := err.Error()
	switch strErr {
	case wemade.ErrStatusNoContent:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, apiOutput(false, wemade.ErrStatusNoContent))
	case wemade.ErrAccountNotEnabled:
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, apiOutput(false, "%s, -11", strErr))
		return err
	case wemade.ErrInvalidAccessKey:
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, apiOutput(false, "%s, -10", strErr))
		return err
	default:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, apiOutput(false, "%s, -2", strErr))
		return err
	}
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprint(w, apiOutput(false, "%s, -3", err.Error()))
	return err
}

// ApiOutput builds a json with the response for the client
func apiOutput(success bool, msg string, args ...interface{}) string {
	fmsg := fmt.Sprintf(msg, args...)
	o, _ := json.Marshal(wemade.APIOutput{Success: success, Message: fmsg})
	return string(o)
}

// ApiOutput builds a json with the response for the client
func apiOutputWithRecords(success bool, msg string, records wemade.OutputRecords) string {
	o, _ := json.Marshal(wemade.APIOutput{
		Success: success, Message: msg,
		Records: records,
	})
	return string(o)
}

// CheckAllowedMethod check if the method is not a OPTIONS - reasons? ask Jie.
func CheckAllowedMethod(w http.ResponseWriter, r *http.Request, method string) error {
	logger.InfoFmt("Client IP: [%s] - UserAgent: [%s] - Headers: %q", r.RemoteAddr, r.UserAgent(), r.Header)
	if r.Method == http.MethodOptions || r.Method != method {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", method)
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "3600")
		w.WriteHeader(http.StatusNoContent)
		return logger.Err(fmt.Errorf(wemade.ErrStatusNoContent, r.Method))
	}
	// Set CORS headers for the request.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	return nil
}

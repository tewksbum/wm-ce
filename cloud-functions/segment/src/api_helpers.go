package segment

import (
	"encoding/json"
	"fmt"
	"net/http"

	"segment/utils/logger"
	"segment/wemade"
)

func errToHTTP(w http.ResponseWriter, r *http.Request, err error) error {
	strErr := err.Error()
	switch strErr {
	case wemade.ErrStatusNoContent:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, apiOutput(false, wemade.ErrStatusNoContent))
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
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprint(w, apiOutput(false, fmt.Sprintf("%s, -3", err.Error())))
	return err
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

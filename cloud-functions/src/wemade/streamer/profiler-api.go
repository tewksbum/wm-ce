// Package streamer contains a series of cloud functions for streamer
package streamer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"reflect"

	"cloud.google.com/go/datastore"
)

// Profile is a dynamic map of the profile results
type Profile map[string]interface {
}

// NameSpaceStreamer is the namespace of the streamer
const NameSpaceStreamer = "wemade.streamer"

// NameSpaceProfiler is the namespace of the profiler
const NameSpaceProfiler = "wemade.profiler"

// ProfilerAPI is the API body
func ProfilerAPI(w http.ResponseWriter, r *http.Request) {
	var d struct {
		AccessKey string `json:"accessKey"`
		RequestID string `json:"requestId"`
	}

	ctx := context.Background()

	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "3600")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Set CORS headers for the main request.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{success: false, message: \"Error decoding request\"}")
		return
	}
	requestID := d.RequestID
	log.Printf("requesting status on request id %v with access key %v", requestID, d.AccessKey)

	// validate key
	dsClient, err := datastore.NewClient(ctx, ProjectID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error accessing datastore: %v", err)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -1\"}")
		return
	}
	var entities []Customer
	query := datastore.NewQuery("Customer").Namespace(NameSpaceStreamer)
	query.Filter("AccessKey =", d.AccessKey).Limit(1)

	if _, err := dsClient.GetAll(ctx, query, &entities); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error querying customer: %v", err)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -2\"}")
		return
	}
	if len(entities) == 0 {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{success: false, message: \"Invalid access key, -10\"}")
		return
	}

	customer := entities[0]
	if customer.Enabled == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{success: false, message: \"Account is not enabled, -11\"}")
		return
	}

	var profiles []Profile
	profile := datastore.NewQuery(requestID).Namespace(NameSpaceProfiler)
	_, err2 := dsClient.GetAll(ctx, profile, &profiles)
	if err2 != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error getting profile keys: %v", err2)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -21\"}")
		return
	}
	if len(profiles) == 0 {
		w.WriteHeader(http.StatusNoContent)
		fmt.Fprint(w, "{success: false, message: \"RequestID not found, -22\"}")
		return
	}
	for k, v := range profiles[0] {

		log.Printf("K: %v, V: %v, T: %v", k, v, reflect.TypeOf(v))
		if reflect.TypeOf(v).Kind() == reflect.Float64 {
			if math.IsNaN(v.(float64)) {
				profiles[0][k] = nil
			}
		}

	}

	//var re map[string]interface

	re := make(map[string]interface{})
	re["success"] = true
	re["requestId"] = requestID
	re["profile"] = profiles[0]

	jsonString, err := json.Marshal(re)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error converting profiler result to json: %v", err)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -22\"}")
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, string(jsonString))
	return
}

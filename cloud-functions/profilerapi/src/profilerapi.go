// Package profilerapi contains a series of cloud functions for streamer
package profilerapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"reflect"

	"cloud.google.com/go/datastore"
)

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")

// NameSpaceStreamer is the env var for datastore name space of streamer
var NameSpaceStreamer = os.Getenv("NSSTREAMER")

// NameSpaceProfiler is the env var for datastore name space of profiler
var NameSpaceProfiler = os.Getenv("NSPROFILER")

// Customer contains Customer fields
type Customer struct {
	Name      string
	AccessKey string
	Enabled   bool
	Key       *datastore.Key `datastore:"__key__"`
}

// Profile is a dynamic map of the profile results
type Profile map[string]interface {
}

// Load a datastore field
func (d *Profile) Load(props []datastore.Property) error {
	// Note: you might want to clear current values from the map or create a new map
	for _, p := range props {
		(*d)[p.Name] = p.Value
	}
	return nil
}

// Save a datastore field
func (d *Profile) Save() (props []datastore.Property, err error) {
	for k, v := range *d {
		props = append(props, datastore.Property{Name: k, Value: v})
	}
	return
}

// Main is the API body
func Main(w http.ResponseWriter, r *http.Request) {
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

// Package purgeutil is a CF that performs purging on DS/BQ
package purgeutil

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
)

// ProjectID is the env var of project id
var pid = os.Getenv("PROJECTID")
var env = os.Getenv("ENVIRONMENT")
var uid = os.Getenv("CLIENTID")
var pwd = os.Getenv("CLIENTSECRET")

// global vars
var ctx context.Context
var ds *datastore.Client
var bq *bigquery.Client

var allowedOperations = map[string]map[string]map[string]bool{
	"datastore": map[string]map[string]bool{
		"namespace": map[string]bool{"delete": true},
		"kind":      map[string]bool{"delete": true},
	},
	"bigquery": map[string]map[string]bool{
		"dataset": map[string]bool{"delete": true},
	},
}

func init() {
	ctx = context.Background()
	ds, _ = datastore.NewClient(ctx, pid)
	bq, _ = bigquery.NewClient(ctx, pid)

	log.Printf("init completed")
}

// ProcessRequest Receives a http event request
func ProcessRequest(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ClientID        string `json:"clientId"`
		ClientSecret    string `json:"clientSecret"`
		TargetType      string `json:"targetType"`  // datastore
		TargetLevel     string `json:"targetLevel"` // datastore
		Operation       string `json:"operation"`
		TargetSelection string `json:"targetSelection"` //regex
	}

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

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Error decoding request\"}")
		log.Fatalf("error decoding request %v", err)
		return
	}

	if input.ClientID != uid || input.ClientSecret != pwd {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Authentication failed\"}")
		return
	}

	if levels, ok := allowedOperations[strings.ToLower(input.TargetType)]; ok { // check type exists
		if operations, ok := levels[strings.ToLower(input.TargetLevel)]; ok { // check level exists
			if _, ok := operations[strings.ToLower(input.Operation)]; ok { // check op exists
				// let's do some deletes
				if strings.EqualFold(input.TargetType, "datastore") {
					purgeDataStore(w, strings.ToLower(input.TargetLevel), input.TargetSelection)
				}
				return
			} else {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "{\"success\": false, \"message\": \"requested operation on type %v level %v op %v is not allowed\"}", input.TargetType, input.TargetLevel, input.Operation)
				return
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "{\"success\": false, \"message\": \"requested operation on type %v level %v is not allowed\"}", input.TargetType, input.TargetLevel)
			return
		}
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "{\"success\": false, \"message\": \"requested operation on type %v is not allowed\"}", input.TargetType)
		return
	}

}

func purgeDataStore(w http.ResponseWriter, level string, filter string) {
	switch level {
	case "namespace":
		query := datastore.NewQuery("__namespace__").KeysOnly()
		keys, err := ds.GetAll(ctx, query, nil)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "error %v", err)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "\t%v keys\n", len(keys))
		for _, k := range keys {
			fmt.Fprintf(w, "\t%v\n", k.Namespace)
		}
	case "kind":
		query := datastore.NewQuery("__kind__").KeysOnly()
		keys, err := ds.GetAll(ctx, query, nil)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "error %v", err)
			return
		}
		w.WriteHeader(http.StatusOK)
		for _, k := range keys {
			fmt.Fprintf(w, "\t%v", k.Name)
		}
	}
	return
}

func purgeBigQuery(level string, filter string) {

}

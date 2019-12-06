// Package purgeutil is a CF that performs purging on DS/BQ
package purgeutil

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
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
		"table":   map[string]bool{"delete": true},
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
		ClientID           string `json:"clientId"`
		ClientSecret       string `json:"clientSecret"`
		TargetType         string `json:"targetType"`  // datastore
		TargetLevel        string `json:"targetLevel"` // datastore
		Operation          string `json:"operation"`
		TargetSelection    string `json:"targetSelection"`    //regex
		TargetSubSelection string `json:"targetSubSelection"` //regex
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
		log.Fatalf("auth failure")
		return
	}

	if levels, ok := allowedOperations[strings.ToLower(input.TargetType)]; ok { // check type exists
		if operations, ok := levels[strings.ToLower(input.TargetLevel)]; ok { // check level exists
			if _, ok := operations[strings.ToLower(input.Operation)]; ok { // check op exists
				// let's do some deletes
				log.Printf("processing request %v", input)
				w.WriteHeader(http.StatusOK)

				// if level is kind, then must specify TargetSelection

				if strings.EqualFold(input.TargetType, "datastore") {
					purgeDataStore(w, strings.ToLower(input.TargetLevel), input.TargetSelection, input.TargetSubSelection)
				}
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

func purgeDataStore(w http.ResponseWriter, level string, filter string, subfilter string) {
	switch level {
	case "namespace":
		query := datastore.NewQuery("__namespace__").KeysOnly()
		namespaces, err := ds.GetAll(ctx, query, nil)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "error %v", err)
			return
		}
		fmt.Fprintf(w, "\t%v keys\n", len(namespaces))
		regex, _ := regexp.Compile("^" + env + filter)
		for _, n := range namespaces {
			if regex.MatchString(n.Name) {
				fmt.Fprintf(w, "NameSpace: %v\n", n.Name)
				query := datastore.NewQuery("__kind__").Namespace(n.Name).KeysOnly()

				kinds, err := ds.GetAll(ctx, query, nil)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintf(w, "error %v", err)
					return
				}
				if len(subfilter) > 0 {
					regex, _ := regexp.Compile(subfilter)
					for _, k := range kinds {
						if regex.MatchString(k.Name) {
							fmt.Fprintf(w, "Deleting Kind: %v", k.Name)
							deleteDS(n.Name, k.Name)
						}
					}
				} else {
					for _, k := range kinds {
						fmt.Fprintf(w, "Deleting Kind: %v", k.Name)
						deleteDS(n.Name, k.Name)
					}
				}
			}
		}
	case "kind":
		query := datastore.NewQuery("__kind__").Namespace(filter).KeysOnly()
		keys, err := ds.GetAll(ctx, query, nil)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "error %v", err)
			return
		}
		if len(subfilter) > 0 {

			regex, _ := regexp.Compile(subfilter)
			for _, k := range keys {
				if regex.MatchString(k.Name) {
					fmt.Fprintf(w, "Deleting Kind: %v", k.Name)
					deleteDS(filter, k.Name)
				}
			}
		} else {
			for _, k := range keys {
				fmt.Fprintf(w, "Deleting Kind: %v", k.Name)
				deleteDS(filter, k.Name)
			}
		}
	}
	return
}

func purgeBigQuery(level string, filter string) {

}

func deleteDS(ns string, kind string) {
	query := datastore.NewQuery(kind).Namespace(ns).KeysOnly()
	keys, err := ds.GetAll(ctx, query, nil)
	log.Printf("Deleting %v records", len(keys))
}

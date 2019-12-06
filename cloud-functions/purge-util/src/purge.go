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

				// if level is kind, then must specify TargetSelection
				if strings.EqualFold(input.TargetLevel, "kind") && len(input.TargetSelection) == 0 {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprintf(w, "{\"success\": false, \"message\": \"requested operation on type %v level %v op %v is not allowed without targetSelection\"}", input.TargetType, input.TargetLevel, input.Operation)
					return
				}

				if strings.EqualFold(input.TargetType, "datastore") {
					n, k, e := purgeDataStore(strings.ToLower(input.TargetLevel), input.TargetSelection, input.TargetSubSelection)
					w.WriteHeader(http.StatusOK)
					fmt.Fprintf(w, "{\"success\": true, \"message\": \"deleted %v namespaces, %v kinds, %v entities\"}", n, k, e)
					return
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

func purgeDataStore(level string, filter string, subfilter string) (int, int, int) {
	countNS := 0
	countKind := 0
	countEntity := 0
	switch level {
	case "namespace":
		query := datastore.NewQuery("__namespace__").KeysOnly()
		namespaces, err := ds.GetAll(ctx, query, nil)
		if err != nil {
			return countNS, countKind, countEntity
		}
		rens, _ := regexp.Compile("^" + env + "-" + filter)
		for _, n := range namespaces {
			if rens.MatchString(n.Name) {
				query := datastore.NewQuery("__kind__").Namespace(n.Name).KeysOnly()

				kinds, err := ds.GetAll(ctx, query, nil)
				if err != nil {
					return countNS, countKind, countEntity
				}
				if len(subfilter) > 0 {
					rekind, _ := regexp.Compile(subfilter)
					for _, k := range kinds {
						if rekind.MatchString(k.Name) {
							countKind++
							countEntity += deleteDS(n.Name, k.Name)
						}
					}
				} else {
					for _, k := range kinds {
						countKind++
						countEntity += deleteDS(n.Name, k.Name)
					}
				}
				countNS++
			}
		}
	case "kind":
		query := datastore.NewQuery("__kind__").Namespace(filter).KeysOnly()
		keys, err := ds.GetAll(ctx, query, nil)
		if err != nil {
			return countNS, countKind, countEntity
		}
		if len(subfilter) > 0 {

			regex, _ := regexp.Compile(subfilter)
			for _, k := range keys {
				if regex.MatchString(k.Name) {
					countKind++
					countEntity += deleteDS(filter, k.Name)
				}
			}
		} else {
			for _, k := range keys {
				countKind++
				countEntity += deleteDS(filter, k.Name)
			}
		}
		countNS++
	}
	return countNS, countKind, countEntity
}

func purgeBigQuery(level string, filter string) {

}

func deleteDS(ns string, kind string) int {
	if strings.HasPrefix(kind, "_") { // statistics entities, cannot delete them without error
		return 0
	}
	query := datastore.NewQuery(kind).Namespace(ns).KeysOnly()
	keys, _ := ds.GetAll(ctx, query, nil)

	l := len(keys) / 500
	if l%500 == 0 {
		l++
	}
	log.Printf("Deleting %v records from ns %v, kind %v", len(keys), ns, kind)
	for r := 0; r < l; r++ {
		s := r * 500
		e := s + 499
		if e > len(keys)-1 {
			e = len(keys) - 1
		}
		err := ds.DeleteMulti(ctx, keys[s:e])
		if err != nil {
			log.Printf("Error Deleting records from ns %v, kind %v, err %v", ns, kind, err)
		}
	}

	return len(keys)
}
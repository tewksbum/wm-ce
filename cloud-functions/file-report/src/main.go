package filereport

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/datastore"

	"github.com/olivere/elastic/v7"

	secretmanager "cloud.google.com/go/secretmanager/apiv1beta1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1beta1"
)

var test FileReport
var ctx context.Context
var esClient *elastic.Client
var dsClient *datastore.Client
var fsClient *datastore.Client
var smClient *secretmanager.Client
var esSecret elasticSecret

func init() {
	var err error
	ctx = context.Background()

	dsClient, err = datastore.NewClient(ctx, os.Getenv("PROJECTID"))
	fsClient, err = datastore.NewClient(ctx, os.Getenv("DSPROJECTID"))

	smClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to setup secrets manager client: %v", err)
	}

	secretReq := &secretmanagerpb.AccessSecretVersionRequest{
		Name: os.Getenv("ELASTIC_SECRET"),
	}
	secretresult, err := smClient.AccessSecretVersion(ctx, secretReq)
	if err != nil {
		log.Fatalf("failed to get secret: %v", err)
	}
	secretsData1 := secretresult.Payload.Data
	if err := json.Unmarshal(secretsData1, &esSecret); err != nil {
		log.Fatalf("error decoding secrets %v", err)
		return
	}

	esClient, err = elastic.NewClient(
		elastic.SetURL(esSecret.URL),
		elastic.SetBasicAuth(esSecret.User, esSecret.Password),
		elastic.SetSniff(false),
	)
	if err != nil {
		log.Panicf("Error creating elastic client %v", err)
	}
}

// ProcessUpdate processes update from pubsub into elastic
func ProcessUpdate(ctx context.Context, m psMessage) error {
	var input FileReport
	log.Printf("received message %+v", m)
	err := json.Unmarshal(m.Data, &input)
	if err != nil {
		log.Printf("ERROR unable to unmarshal request %v", err)
		return nil
	}

	if source, ok := m.Attributes["source"]; ok {
		if strings.Contains(source, "file-api") {
			// initialize arrays and run insert
			input.Errors = []ReportError{}
			input.Warnings = []ReportError{}
			input.Counts = map[string]interface{}{
				"record":     map[string]interface{}{},
				"preprocess": map[string]interface{}{},
				"peoplepost": map[string]interface{}{},
				"people360":  map[string]interface{}{},
				"people720":  map[string]interface{}{},
				"fiber":      map[string]interface{}{},
				"set":        map[string]interface{}{},
				"golden":     map[string]interface{}{},
			}
			input.StatusHistory = []ReportStatus{
				ReportStatus{
					Label:     input.StatusLabel,
					Timestamp: time.Now(),
					Function:  input.StatusBy,
				},
			}
			esResponse, err := esClient.Index().
				Index(os.Getenv("REPORT_ESINDEX")).
				Id(input.ID).
				BodyJson(input).
				Do(ctx)
			if err != nil {
				log.Fatalf("error indexing %v", err)
			}
			log.Printf("index request on %v successful %+v", input.ID, esResponse)
		} else {
			// we'll run some updates
			esUpdate := esClient.Update().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID)

			// append to the status history
			if len(input.StatusLabel) > 0 {
				newStatus := ReportStatus{
					Label:     input.StatusLabel,
					Timestamp: time.Now(),
					Function:  input.StatusBy,
				}
				runUpdate(esUpdate.Script(elastic.NewScript("ctx._source.history.add(params.historyEntry)").Param("historyEntry", newStatus)))
			}

			if len(input.Counters) > 0 {
				for _, counter := range input.Counters {
					script := ""
					n := strings.ToLower(counter.Name)
					t := strings.ToLower(counter.Type)
					if counter.Increment {
						script = `if (ctx._source.counts.` + t + `.containsKey("` + n + `")) { ctx._source.counts.` + t + `["` + n + `"] += params.count} else { ctx._source.counts.` + t + `["` + n + `"] = params.count}`
					} else {
						script = `if (ctx._source.counts.` + t + `.containsKey("` + n + `")) { ctx._source.counts.` + t + `["` + n + `"] = params.count} else { ctx._source.counts.` + t + `["` + n + `"] = params.count}`
					}
					runUpdate(esUpdate.Script(elastic.NewScript(script).Param("count", counter.Count)))
				}
			}

			if len(input.Columns) > 0 {
				// let's make a map
				mapping := map[string]interface{}{}
				for _, column := range input.Columns {
					mapping[column] = map[string]int{}
				}
				runUpdate(esUpdate.Doc(map[string]interface{}{"mapping": mapping}))
			}

			if len(input.ColumnMaps) > 0 { // this goes into mapping
				for _, mapping := range input.ColumnMaps {
					script := `if (ctx._source.mapping["` + mapping.Name + `"].containsKey("` + mapping.Value + `")) { ctx._source.mapping["` + mapping.Name + `"]["` + mapping.Value + `"] ++} else { ctx._source.mapping["` + mapping.Name + `"]["` + mapping.Value + `"] = 1}`
					// log.Printf(script)
					runUpdate(esUpdate.Script(elastic.NewScript(script)))
				}
			}

			if len(input.InputStatistics) > 0 {
				runUpdate(esUpdate.Doc(map[string]interface{}{"inputStats": input.InputStatistics}))
			}

			// if len(input.OutputStatistics) > 0 {
			// 	esUpdate = esUpdate.Doc(map[string]interface{}{"outputStats": input.OutputStatistics})
			// }

			// apend errors and warnings
			if len(input.Errors) > 0 {
				for _, e := range input.Errors {
					runUpdate(esUpdate.Script(elastic.NewScript("ctx._source.errors.add(params.error)").Param("error", e)))
				}
			}

			if len(input.Warnings) > 0 {
				for _, e := range input.Warnings {
					runUpdate(esUpdate.Script(elastic.NewScript("ctx._source.warnings.add(params.warn)").Param("warn", e)))
				}
			}
		}

	} else {
		log.Printf("ERROR source is missing from message attributes %v", err)
		return nil
	}

	return nil
}

func runUpdate(eu *elastic.UpdateService) {
	esUpdateResponse, err := eu.DetectNoop(true).Refresh("true").Do(ctx)
	if err != nil {
		log.Printf("error updating es %v", err)
	} else {
		log.Printf("es updated with response %v", esUpdateResponse)
	}
}

// GetReport returns all information about an event stored in elastic
func GetReport(w http.ResponseWriter, r *http.Request) {
	var input reportRequest
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
		fmt.Fprint(w, "{success: false, message: \"Error decoding request\"}")
		log.Fatalf("error decoding request %v", err)
		return
	}

	// TODO: remove this
	if input.Bypass == "@U1Q6TAy^QH,98y" { // secret to bypass check for testing
	} else {

		// first verify customer has valid credentials
		var customerLookup []customer
		customerQuery := datastore.NewQuery("Customer").Namespace(os.Getenv("DATASTORENS")).Filter("AccessKey =", input.AccessKey).Limit(1)
		if _, err := dsClient.GetAll(ctx, customerQuery, &customerLookup); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Fatalf("Error querying customer: %v", err)
			fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -2\"}")
			return
		}
		if len(customerLookup) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, "{success: false, message: \"Invalid access key, -10\"}")
			return
		}
		log.Printf("found %v customer matches: %v", len(customerLookup), customerLookup)
		customer := customerLookup[0]
		if customer.Enabled == false {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, "{success: false, message: \"Account is not enabled, -11\"}")
			return
		}

		if !strings.EqualFold(customer.Owner, input.Owner) {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, "{success: false, message: \"Invalid credentials, -9\"}")
			return
		}

		// next validate the request id belongs to the customer
		var eventLookup []event
		eventQuery := datastore.NewQuery("Event").Namespace(os.Getenv("DATASTORENS")).Filter("EventID =", input.EventID).Limit(1)
		if _, err := fsClient.GetAll(ctx, eventQuery, &eventLookup); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Fatalf("Error querying customer: %v", err)
			fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -2\"}")
			return
		}
		if len(eventLookup) == 0 {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "{success: false, message: \"Event not found, -20\"}")
			return
		}
		log.Printf("found %v event matches: %v", len(eventLookup), eventLookup)
		event := eventLookup[0]
		if !strings.EqualFold(event.CustomerID, input.CustomerID) {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, "{success: false, message: \"Event does not match to the customer id, -25\"}")
			return
		}
	}
	// fetch the doc from elastic
	idQuery := elastic.NewTermQuery("_id", input.EventID)
	searchResult, err := esClient.Search().
		Index(os.Getenv("REPORT_ESINDEX")). // search in index "twitter"
		Query(idQuery).                     // specify the query
		From(0).Size(1).                    // take documents 0-9
		Pretty(true).                       // pretty print request and response JSON
		Do(ctx)                             // execute
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error fetching from elastic: %v", err)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -121\"}")
	}

	var doc FileReport
	for _, item := range searchResult.Each(reflect.TypeOf(doc)) {
		if t, ok := item.(FileReport); ok {
			jsonStr, err := json.Marshal(t)
			if err != nil {
				log.Fatalf("Error unable to marshal response  %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -122\"}")
			} else {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, string(jsonStr))
			}
		}
	}
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"

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
var ps *pubsub.Client
var sub *pubsub.Subscription
var mutex sync.Mutex

var projectID = os.Getenv("GCP_PROJECT")

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

	ps, err = pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("failed to setup pubsub client: %v", err)
	}

	sub = ps.Subscription(os.Getenv("REPORT_SUB"))
	sub.ReceiveSettings.Synchronous = true                     // run this synchronous
	sub.ReceiveSettings.MaxOutstandingBytes = 50 * 1024 * 1024 // 50MB max messages
	sub.ReceiveSettings.NumGoroutines = 1                      // run this as single threaded for elastic's sake
}

// PullMessages pulls messages from a pubsub subscription
func PullMessages(ctx context.Context, m psMessage) error {
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		mutex.Lock()
		ProcessUpdate(ctx, msg)
		defer mutex.Unlock()
		msg.Ack()
	})
	if err != nil {
		log.Printf("receive error: %v", err)
	}
	return nil
}

func main() {
	PullMessages(context.Background(), psMessage{Data: []byte("{}")})
}

// ProcessUpdate processes update from pubsub into elastic, returns bool indicating if the message should be retried
func ProcessUpdate(ctx context.Context, m *pubsub.Message) bool {
	var input FileReport
	err := json.Unmarshal(m.Data, &input)
	if err != nil {
		log.Printf("ERROR unable to unmarshal request %v", err)
		return false
	}
	bulk, err := esClient.BulkProcessor().Name("BulkUpdate").Stats(true).Workers(1).Do(ctx)
	if err != nil {
		log.Printf("error starting bulk processor %v", err)
		return true
	}

	log.Printf("received from -%v- message %v", m.Attributes, string(m.Data))
	if source, ok := m.Attributes["source"]; ok {
		if strings.Contains(source, "file-api") {
			// initialize arrays and run insert
			var report FileReport
			report.ID = input.ID
			report.RequestedAt = input.RequestedAt
			report.ProcessingBegin = input.ProcessingBegin
			report.ProcessingEnd = input.ProcessingEnd
			report.Attributes = input.Attributes
			report.Passthroughs = input.Passthroughs
			report.CustomerID = input.CustomerID
			report.InputFilePath = input.InputFilePath
			report.InputFileName = input.InputFileName
			report.Owner = input.Owner
			report.StatusLabel = input.StatusLabel
			report.StatusBy = input.StatusBy
			report.StatusTime = input.StatusTime
			report.Errors = []ReportError{}
			report.Warnings = []ReportError{}
			report.Audits = []ReportError{}
			report.Records = []RecordDetail{}
			report.Fibers = []FiberDetail{}
			report.Sets = []SetDetail{}
			report.Counts = []CounterGroup{
				CounterGroup{Group: "record", Items: []KeyCounter{}},
				CounterGroup{Group: "preprocess", Items: []KeyCounter{}},
				CounterGroup{Group: "peoplepost", Items: []KeyCounter{}},
				CounterGroup{Group: "people360", Items: []KeyCounter{}},
				CounterGroup{Group: "people720", Items: []KeyCounter{}},
				CounterGroup{Group: "fiber", Items: []KeyCounter{}},
				CounterGroup{Group: "set", Items: []KeyCounter{}},
				CounterGroup{Group: "golden", Items: []KeyCounter{}},
			}
			report.MatchKeyCounts = []KeyCounter{
				KeyCounter{Key: "AD1", Count: 0},
				KeyCounter{Key: "AD2", Count: 0},
				KeyCounter{Key: "FNAME", Count: 0},
				KeyCounter{Key: "LNAME", Count: 0},
				KeyCounter{Key: "EMAIL", Count: 0},
				KeyCounter{Key: "PHONE", Count: 0},
				KeyCounter{Key: "CITY", Count: 0},
				KeyCounter{Key: "STATE", Count: 0},
				KeyCounter{Key: "ZIP", Count: 0},
				KeyCounter{Key: "COUNTRY", Count: 0},
			}
			report.StatusHistory = []ReportStatus{
				ReportStatus{
					Label:     input.StatusLabel,
					Timestamp: time.Now(),
					Function:  input.StatusBy,
				},
			}
			js, _ := json.Marshal(report)
			log.Printf("%v", string(js))
			bulk.Add(elastic.NewBulkIndexRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(report.ID).Doc(report))
		} else {

			if !input.ProcessingBegin.IsZero() {
				bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Doc(map[string]interface{}{"processingBegin": input.ProcessingBegin}))
			}
			if !input.ProcessingEnd.IsZero() {
				bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Doc(map[string]interface{}{"processingEnd": input.ProcessingEnd}))
			}
			// append to the status history
			if len(input.StatusLabel) > 0 {
				newStatus := ReportStatus{
					Label:     input.StatusLabel,
					Timestamp: input.StatusTime,
					Function:  input.StatusBy,
				}
				bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Doc(map[string]interface{}{"statusLabel": input.StatusLabel, "statusBy": input.StatusBy, "statusTime": input.StatusTime}))
				bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript("ctx._source.history.add(params.historyEntry)").Param("historyEntry", newStatus)))
			}

			if len(input.Counters) > 0 {
				exists := `if (!ctx._source.containsKey("counts")) {ctx._source["counts"] = [];}`
				bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(exists)))

				for _, counter := range input.Counters {
					script := ""

					t := strings.ToLower(counter.Type)
					kc := KeyCounter{
						Key:   strings.ToLower(counter.Name),
						Count: counter.Count,
					}

					if counter.Increment {
						//script = `def groups = ctx._source.counts.findAll(g -> g.group == "` + t + `"); for(group in groups) {def counter = group.items.find(c -> c.key == params.count.key); if (counter == null) {group.items.add(params.count)} else {counter.count += params.count.count}}`
						script = `def group = ctx._source.counts.find(g -> g.group == "` + t + `"); def counter = group.items.find(c -> c.key == params.count.key); if (counter == null) {group.items.add(params.count)} else {counter.count += params.count.count}`
					} else {
						//script = `def groups = ctx._source.counts.findAll(g -> g.group == "` + t + `"); for(group in groups) {def counter = group.items.find(c -> c.key == params.count.key); if (counter == null) {group.items.add(params.count)} else {}}`
						script = `def group = ctx._source.counts.find(g -> g.group == "` + t + `"); def counter = group.items.find(c -> c.key == params.count.key); if (counter == null) {group.items.add(params.count)}`
					}
					bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(script).Param("count", kc)))
				}
			}

			if len(input.Columns) > 0 {
				exists := `if (!ctx._source.containsKey("mapping")) {ctx._source["mapping"] = [];}`
				bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(exists)))
				// let's make a list
				for _, column := range input.Columns {
					columnMapping := NameMappedCounter{
						Name:        column,
						MapCounters: []MapCounter{},
					}
					script := `def column = ctx._source.mapping.find(c -> c.name == params.m.name); if (column == null) {ctx._source.mapping.add(params.m)}`
					bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(script).Param("m", columnMapping)))
				}
			}

			if len(input.ColumnMaps) > 0 { // this goes into mapping
				exists := `if (!ctx._source.containsKey("mapping")) {ctx._source["mapping"] = [];}`
				bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(exists)))
				for _, mapping := range input.ColumnMaps {
					columnMapping := NameMappedCounter{
						Name: mapping.Name,
						MapCounters: []MapCounter{
							MapCounter{
								Name:  mapping.Value,
								Count: 1,
							},
						},
					}
					script := `def column = ctx._source.mapping.find(c -> c.name == params.map.name); if (column == null) {ctx._source.mapping.add(params.map)} else { def mapping = column.mapped.find(m -> m.name == params.map.mapped[0].name); if (mapping == null) {column.mapped.add(params.map.mapped[0]);} else {mapping.count++;}}`
					bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(script).Param("map", columnMapping)))
				}
			}

			if len(input.InputStatistics) > 0 {
				values := []ColumnStat{}
				for _, v := range input.InputStatistics {
					values = append(values, v)
				}
				bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Doc(map[string]interface{}{"inputStats": values}))
			}

			if len(input.MatchKeyStatistics) > 0 {
				exists := `if (!ctx._source.containsKey("matchKeyCounts")) {ctx._source["matchKeyCounts"] = [];}`
				bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(exists)))
				for k, v := range input.MatchKeyStatistics {
					count := KeyCounter{
						Key:   k,
						Count: v,
					}
					script := `def mk = ctx._source.matchKeyCounts.find(g -> g.key == params.count.key); if (mk == null) {ctx._source.matchKeyCounts.add(params.count);} else {mk.count += params.count.count}`
					bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(script).Param("count", count)))
				}
			}

			// apend errors and warnings
			if len(input.Errors) > 0 {
				for _, e := range input.Errors {
					bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript("ctx._source.errors.add(params.error)").Param("error", e)))
				}
			}

			if len(input.Warnings) > 0 {
				for _, e := range input.Warnings {
					bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript("ctx._source.warnings.add(params.warn)").Param("warn", e)))
				}
			}

			if len(input.Audits) > 0 {
				for _, e := range input.Audits {
					bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript("ctx._source.audits.add(params.audit)").Param("audit", e)))
				}
			}

			if len(input.RecordList) > 0 {
				for _, r := range input.RecordList {
					var record RecordDetail
					record.ID = r.ID
					record.RowNumber = r.RowNumber
					record.CreatedOn = r.CreatedOn
					record.Fibers = []string{}
					// add record if does not exist
					script := `def r = ctx._source.records.find(g -> g.id == params.record.id); if (r == null) {ctx._source.records.add(params.record);}`
					bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(script).Param("record", record)))

					combinedscript := "def r = ctx._source.records.find(g -> g.id == params.r.id); "
					// update isPerson
					if len(r.IsPerson) > 0 {
						combinedscript += `r.isPerson = params.r.isPerson; `
					}
					//update disposition
					if len(r.Disposition) > 0 {
						combinedscript += `r.disposition = params.r.disposition; `
					}
					// add fiber
					if len(r.Fibers) > 0 {
						combinedscript += `r.fibers.addAll(params.r.fibers); `
					}
					if len(combinedscript) > 70 {
						log.Printf("script: %v", combinedscript)
						js, _ := json.Marshal(r)
						log.Printf("record: %v", string(js))
						bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(combinedscript).Param("r", r)))
					}
				}
			}

			if len(input.FiberList) > 0 {
				for _, r := range input.FiberList {
					var fiber FiberDetail
					fiber.ID = r.ID
					fiber.CreatedOn = r.CreatedOn
					fiber.Disposition = r.Disposition
					fiber.Type = r.Type
					fiber.Sets = []string{}
					// create the fiber if not already there
					script := `def r = ctx._source.fibers.find(g -> g.id == params.fiber.id); if (r == null) {ctx._source.fibers.add(params.fiber);}`
					bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(script).Param("fiber", fiber)))

					combinedscript := "def r = ctx._source.fibers.find(g -> g.id == params.r.id); "
					//update disposition
					if len(r.Disposition) > 0 {
						combinedscript += `r.disposition = params.r.disposition; `
					}
					// add sets
					if len(r.Sets) > 0 {
						combinedscript += `r.sets.addAll(params.r.sets); `
					}
					if len(combinedscript) > 65 {
						bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(combinedscript).Param("r", r)))
					}
				}
			}

			if len(input.SetList) > 0 {
				for _, r := range input.SetList {
					var set SetDetail
					set.ID = r.ID
					set.FiberCount = r.FiberCount //this does not change as we do not update set
					set.CreatedOn = r.CreatedOn

					// create the set if not already there
					script := `def r = ctx._source.sets.find(g -> g.id == params.set.id); if (r == null) {ctx._source.sets.add(params.set);}`
					bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(script).Param("set", set)))

					combinedscript := "def r = ctx._source.sets.find(g -> g.id == params.r.id); "
					if r.IsDeleted {
						combinedscript += `r.isDeleted = params.r.isDeleted; r.deletedOn = params.r.deletedOn; `
					}
					if len(r.ReplacedBy) > 0 {
						combinedscript += `r.replacedBy = params.r.replacedBy; `
					}
					if len(combinedscript) > 60 {
						bulk.Add(elastic.NewBulkUpdateRequest().Index(os.Getenv("REPORT_ESINDEX")).Id(input.ID).Script(elastic.NewScript(combinedscript).Param("r", r)))
					}
				}
			}
		}

		// run the bulk request
		err = bulk.Flush()
		if err != nil {
			log.Printf("error running bulk update %v", err)
			return true
		}

		stats := bulk.Stats()
		log.Printf("Bulk action created %d, updated %d with %d success and %d failure", stats.Created, stats.Updated, stats.Succeeded, stats.Failed)
		if stats.Succeeded == 0 && stats.Failed > 0 {
			return true
		}

	} else {
		log.Printf("ERROR source is missing from message attributes %v", err)
		return false
	}

	return false
}

func runElasticIndex(eu *elastic.IndexService) {
	_, err := eu.Do(ctx)
	if err != nil {
		log.Printf("error updating es %v", err)
	}
}

func runElasticUpdate(eu *elastic.UpdateService) {
	_, err := eu.DetectNoop(true).Do(ctx)
	if err != nil {
		log.Printf("error updating es %v", err)
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

	for _, hit := range searchResult.Hits.Hits {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, string(hit.Source))
	}
}

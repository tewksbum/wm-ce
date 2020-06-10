package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"

	"github.com/olivere/elastic/v7"

	// cloud sql
	_ "github.com/go-sql-driver/mysql"

	secretmanager "cloud.google.com/go/secretmanager/apiv1beta1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1beta1"
)

var (
	test FileReport

	ctx context.Context

	esClient *elastic.Client

	dsClient *datastore.Client
	fsClient *datastore.Client

	smClient *secretmanager.Client

	esSecret elasticSecret

	ps  *pubsub.Client
	sub *pubsub.Subscription

	mutex sync.Mutex

	db *sql.DB

	insertRecord            *sql.Stmt
	insertRecordFiber       *sql.Stmt
	insertFiber             *sql.Stmt
	insertFiberSet          *sql.Stmt
	insertSet               *sql.Stmt
	updateRecordPerson      *sql.Stmt
	updateRecordDisposition *sql.Stmt
	updateFiberDisposition  *sql.Stmt
	updateSetDeleted        *sql.Stmt

	projectID = os.Getenv("GCP_PROJECT")
	index     = os.Getenv("REPORT_ESINDEX")
)

func init() {
	log.Printf("starting exception-report")
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
	sub.ReceiveSettings.Synchronous = true                      // run this synchronous
	sub.ReceiveSettings.MaxOutstandingMessages = 1024 * 1024    // 1M max message
	sub.ReceiveSettings.MaxOutstandingBytes = 500 * 1024 * 1024 // 500MB max message bytes
	sub.ReceiveSettings.NumGoroutines = 1                       // there are only 2 CPU in the VM, one for dev, one for prod

	dsn := fmt.Sprintf("pipeline@tcp(%v:3306)/pipeline?tls=skip-verify&autocommit=true&parseTime=true", os.Getenv("MYSQL_HOST"))

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("error opening db %v", err)
	}

	insertRecord, err = db.Prepare(`insert into records (id, eventId, row, createdOn, isPerson, disposition) values (?,?,?,?,?,?)`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "insertRecord", err)
	}
	insertRecordFiber, err = db.Prepare(`insert into record_fibers (record_id, fiber_id) values (?,?)`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "insertRecordFiber", err)
	}
	insertFiber, err = db.Prepare(`insert into fibers (id, eventId, createdOn, type, disposition) values (?,?,?,?,?)`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "insertFiber", err)
	}
	insertFiberSet, err = db.Prepare(`insert into fiber_sets (fiber_id, set_id) values (?,?)`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "insertFiberSet", err)
	}
	insertSet, err = db.Prepare(`insert into sets (id, eventId, fiberCount, createdOn, isDeleted, replacedBy) values (?,?,?,?,?,?)`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "insertSet", err)
	}
	updateRecordPerson, err = db.Prepare(`update records set isPerson = ? where id = ?`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "updateRecordPerson", err)
	}
	updateRecordDisposition, err = db.Prepare(`update records set disposition = ? where id = ?`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "updateRecordDisposition", err)
	}
	updateFiberDisposition, err = db.Prepare(`update fibers set disposition = ? where id = ?`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "updateFiberDisposition", err)
	}
	updateSetDeleted, err = db.Prepare(`update sets set isDeleted = ?, deletedOn = ?, replacedBy = ? where id = ?`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "updateSetDeleted", err)
	}
}

// PullMessages pulls messages from a pubsub subscription
func PullMessages(ctx context.Context, m psMessage) error {
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		//mutex.Lock()
		processUpdate(ctx, msg)
		//mutex.Unlock()
		msg.Ack()
	})
	if err != nil {
		log.Printf("receive error: %v", err)
	}
	return nil
}

func getMessages() {
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		processUpdate(ctx, msg)
		msg.Ack()
	})
	if err != nil {
		log.Printf("receive error: %v", err)
	}
}

// main for go
func main() {
	errc := make(chan error)

	// Watch for SIGINT and SIGTERM from the console.
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		fmt.Printf("caught signal %v\n", <-c)
		errc <- nil
	}()

	go getMessages()
	// Wait for problems.
	if err := <-errc; err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func afterUpdate(id int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	for i, r := range response.Items {
		for k, v := range r {
			if len(v.Result) == 0 {
				log.Printf("%v error %v for input %v", k, v.Error, requests[i])
			}
		}
	}
}

// processUpdate processes update from pubsub into elastic, returns bool indicating if the message should be retried
func processUpdate(ctx context.Context, m *pubsub.Message) bool {
	var input FileReport
	err := json.Unmarshal(m.Data, &input)
	if err != nil {
		log.Printf("ERROR unable to unmarshal request %v", err)
		return false
	}
	bulk, err := esClient.BulkProcessor().Name("BulkUpdate").Stats(true).After(afterUpdate).Workers(1).Do(ctx)
	if err != nil {
		log.Printf("error starting bulk processor %v", err)
		return true
	}

	// in case we dont have the doc yet
	idReport := IDOnly{ID: input.ID}
	bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Doc(idReport).DocAsUpsert(true).RetryOnConflict(5))

	if !input.RequestedAt.IsZero() && len(input.Owner) > 0 && len(input.InputFileName) > 0 {
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Doc(FileReport{
			RequestedAt:   input.RequestedAt,
			Owner:         input.Owner,
			InputFileName: input.InputFileName,
			InputFilePath: input.InputFilePath,
			CustomerID:    input.CustomerID,
			Attributes:    input.Attributes,
			Passthroughs:  input.Passthroughs,
		}).DocAsUpsert(true).RetryOnConflict(5))
	}

	if !input.ProcessingBegin.IsZero() {
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Doc(map[string]interface{}{"processingBegin": input.ProcessingBegin}).RetryOnConflict(5))
	}
	if !input.ProcessingEnd.IsZero() {
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Doc(map[string]interface{}{"processingEnd": input.ProcessingEnd}).RetryOnConflict(5))
	}
	// append to the status history
	if len(input.StatusLabel) > 0 {
		exists := `if (!ctx._source.containsKey("history")) {ctx._source["history"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)).RetryOnConflict(5))
		newStatus := ReportStatus{
			Label:     input.StatusLabel,
			Timestamp: input.StatusTime,
			Function:  input.StatusBy,
		}
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Doc(map[string]interface{}{"statusLabel": input.StatusLabel, "statusBy": input.StatusBy, "statusTime": input.StatusTime}).RetryOnConflict(5))
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript("ctx._source.history.add(params.historyEntry)").Param("historyEntry", newStatus)).RetryOnConflict(5))
	}

	if len(input.Counters) > 0 {
		exists := `if (!ctx._source.containsKey("counts")) {ctx._source["counts"] = params.init}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists).Param("init", []CounterGroup{
			CounterGroup{Group: "fileprocessor", Items: []KeyCounter{KeyCounter{Key: "purge", Count: 0}, KeyCounter{Key: "raw", Count: 0}, KeyCounter{Key: "columns", Count: 0}, KeyCounter{Key: "outputted", Count: 0}}},
			CounterGroup{Group: "preprocess", Items: []KeyCounter{KeyCounter{Key: "ispeople", Count: 0}, KeyCounter{Key: "isevent", Count: 0}}},
			CounterGroup{Group: "peoplepost", Items: []KeyCounter{KeyCounter{Key: "default", Count: 0}, KeyCounter{Key: "mar", Count: 0}, KeyCounter{Key: "mpr", Count: 0}, KeyCounter{Key: "total", Count: 0}}},
			CounterGroup{Group: "people360", Items: []KeyCounter{KeyCounter{Key: "unmatchable", Count: 0}, KeyCounter{Key: "dupe", Count: 0}, KeyCounter{Key: "singletons", Count: 0}, KeyCounter{Key: "sets", Count: 0}, KeyCounter{Key: "total", Count: 0}}},
			CounterGroup{Group: "people720", Items: []KeyCounter{KeyCounter{Key: "reprocess", Count: 0}}},
			CounterGroup{Group: "golden", Items: []KeyCounter{KeyCounter{Key: "unique", Count: 0}, KeyCounter{Key: "isadvalid", Count: 0}, KeyCounter{Key: "hasemail", Count: 0}}},
			CounterGroup{Group: "golden:mpr", Items: []KeyCounter{KeyCounter{Key: "unique", Count: 0}, KeyCounter{Key: "isadvalid", Count: 0}, KeyCounter{Key: "hasemail", Count: 0}}},
			CounterGroup{Group: "golden:nonmpr", Items: []KeyCounter{KeyCounter{Key: "unique", Count: 0}, KeyCounter{Key: "isadvalid", Count: 0}, KeyCounter{Key: "hasemail", Count: 0}}},
			CounterGroup{Group: "people360:audit", Items: []KeyCounter{}},
		})).RetryOnConflict(5))
		for _, counter := range input.Counters {
			script := ""
			kc := KeyCounter{
				Key:   strings.ToLower(counter.Name),
				Count: counter.Count,
			}
			cg := CounterGroup{
				Group: strings.ToLower(counter.Type),
				Items: []KeyCounter{kc},
			}
			if counter.Increment {
				//script = `def groups = ctx._source.counts.findAll(g -> g.group == "` + t + `"); for(group in groups) {def counter = group.items.find(c -> c.key == params.count.key); if (counter == null) {group.items.add(params.count)} else {counter.count += params.count.count}}`
				script = `def group = ctx._source.counts.find(g -> g.group == params.cg.group); if (group == null) {ctx._source.counts.add(params.cg)} else {def counter = group.items.find(c -> c.key == params.cg.items[0].key); if (counter == null) {group.items.add(params.cg.items[0])} else {counter.count += params.cg.items[0].count}}`
			} else {
				//script = `def groups = ctx._source.counts.findAll(g -> g.group == "` + t + `"); for(group in groups) {def counter = group.items.find(c -> c.key == params.count.key); if (counter == null) {group.items.add(params.count)} else {}}`
				script = `def group = ctx._source.counts.find(g -> g.group == params.cg.group); if (group == null) {ctx._source.counts.add(params.cg)} else {def counter = group.items.find(c -> c.key == params.cg.items[0].key); if (counter == null) {group.items.add(params.cg.items[0])}}`
			}
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("cg", cg)).RetryOnConflict(5))
		}
	}

	if len(input.Columns) > 0 { // this goes into fields
		exists := `if (!ctx._source.containsKey("fields")) {ctx._source["fields"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)).RetryOnConflict(5))
		// let's make a list
		for _, column := range input.Columns {
			columnMapping := NameMappedCounter{
				Name:        column,
				MapCounters: []MapCounter{},
			}
			script := `def column = ctx._source.fields.find(c -> c.name == params.m.name); if (column == null) {ctx._source.fields.add(params.m)}`
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("m", columnMapping)).RetryOnConflict(5))
		}
	}

	if len(input.ColumnMaps) > 0 { // this goes into fields
		exists := `if (!ctx._source.containsKey("fields")) {ctx._source["fields"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)).RetryOnConflict(5))
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
			script := `def column = ctx._source.fields.find(c -> c.name == params.map.name); if (column == null) {ctx._source.fields.add(params.map)} else { def mapping = column.mapped.find(m -> m.name == params.map.mapped[0].name); if (mapping == null) {column.mapped.add(params.map.mapped[0]);} else {mapping.count++;}}`
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("map", columnMapping)).RetryOnConflict(5))
		}
	}

	if len(input.InputStatistics) > 0 { // this maps to fields
		exists := `if (!ctx._source.containsKey("fields")) {ctx._source["fields"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)).RetryOnConflict(5))
		for _, v := range input.InputStatistics {
			v.Mapped = []MapCounter{}
			script := `def column = ctx._source.fields.find(c -> c.name == params.stat.name); if (column == null) {ctx._source.fields.add(params.stat)} else { column.min = params.stat.min; column.max = params.stat.max; column.sparsity = params.stat.sparsity;}`
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("stat", v)).RetryOnConflict(5))
		}
	}

	if len(input.MatchKeyStatistics) > 0 {
		exists := `if (!ctx._source.containsKey("matchKeyCounts")) {ctx._source["matchKeyCounts"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)).RetryOnConflict(5))
		for k, v := range input.MatchKeyStatistics {
			count := KeyCounter{
				Key:   k,
				Count: v,
			}
			script := `def mk = ctx._source.matchKeyCounts.find(g -> g.key == params.count.key); if (mk == null) {ctx._source.matchKeyCounts.add(params.count);} else {mk.count += params.count.count}`
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("count", count)).RetryOnConflict(5))
		}
	}

	// apend errors and warnings
	if len(input.Errors) > 0 {
		exists := `if (!ctx._source.containsKey("errors")) {ctx._source["errors"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)).RetryOnConflict(5))
		for _, e := range input.Errors {
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript("ctx._source.errors.add(params.error)").Param("error", e)).RetryOnConflict(5))
		}
	}

	if len(input.Warnings) > 0 {
		exists := `if (!ctx._source.containsKey("warnings")) {ctx._source["warnings"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)).RetryOnConflict(5))
		for _, e := range input.Warnings {
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript("ctx._source.warnings.add(params.warn)").Param("warn", e)).RetryOnConflict(5))
		}
	}

	if len(input.Audits) > 0 {
		exists := `if (!ctx._source.containsKey("audits")) {ctx._source["audits"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)).RetryOnConflict(5))
		for _, e := range input.Audits {
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript("ctx._source.audits.add(params.audit)").Param("audit", e)).RetryOnConflict(5))
		}
	}

	if len(input.RecordList) > 0 {
		go processRecordList(input.RecordList, input.ID)
	}

	if len(input.FiberList) > 0 {
		for _, r := range input.FiberList {
			if !r.CreatedOn.IsZero() {
				_, err = insertFiber.Exec(r.ID, input.ID, r.CreatedOn, r.Type, r.Disposition)
				if err != nil {
					log.Printf("Error running insertFiber: %v", err)
				}
			}

			if len(r.Disposition) > 0 {
				_, err = updateFiberDisposition.Exec(r.Disposition, r.ID)
				if err != nil {
					log.Printf("Error running updateFiberDisposition: %v", err)
				}
			}
			// add Sets
			if len(r.Sets) > 0 {
				for _, set := range r.Sets {
					_, err = insertFiberSet.Exec(r.ID, set)
					if err != nil {
						log.Printf("Error running insertFiberSet: %v", err)
					}
				}
			}
		}

	}

	if len(input.SetList) > 0 {
		for _, r := range input.SetList {
			if !r.CreatedOn.IsZero() {
				_, err = insertSet.Exec(r.ID, input.ID, r.FiberCount, r.CreatedOn, r.IsDeleted, r.ReplacedBy)
				if err != nil {
					log.Printf("Error running insertSet: %v", err)
				}
			}

			if len(r.ReplacedBy) > 0 {
				_, err = updateSetDeleted.Exec(r.IsDeleted, r.DeletedOn, r.ReplacedBy, r.ID)
				if err != nil {
					log.Printf("Error running updateSetDeleted: %v", err)
				}
			}
		}
	}
	err = bulk.Flush()
	if err != nil {
		log.Printf("error running bulk update %v", err)
		return true
	}

	err = nil
	input = FileReport{}

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

func processRecordList(records []RecordDetail, eventID string) {
	var err error
	for _, r := range records {
		if !r.CreatedOn.IsZero() {
			_, err = insertRecord.Exec(r.ID, eventID, r.RowNumber, r.CreatedOn, r.IsPerson, r.Disposition)
			if err != nil {
				log.Printf("Error running insertRecord: %v", err)
			}
		}

		if len(r.IsPerson) > 0 {
			_, err = updateRecordPerson.Exec(r.IsPerson, r.ID)
			if err != nil {
				log.Printf("Error running updateRecordPerson: %v", err)
			}
		}
		//update disposition
		if len(r.Disposition) > 0 {
			_, err = updateRecordDisposition.Exec(r.Disposition, r.ID)
			if err != nil {
				log.Printf("Error running updateRecordDisposition: %v", err)
			}
		}
		// add fiber
		if len(r.Fibers) > 0 {
			for _, fiber := range r.Fibers {
				_, err = insertRecordFiber.Exec(r.ID, fiber)
				if err != nil {
					log.Printf("Error running insertRecordFiber: %v", err)
				}
			}
		}
	}
}

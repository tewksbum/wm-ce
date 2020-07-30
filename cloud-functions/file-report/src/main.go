package filereport

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

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

	index = os.Getenv("REPORT_ESINDEX")
)

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
	log.Printf("secret: %v", string(secretsData1))

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
	sub.ReceiveSettings.MaxOutstandingBytes = 500 * 1024 * 1024 // 50MB max messages
	sub.ReceiveSettings.NumGoroutines = 1                       // run this as single threaded for elastic's sake

	dsn := fmt.Sprintf("pipeline@unix(/cloudsql/%v)/pipeline?tls=false&autocommit=true&parseTime=true", os.Getenv("MYSQL_INSTANCE"))
	if os.Getenv("VENDOR") == "apple" { // detect local
		dsn = fmt.Sprintf("pipeline@tcp(%v:3306)/pipeline?tls=skip-verify&autocommit=true&parseTime=true", os.Getenv("MYSQL_TESTING"))
	}

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("error opening db %v", err)
	}

	insertRecord, err = db.Prepare(`insert into records (id, row, createdOn, isPerson, disposition) values (?,?,?,?,?)`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "insertRecord", err)
	}
	insertRecordFiber, err = db.Prepare(`insert into record_fibers (record_id, fiber_id) values (?,?)`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "insertRecordFiber", err)
	}
	insertFiber, err = db.Prepare(`insert into fibers (id, createdOn, type, disposition) values (?,?,?,?)`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "insertFiber", err)
	}
	insertFiberSet, err = db.Prepare(`insert into fiber_sets (fiber_id, set_id) values (?,?)`)
	if err != nil {
		log.Fatalf("Unable to prepare statement %v error %v", "insertFiberSet", err)
	}
	insertSet, err = db.Prepare(`insert into sets (id, fiberCount, createdOn, isDeleted, replacedBy) values (?,?,?,?,?)`)
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

// main for go
func main() {
	PullMessages(ctx, psMessage{})
}

// ProcessUpdate processes update from pubsub into elastic, returns bool indicating if the message should be retried
func ProcessUpdate(ctx context.Context, m *pubsub.Message) bool {
	var input FileReport
	err := json.Unmarshal(m.Data, &input)
	if err != nil {
		log.Printf("ERROR unable to unmarshal request %v", err)
		return false
	}
	bulk, err := esClient.BulkProcessor().Name("BulkUpdate").Stats(true).After(after).Workers(1).Do(ctx)
	if err != nil {
		log.Printf("error starting bulk processor %v", err)
		return true
	}

	log.Printf("received from -%v- message %v", m.Attributes, string(m.Data))
	idReport := IDOnly{ID: input.ID}
	idReportSponsor := IDOnly{ID: input.CustomerID}
	log.Printf("idReportSponsor: %v", idReportSponsor)
	bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Doc(idReport).DocAsUpsert(true))

	if !input.RequestedAt.IsZero() && len(input.Owner) > 0 && len(input.InputFileName) > 0 {
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Doc(FileReport{
			RequestedAt:   input.RequestedAt,
			Owner:         input.Owner,
			InputFileName: input.InputFileName,
			InputFilePath: input.InputFilePath,
			CustomerID:    input.CustomerID,
			Attributes:    input.Attributes,
			Passthroughs:  input.Passthroughs,
		}).DocAsUpsert(true))
	}

	if !input.ProcessingBegin.IsZero() {
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Doc(map[string]interface{}{"processingBegin": input.ProcessingBegin}))
	}
	if !input.ProcessingEnd.IsZero() {
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Doc(map[string]interface{}{"processingEnd": input.ProcessingEnd}))
	}
	// append to the status history
	if len(input.StatusLabel) > 0 {
		exists := `if (!ctx._source.containsKey("history")) {ctx._source["history"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)))
		newStatus := ReportStatus{
			Label:     input.StatusLabel,
			Timestamp: input.StatusTime,
			Function:  input.StatusBy,
		}
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Doc(map[string]interface{}{"statusLabel": input.StatusLabel, "statusBy": input.StatusBy, "statusTime": input.StatusTime}))
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript("ctx._source.history.add(params.historyEntry)").Param("historyEntry", newStatus)))
	}

	if len(input.Counters) > 0 {
		exists := `if (!ctx._source.containsKey("counts")) {ctx._source["counts"] = params.init}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists).Param("init", []CounterGroup{
			CounterGroup{Group: "fileprocessor", Items: []KeyCounter{KeyCounter{Key: "purge", Count: 0}, KeyCounter{Key: "raw", Count: 0}, KeyCounter{Key: "columns", Count: 0}, KeyCounter{Key: "outputted", Count: 0}}},
			CounterGroup{Group: "preprocess", Items: []KeyCounter{KeyCounter{Key: "purge", Count: 0}, KeyCounter{Key: "ispeople", Count: 0}, KeyCounter{Key: "isevent", Count: 0}}},
			CounterGroup{Group: "peoplepost", Items: []KeyCounter{KeyCounter{Key: "default", Count: 0}, KeyCounter{Key: "mar", Count: 0}, KeyCounter{Key: "mpr", Count: 0}, KeyCounter{Key: "total", Count: 0}}},
			CounterGroup{Group: "people360", Items: []KeyCounter{KeyCounter{Key: "unmatchable", Count: 0}, KeyCounter{Key: "dupe", Count: 0}, KeyCounter{Key: "singletons", Count: 0}, KeyCounter{Key: "sets", Count: 0}, KeyCounter{Key: "total", Count: 0}}},
			CounterGroup{Group: "people720", Items: []KeyCounter{KeyCounter{Key: "reprocess", Count: 0}}},
			CounterGroup{Group: "golden", Items: []KeyCounter{KeyCounter{Key: "unique", Count: 0}, KeyCounter{Key: "isadvalid", Count: 0}, KeyCounter{Key: "hasemail", Count: 0}}},
			CounterGroup{Group: "golden:mpr", Items: []KeyCounter{KeyCounter{Key: "unique", Count: 0}, KeyCounter{Key: "isadvalid", Count: 0}, KeyCounter{Key: "hasemail", Count: 0}}},
			CounterGroup{Group: "golden:nonmpr", Items: []KeyCounter{KeyCounter{Key: "unique", Count: 0}, KeyCounter{Key: "isadvalid", Count: 0}, KeyCounter{Key: "hasemail", Count: 0}}},
			CounterGroup{Group: "people360:audit", Items: []KeyCounter{}},
		})))
		var flag bool
		for _, counter := range input.Counters {
			script := ""

			kc := KeyCounter{
				Key:   strings.ToLower(counter.Name),
				Count: counter.Count,
			}

			cg := CounterGroup{
				Group: counter.Type,
				Items: []KeyCounter{kc},
			}

			if counter.Increment {
				//script = `def groups = ctx._source.counts.findAll(g -> g.group == "` + t + `"); for(group in groups) {def counter = group.items.find(c -> c.key == params.count.key); if (counter == null) {group.items.add(params.count)} else {counter.count += params.count.count}}`
				script = `def group = ctx._source.counts.find(g -> g.group == params.cg.group); if (group == null) {ctx._source.counts.add(params.cg)} else {def counter = group.items.find(c -> c.key == params.cg.items[0].key); if (counter == null) {group.items.add(params.cg.items[0])} else {counter.count += params.cg.items[0].count}}`
			} else {
				//script = `def groups = ctx._source.counts.findAll(g -> g.group == "` + t + `"); for(group in groups) {def counter = group.items.find(c -> c.key == params.count.key); if (counter == null) {group.items.add(params.count)} else {}}`
				script = `def group = ctx._source.counts.find(g -> g.group == params.cg.group); if (group == null) {ctx._source.counts.add(params.cg)} else {def counter = group.items.find(c -> c.key == params.cg.items[0].key); if (counter == null) {group.items.add(params.cg.items[0])}}`
			}

			if strings.HasPrefix(counter.Type, "SchoolYear:") {
				if !flag {
					bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.CustomerID).Doc(idReportSponsor).DocAsUpsert(true))
					flag = true
				}
				bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.CustomerID).Script(elastic.NewScript(script).Param("cg", cg)))
			}

			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("cg", cg)))
		}
	}

	if len(input.Columns) > 0 { // this goes into fields
		exists := `if (!ctx._source.containsKey("fields")) {ctx._source["fields"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)))
		// let's make a list
		for _, column := range input.Columns {
			columnMapping := NameMappedCounter{
				Name:        column,
				MapCounters: []MapCounter{},
			}
			script := `def column = ctx._source.fields.find(c -> c.name == params.m.name); if (column == null) {ctx._source.fields.add(params.m)}`
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("m", columnMapping)))
		}
	}

	if len(input.ColumnMaps) > 0 { // this goes into fields
		exists := `if (!ctx._source.containsKey("fields")) {ctx._source["fields"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)))
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
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("map", columnMapping)))
		}
	}

	if len(input.InputStatistics) > 0 { // this maps to fields
		exists := `if (!ctx._source.containsKey("fields")) {ctx._source["fields"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)))
		for _, v := range input.InputStatistics {
			v.Mapped = []MapCounter{}
			script := `def column = ctx._source.fields.find(c -> c.name == params.stat.name); if (column == null) {ctx._source.fields.add(params.stat)} else { column.min = params.stat.min; column.max = params.stat.max; column.sparsity = params.stat.sparsity;}`
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("stat", v)))
		}
	}

	if len(input.MatchKeyStatistics) > 0 {
		exists := `if (!ctx._source.containsKey("matchKeyCounts")) {ctx._source["matchKeyCounts"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)))
		for k, v := range input.MatchKeyStatistics {
			count := KeyCounter{
				Key:   k,
				Count: v,
			}
			script := `def mk = ctx._source.matchKeyCounts.find(g -> g.key == params.count.key); if (mk == null) {ctx._source.matchKeyCounts.add(params.count);} else {mk.count += params.count.count}`
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("count", count)))
		}
	}

	// apend errors and warnings
	if len(input.Errors) > 0 {
		exists := `if (!ctx._source.containsKey("errors")) {ctx._source["errors"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)))
		for _, e := range input.Errors {
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript("ctx._source.errors.add(params.error)").Param("error", e)))
		}
	}

	if len(input.Warnings) > 0 {
		exists := `if (!ctx._source.containsKey("warnings")) {ctx._source["warnings"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)))
		for _, e := range input.Warnings {
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript("ctx._source.warnings.add(params.warn)").Param("warn", e)))
		}
	}

	if len(input.Audits) > 0 {
		exists := `if (!ctx._source.containsKey("audits")) {ctx._source["audits"] = [];}`
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)))
		for _, e := range input.Audits {
			bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript("ctx._source.audits.add(params.audit)").Param("audit", e)))
		}
	}

	if len(input.RecordList) > 0 {
		for _, r := range input.RecordList {
			_, err = insertRecord.Exec(r.ID, r.RowNumber, r.CreatedOn, r.IsPerson, r.Disposition)
			if err != nil {
				log.Printf("Error running insertRecord: %v", err)
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

		// exists := `if (!ctx._source.containsKey("records")) {ctx._source["records"] = [];}`
		// bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)))

		// for _, r := range input.RecordList {
		// 	var record RecordDetail
		// 	record.ID = r.ID
		// 	record.RowNumber = r.RowNumber
		// 	record.CreatedOn = r.CreatedOn
		// 	record.Fibers = []string{}
		// 	// add record if does not exist
		// 	script := `def r = ctx._source.records.find(g -> g.id == params.record.id); if (r == null) {ctx._source.records.add(params.record);}`
		// 	bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("record", record)))

		// 	combinedscript := "def r = ctx._source.records.find(g -> g.id == params.r.id); "
		// 	// update isPerson
		// 	if len(r.IsPerson) > 0 {
		// 		combinedscript += `r.isPerson = params.r.isPerson; `
		// 	}
		// 	//update disposition
		// 	if len(r.Disposition) > 0 {
		// 		combinedscript += `r.disposition = params.r.disposition; `
		// 	}
		// 	// add fiber
		// 	if len(r.Fibers) > 0 {
		// 		combinedscript += `r.fibers.addAll(params.r.fibers); `
		// 	}
		// 	if len(combinedscript) > 70 {
		// 		log.Printf("script: %v", combinedscript)
		// 		js, _ := json.Marshal(r)
		// 		log.Printf("record: %v", string(js))
		// 		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(combinedscript).Param("r", r)))
		// 	}
		// }
	}

	if len(input.FiberList) > 0 {
		for _, r := range input.FiberList {
			_, err = insertFiber.Exec(r.ID, r.CreatedOn, r.Type, r.Disposition)
			if err != nil {
				log.Printf("Error running insertFiber: %v", err)
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

		// exists := `if (!ctx._source.containsKey("fibers")) {ctx._source["fibers"] = [];}`
		// bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)))

		// for _, r := range input.FiberList {
		// 	var fiber FiberDetail
		// 	fiber.ID = r.ID
		// 	fiber.CreatedOn = r.CreatedOn
		// 	fiber.Disposition = r.Disposition
		// 	fiber.Type = r.Type
		// 	fiber.Sets = []string{}
		// 	// create the fiber if not already there
		// 	script := `def r = ctx._source.fibers.find(g -> g.id == params.fiber.id); if (r == null) {ctx._source.fibers.add(params.fiber);}`
		// 	bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("fiber", fiber)))

		// 	combinedscript := "def r = ctx._source.fibers.find(g -> g.id == params.r.id); "
		// 	//update disposition
		// 	if len(r.Disposition) > 0 {
		// 		combinedscript += `r.disposition = params.r.disposition; `
		// 	}
		// 	// add sets
		// 	if len(r.Sets) > 0 {
		// 		combinedscript += `r.sets.addAll(params.r.sets); `
		// 	}
		// 	if len(combinedscript) > 65 {
		// 		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(combinedscript).Param("r", r)))
		// 	}
		// }
	}

	if len(input.SetList) > 0 {
		for _, r := range input.SetList {
			//id, fiberCount, createdOn, isDeleted, replacedBy
			_, err = insertSet.Exec(r.ID, r.FiberCount, r.CreatedOn, r.IsDeleted, r.ReplacedBy)
			if err != nil {
				log.Printf("Error running insertSet: %v", err)
			}

			if len(r.ReplacedBy) > 0 {
				_, err = updateSetDeleted.Exec(r.IsDeleted, r.DeletedOn, r.ReplacedBy, r.ID)
				if err != nil {
					log.Printf("Error running updateSetDeleted: %v", err)
				}
			}
		}

		// exists := `if (!ctx._source.containsKey("sets")) {ctx._source["sets"] = [];}`
		// bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(exists)))

		// for _, r := range input.SetList {
		// 	var set SetDetail
		// 	set.ID = r.ID
		// 	set.FiberCount = r.FiberCount //this does not change as we do not update set
		// 	set.CreatedOn = r.CreatedOn

		// 	// create the set if not already there
		// 	script := `def r = ctx._source.sets.find(g -> g.id == params.set.id); if (r == null) {ctx._source.sets.add(params.set);}`
		// 	bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(script).Param("set", set)))

		// 	combinedscript := "def r = ctx._source.sets.find(g -> g.id == params.r.id); "
		// 	if r.IsDeleted {
		// 		combinedscript += `r.isDeleted = params.r.isDeleted; r.deletedOn = params.r.deletedOn; `
		// 	}
		// 	if len(r.ReplacedBy) > 0 {
		// 		combinedscript += `r.replacedBy = params.r.replacedBy; `
		// 	}
		// 	if len(combinedscript) > 60 {
		// 		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(input.ID).Script(elastic.NewScript(combinedscript).Param("r", r)))
		// 	}
		// }
	}

	// run the bulk request
	err = bulk.Flush()
	if err != nil {
		log.Printf("error running bulk update %v", err)
		return true
	}

	stats := bulk.Stats()
	log.Printf("Bulk action created %d, updated %d with %d success and %d failure", stats.Created, stats.Updated, stats.Succeeded, stats.Failed)
	if stats.Succeeded == 0 && stats.Failed > 0 { // ???
		return true
	}

	return false
}

func after(id int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	for i, r := range response.Items {
		for k, v := range r {
			if len(v.Result) == 0 {
				log.Printf("%v error %v for input %v", k, v.Error, requests[i])
			}
		}
	}
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
		if input.EventID != "skip" {
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
	}

	// fetch the doc from elastic
	detail := strings.ToLower(r.URL.Query().Get("detail"))
	if detail == "1" || detail == "true" { // return complete doc
		getRequest := esClient.Get().
			Index(index).
			Id(input.EventID).
			Pretty(false)
		getResult, _ := getRequest.Do(ctx)
		// if err != nil {
		// 	w.WriteHeader(http.StatusInternalServerError)
		// 	fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -121\"}")
		// 	log.Fatalf("Error fetching from elastic: %v", err)
		// }
		if !getResult.Found {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "{success: false, message: \"Not found\"}")
			log.Printf("Not found: %v", input.EventID)
		} else {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, string(getResult.Source))
		}
	} else if detail == "2" || detail == "true" {
		getRequest := esClient.Get().
			Index(index).
			Id(input.CustomerID).
			Pretty(false)
		getResult, _ := getRequest.Do(ctx)
		// if err != nil {
		// 	w.WriteHeader(http.StatusInternalServerError)
		// 	fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -121\"}")
		// 	log.Fatalf("Error fetching from elastic: %v", err)
		// }
		if !getResult.Found {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "{success: false, message: \"Not found\"}")
			log.Printf("Not found: %v", input.CustomerID)
		} else {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, string(getResult.Source))
		}
	} else {
		sourceFilter := elastic.NewFetchSourceContext(true).Include("counts", "id", "requestedAt", "processingBegin", "processingEnd", "customerId", "inputFileName", "statusLabel")
		getRequest := esClient.Get().
			Index(index).
			Id(input.EventID).
			FetchSourceContext(sourceFilter).
			Pretty(false)

		getResult, err := getRequest.Do(ctx)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -121\"}")
			log.Fatalf("Error fetching from elastic: %v", err)
		}
		if !getResult.Found {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "{success: false, message: \"Result not found\"}")
			log.Printf("Not found: %v", input.EventID)
		} else {
			var report FileReport
			err = json.Unmarshal([]byte(getResult.Source), &report)
			if err != nil {
				log.Printf("Error unmarshaling %v", err)
			}
			var cleaned []CounterGroup
			for _, counter := range report.Counts {
				newCounter := CounterGroup{
					Group: counter.Group,
					Items: []KeyCounter{},
				}
				for _, item := range counter.Items {
					if !strings.Contains(item.Key, ":") {
						newCounter.Items = append(newCounter.Items, item)
					}
				}
				cleaned = append(cleaned, newCounter)
			}
			report.Counts = cleaned
			js, _ := json.Marshal(report)
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, string(js))
		}
	}
}

package streamer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/profiler"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/google/uuid"
	"github.com/h2non/filetype"
	"github.com/jfyne/csvd"
	"github.com/tealeg/xlsx"
	"go.opencensus.io/trace"
)

// ProjectID is the GCP Project ID
var ProjectID = os.Getenv("PROJECTID")

// PubsubTopic is the pubsub topic to publish to
var PubsubTopic = os.Getenv("PUBSUBTOPIC")

// NameSpaceRecordTemplate is the namespace to write the original records to
var NameSpaceRecordTemplate = os.Getenv("NAMESPACETEMPLATE")

// KindRecordTemplate is the kind to write the original records to
var KindRecordTemplate = os.Getenv("KINDTEMPLATTE")

// HeuristicRecordTemplate is the kind to write heuristics to
var HeuristicRecordTemplate = os.Getenv("HEURISTICSTEMPLATE")

// NERRecordTemplate is the kind to write ner data to
var NERRecordTemplate = os.Getenv("NERTEMPLATE")

// ERRRecordTemplate is the kind to write err data to
var ERRRecordTemplate = os.Getenv("ERRTEMPLATE")

// NERApiEndpoint url for the ner endpoint
var NERApiEndpoint = os.Getenv("NERENDPOINT")

// NameSpaceRequest is the namespace of the streamer request
var NameSpaceRequest = os.Getenv("NAMESPACEREQUEST")

// GCSEvent contains GS event
type GCSEvent struct {
	Bucket         string    `json:"bucket"`
	Name           string    `json:"name"`
	Metageneration string    `json:"metageneration"`
	ResourceState  string    `json:"resourceState"`
	TimeCreated    time.Time `json:"timeCreated"`
	Updated        time.Time `json:"updated"`
}

// Request contains a record for the request
type Request struct {
	CustomerID   int64
	RequestID    string
	Organization string
	Source       string
	FetchURL     string
	FilePath     string
	Status       string
	SubmittedAt  time.Time
	ProcessedAt  time.Time
}

type IDColumn struct {
	Source   string
	IDColumn string
}

// Record is a dynamic map of the profile results
type Record map[string]interface {
}

// Load a datastore field
func (d *Record) Load(props []datastore.Property) error {
	// Note: you might want to clear current values from the map or create a new map
	for _, p := range props {
		(*d)[p.Name] = p.Value
	}
	return nil
}

// Save a datastore field
func (d *Record) Save() (props []datastore.Property, err error) {
	for k, v := range *d {
		props = append(props, datastore.Property{Name: k, Value: v})
	}
	return
}

// RenameDuplicateColumns renames duplicate columns
func RenameDuplicateColumns(s []string) []string {
	m := make(map[string]int)
	var result []string
	for _, item := range s {
		if _, ok := m[item]; ok {
			m[item]++
			result = append(result, item+"_"+strconv.Itoa(m[item]))

		} else {
			m[item] = 0
			result = append(result, item)
		}
	}

	return result
}

func getCsvMap(headers []string, data [][]string) map[string][]string {
	csvMap := make(map[string][]string)
	for j, col := range headers {
		for index := 0; index < len(data); index++ {
			csvMap[col] = append(csvMap[col], data[index][j])
		}
	}
	return csvMap
}

func contains(slice []string, item string) bool {
	set := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}
	_, ok := set[item]
	return ok
}

func uniqueElements(s []string) []string {
	unique := make(map[string]bool, len(s))
	us := make([]string, len(unique))
	for _, elem := range s {
		if len(elem) != 0 {
			if !unique[elem] {
				us = append(us, elem)
				unique[elem] = true
			}
		}
	}
	return us
}

type ColumnStats struct {
	rows      int
	unique    int
	populated float64
	min       string
	max       string
}

func getColumnStats(column []string) map[string]string {
	stats := map[string]string{"rows": strconv.Itoa(len(column)), "unique": strconv.Itoa(len(uniqueElements(column)))}
	emptyCounter := 0
	for i, v := range column {
		if v == "" {
			emptyCounter++
			continue
		}
		if i == 0 || v < stats["min"] {
			stats["min"] = v
		}
		if i == 0 || v > stats["max"] {
			stats["max"] = v
		}
	}
	stats["populated"] = fmt.Sprintf("%g", float64(emptyCounter)/float64(len(column)))
	return stats
}
func flattenStats(colStats map[string]map[string]string) map[string]string {
	flatenned := make(map[string]string)
	for colName, stats := range colStats {
		for key, value := range stats {
			flatKey := strings.Join([]string{colName, key}, ".")
			flatenned[flatKey] = value
		}
	}
	return flatenned
}

//Profiler data type to support "dynamic objects" saving on datastore
type Profiler map[string]interface{}

//Load unpacks the datastore properties to the map
func (d *Profiler) Load(props []datastore.Property) error {
	// Note: you might want to clear current values from the map or create a new map
	for _, p := range props {
		(*d)[p.Name] = p.Value
	}
	return nil
}

//Save turns the interface map to a datastore.property map
func (d *Profiler) Save() (props []datastore.Property, err error) {
	for k, v := range *d {
		props = append(props, datastore.Property{Name: k, Value: v})
	}
	return
}

func getProfilerStats(file string, columns int, columnHeaders []string, colStats map[string]map[string]string) Profiler {
	flatColStats := flattenStats(colStats)

	owner, requestFile := path.Split(file)
	request := strings.Trim(requestFile, path.Ext(requestFile))

	profile := Profiler{
		"file":          file,
		"request":       request,
		"owner":         owner,
		"columns":       columns,
		"columnHeaders": strings.Join(columnHeaders, ","),
	}
	for key, value := range flatColStats {
		profile[key] = fmt.Sprintf("%s", value)
	}
	return profile
}

type ERR struct {
	Address1        int `json:"Address1"`
	Address2        int `json:"Address2"`
	Age             int `json:"Age"`
	Birthday        int `json:"Birthday"`
	City            int `json:"City"`
	Country         int `json:"Country"`
	County          int `json:"County"`
	Email           int `json:"Email"`
	FirstName       int `json:"FirstName"`
	FullName        int `json:"FullName"`
	Gender          int `json:"Gender"`
	LastName        int `json:"LastName"`
	MiddleName      int `json:"MiddleName"`
	ParentEmail     int `json:"ParentEmail"`
	ParentFirstName int `json:"ParentFirstName"`
	ParentLastName  int `json:"ParentLastName"`
	ParentName      int `json:"ParentName"`
	Phone           int `json:"Phone"`
	State           int `json:"State"`
	Suffix          int `json:"Suffix"`
	ZipCode         int `json:"ZipCode"`
	TrustedID       int `json:"TrustedID"`
	Title           int `json:"Title"`
	Role            int `json:"Role"`
}

type NERcolumns struct {
	ColumnName  string             `json:"ColumnName"`
	NEREntities map[string]float64 `json:"NEREntities"`
}
type NERresponse struct {
	Columns     []NERcolumns `json:"Columns"`
	ElapsedTime float64      `json:"ElapsedTime"`
	Owner       string       `json:"Owner"`
	Source      string       `json:"Source"`
	TimeStamp   string       `json:"TimeStamp"`
}

type NERrequest struct {
	Owner  string
	Source string
	Data   map[string][]string
}
type NERentry map[string]interface{}

type Output struct {
	Owner     int64          `json:"Owner"`
	Source    string         `json:"Source"`
	Request   string         `json:"Request"`
	Row       int            `json:"Row"`
	Columns   []OutputColumn `json:"Columns"`
	TimeStamp string         `json:"TimeStamp"`
}

type OutputColumn struct {
	Name  string             `json:"Name"`
	Value string             `json:"Value"`
	ERR   ERR                `json:"ERR"`
	NER   map[string]float64 `json:"NER"`
}

//Load unpacks the datastore properties to the map
func (d *NERentry) Load(props []datastore.Property) error {
	// Note: you might want to clear current values from the map or create a new map
	for _, p := range props {
		(*d)[p.Name] = p.Value
	}
	return nil
}

//Save turns the interface map to a datastore.property map
func (d *NERentry) Save() (props []datastore.Property, err error) {
	for k, v := range *d {
		props = append(props, datastore.Property{Name: k, Value: v})
	}
	return
}
func getNERresponse(nerData NERrequest) NERresponse {
	jsonValue, err := json.Marshal(nerData)
	log.Printf("calling NER endpoint with %v", nerData)
	if err != nil {
		log.Panicf("Could not convert the NERrequest to json: %v", err)
	}
	var structResponse NERresponse
	response, err := http.Post(NERApiEndpoint, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		log.Fatalf("The NER request failed: %v", err)
	} else {
		if response.StatusCode != 200 {
			log.Fatalf("NER request failed, status code:%v", response.StatusCode)
		}
		data, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatalf("Couldn't read the NER response: %v", err)
		}
		log.Printf("ner response %v", string(data))
		json.Unmarshal(data, &structResponse)
	}
	return structResponse
}
func getNERentry(structResponse NERresponse) NERentry {
	var nerEntry = NERentry{
		"ElapsedTime": structResponse.ElapsedTime,
		"Owner":       structResponse.Owner,
		"Source":      structResponse.Source,
		"TimeStamp":   structResponse.TimeStamp,
	}
	//flatten the columns
	for _, col := range structResponse.Columns {
		for key, value := range col.NEREntities {
			nerEntry["columns."+col.ColumnName+"."+key] = value
		}
	}

	return nerEntry
}

// FileStreamer is the main function
func FileStreamer(ctx context.Context, e GCSEvent) error {
	if err := profiler.Start(profiler.Config{
		Service:        NameSpaceRequest,
		ServiceVersion: "1.0.0",
	}); err != nil {
		log.Panicf("Failed to start profiling client: %v", err)
	}
	log.Printf("GS triggerred on file named %v created on %v\n", e.Name, e.TimeCreated)

	exporter, err := stackdriver.NewExporter(stackdriver.Options{})
	if err != nil {
		log.Fatalf("Failed to start tracer client: %v", err)
	}
	trace.RegisterExporter(exporter)

	sbclient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to create storage client: %v", err)
		return nil
	}
	bucket := sbclient.Bucket(e.Bucket)
	file := bucket.Object(e.Name)

	reader, err := file.NewReader(ctx)
	if err != nil {
		log.Fatalf("unable to open file from bucket %q, file %q: %v", e.Bucket, e.Name, err)
		return nil
	}
	defer reader.Close()
	slurp, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatalf("readFile: unable to read data from bucket %q, file %q: %v", e.Bucket, e.Name, err)
		return nil
	}

	contentType := http.DetectContentType(slurp)
	log.Printf("http detected file type as %v", contentType)

	fileKind, _ := filetype.Match(slurp)
	if fileKind == filetype.Unknown {
		log.Printf("filetype detection: unknown file type, treat as text")
	} else {
		log.Printf("filetype detection: detected file type: %v", fileKind.Extension)
	}

	fileSize := reader.Size()
	log.Printf("read %v bytes from the file", fileSize)
	if fileSize < 1 {
		log.Fatal("Unable to process an empty file.")
	}
	var headers []string
	var records [][]string

	// assume it is excel file if it is sniffed by http as application/zip
	// if contentType == "application/zip" {
	if fileKind.Extension == "xlsx" {
		xlsxFile, err := xlsx.OpenBinary(slurp)
		if err != nil {
			log.Fatalf("unable to parse xlsx: %v", err)
			return nil
		}
		sheetData, err := xlsxFile.ToSlice()
		if err != nil {
			return fmt.Errorf("unable to read excel data: %v", err)
		}

		// assume data is in sheet 0
		headers = sheetData[0][0]
		records = sheetData[0][1:]
	} else {
		// open a csv reader
		fileReader := bytes.NewReader(slurp)

		// Use the custom sniffer to parse the CSV
		csvReader := csvd.NewReader(fileReader)
		csvReader.FieldsPerRecord = -1
		csvHeader, err := csvReader.Read()
		if err != nil {
			log.Fatalf("unable to read header: %v", err)
			return nil
		}
		csvRecords, err := csvReader.ReadAll()
		if err != nil {
			log.Fatalf("unable to read file content: %v", err)
			return nil
		}
		headers = csvHeader
		records = csvRecords
	}

	headers = RenameDuplicateColumns(headers)
	errResult := make(map[string]ERR)
	for _, header := range headers {
		var err ERR
		key := strings.ToLower(header)
		switch key {
		case "fname", "f name", "first name", "name first", "first_name", "first":
			err.FirstName = 1
		case "lname", "lname ", "l name ", "l name", "last name", "name last", "last":
			err.LastName = 1
		case "mi", "mi ", "mname", "m", "middle name":
			err.MiddleName = 1
		case "suffix", "jr., iii, etc.":
			err.Suffix = 1
		case "ad", "ad1", "ad1 ", "add1", "add 1", "address 1", "ad 1", "address line 1", "street line 1", "street address 1", "address1", "street", "street_line1", "street address line 1":
			err.Address1 = 1
		case "ad2", "add2", "ad 2", "address 2", "address line 2", "street line 2", "street address 2", "address2", "street_line2", "street 2", "street address line 2":
			err.Address2 = 1
		case "city", "city ", "street city":
			err.City = 1
		case "state", "st", "state ", "state_province", "st ", "state province", "street state":
			err.State = 1
		case "zip", "zip code", "zip ", "postal_code", "postal code", "zip postcode", "street zip":
			err.ZipCode = 1
		case "citystzip", "city/st/zip ":
			err.City = 1
			err.State = 1
			err.ZipCode = 1
		case "county":
			err.County = 1
		case "country", "country (blank for us)":
			err.Country = 1
		case "email", "student email", "email ", "email1", "emali address", "stu_email", "student e mail", "studentemail", "student personal email address", "student emails", "student e-mail", "student personal email", "student email address", "email2", "email_address_2", "student school email":
			err.Email = 1
		case "par_email", "par_email1", "parent e-mail", "par email", "parent email", "parent email address", "par_email2":
			err.Email = 1
			err.ParentEmail = 1
		case "gender", "m/f":
			err.Gender = 1
		case "pfname", "pfname1", "pfname2":
			err.ParentFirstName = 1
		case "plname", "plname1", "plname2":
			err.ParentLastName = 1
		case "phone", "phone1", "hphone", "cphone", "mphone":
			err.Phone = 1
		case "bday", "birthday":
			err.Birthday = 1
		case "age":
			err.Age = 1
		case "pname", "pname1", "pname2", "pname 1", "pname 2":
			err.ParentFirstName = 1
			err.ParentLastName = 1
			err.ParentName = 1
		case "fullname", "full name":
			err.FullName = 1
			err.FirstName = 1
			err.LastName = 1
		}
		if strings.Contains(key, "first") {
			err.FirstName = 1
		}
		if strings.Contains(key, "last") {
			err.LastName = 1
		}
		if strings.Contains(key, "country") {
			err.Country = 1
		}
		if strings.Contains(key, "email") {
			err.Email = 1
		}
		if strings.Contains(key, "class") || strings.Contains(key, "year") || strings.Contains(key, "class year") {
			err.Title = 1
		}
		if strings.Contains(key, "address") {
			err.Address1 = 1
		}
		if strings.Contains(key, "city") {
			err.City = 1
		}
		if strings.Contains(key, "state") {
			err.State = 1
		}
		if strings.Contains(key, "zip") {
			err.ZipCode = 1
		}
		if strings.Contains(key, "phone") {
			err.Phone = 1
		}

		if strings.Contains(key, "parent") || strings.Contains(key, "emergency") || strings.Contains(key, "contact") || strings.Contains(key, "father") || strings.Contains(key, "mother") {
			err.Role = 1
		}

		errResult[header] = err
	}
	errJson, _ := json.Marshal(errResult)
	log.Printf("ERR output %v", string(errJson))

	fileName := strings.TrimSuffix(e.Name, filepath.Ext(e.Name))
	fileDetail := strings.Split(fileName, "/")
	_, requestID := fileDetail[0], fileDetail[1]

	// load the original request
	dsClient, err := datastore.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatalf("Error accessing datastore: %v", err)
		return nil
	}

	var requests []Request
	query := datastore.NewQuery("Request").Namespace(NameSpaceRequest)
	query.Filter("RequestID =", requestID).Limit(1)

	if _, err := dsClient.GetAll(ctx, query, &requests); err != nil {
		log.Fatalf("Error querying request: %v", err)
		return nil
	}
	if len(requests) == 0 {
		log.Fatalf("Unable to locate request: %v", err)
		return nil
	}

	btInstanceID := "wemade-" + string(requests[0].CustomerID)
	btClient, err := bigtable.NewClient(ctx, ProjectID, btInstanceID)
	if err != nil {
		log.Fatalf("Error accessing bigtable: %v", err)
		return nil
	}
	log.Printf("btClient obtained, %v", btClient)

	// get the namespace
	var recordNS bytes.Buffer
	dsNstemplate, err := template.New("requests").Parse(NameSpaceRecordTemplate)
	if err != nil {
		log.Fatalf("Unable to parse text template: %v", err)
		return nil
	}
	if err := dsNstemplate.Execute(&recordNS, requests[0]); err != nil {
		return err
	}

	var IDColumns []IDColumn
	idQuery := datastore.NewQuery("id-column").Namespace(recordNS.String())
	query.Filter("Source =", requests[0].Source)
	IDColumnList := make(map[string]bool)
	if _, err := dsClient.GetAll(ctx, idQuery, &IDColumns); err != nil {
		log.Fatalf("Error querying idcolumns: %v", err)
		return nil
	}
	if len(IDColumns) > 0 {
		for _, p := range IDColumns {
			IDColumnList[strings.ToLower(p.IDColumn)] = true
		}

	}

	for _, header := range headers {
		var err ERR
		key := strings.ToLower(header)

		err = errResult[header]
		if _, ok := IDColumnList[key]; ok {
			err.TrustedID = 1
			errResult[header] = err
		}
	}

	psclient, err := pubsub.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
		return nil
	}
	pstopic := psclient.Topic(PubsubTopic)
	log.Printf("pubsub topic is %v", pstopic)

	var recordKind bytes.Buffer
	dsKindtemplate, err := template.New("requests").Parse(KindRecordTemplate)
	if err != nil {
		log.Fatalf("Unable to parse text template: %v", err)
		return nil
	}
	if err := dsKindtemplate.Execute(&recordKind, requests[0]); err != nil {
		return err
	}

	log.Printf("Storing source records with namespace %v and kind %v", recordNS.String(), recordKind.String())
	sourceKey := datastore.IncompleteKey(recordKind.String(), nil)
	sourceKey.Namespace = recordNS.String()
	Records := []interface{}{}
	var keys []*datastore.Key
	for row, d := range records {

		record := Record{}
		record["_wm_owner"] = requests[0].CustomerID
		record["_wm_request"] = requests[0].RequestID
		record["_wm_row"] = strconv.Itoa(row + 1)
		record["_wm_record"] = uuid.New().String()
		record["_wm_timestamp"] = time.Now().Format(time.RFC3339)

		for j, y := range d {
			record[headers[j]] = y
		}

		// store in DS
		keys = append(keys, datastore.IncompleteKey(recordKind.String(), nil))
		Records = append(Records, &record)
		// if _, err := dsClient.Put(ctx, sourceKey, &record); err != nil {

		// 	log.Fatalf("Error storing source record: %v.  record is %v", err, record)
		// }

		// ================================== disabled pubsub for the time being

		// // pub to pubsub
		// recordJSON, err := json.Marshal(record)
		// if err != nil {
		// 	log.Fatalf("Could not convert record to json: %v", err)
		// 	return nil
		// }

		// // push into pubsub
		// psresult := pstopic.Publish(ctx, &pubsub.Message{
		// 	Data: recordJSON,
		// })

		// // psid, err := psresult.Get(ctx)
		// _, err = psresult.Get(ctx)
		// if err != nil {
		// 	log.Fatalf("Could not pub to pubsub: %v", err)
		// 	return nil
		// }
	}
	//Put multi has a 500 element limit
	multiLimit := 500
	for i := 0; i < len(Records); i += multiLimit {
		end := i + multiLimit

		if end > len(Records) {
			end = len(Records)
		}
		_, err = dsClient.PutMulti(ctx, keys[i:end], Records[i:end])
		if err != nil {
			log.Fatalf("Unable to store records: %v", err)
		}
	}

	log.Print("Done storing source records")
	// Heuristics, NER and ERR are handled here
	var heuristicsKind bytes.Buffer
	hKindtemplate, err := template.New("requests").Parse(HeuristicRecordTemplate)
	if err != nil {
		log.Fatalf("Unable to parse heuristic kind template: %v", err)
		return nil
	}
	if err := hKindtemplate.Execute(&heuristicsKind, requests[0]); err != nil {
		return err
	}

	log.Print("Starting with heuristics")
	colStats := make(map[string]map[string]string)
	csvMap := getCsvMap(headers, records)

	for i, col := range headers {
		if contains(headers, col) {
			colStats[col] = getColumnStats(csvMap[col])
		} else {
			colName := fmt.Sprintf("col_%d", i)
			colStats[colName] = getColumnStats(csvMap[col])
		}
	}
	profile := getProfilerStats(fileName, len(headers), headers, colStats)
	profileIKey := datastore.IncompleteKey(heuristicsKind.String(), nil)
	profileIKey.Namespace = recordNS.String()
	_, err = dsClient.Put(ctx, profileIKey, &profile)
	if err != nil {
		log.Fatalf("Error storing profile: %v", err)
	}
	log.Print("Done with heuristics")

	log.Print("Starting with NER")
	var nerKind bytes.Buffer
	nKindtemplate, err := template.New("requests").Parse(NERRecordTemplate)
	if err != nil {
		log.Fatalf("Unable to parse NER kind template: %v", err)
		return nil
	}
	if err := nKindtemplate.Execute(&nerKind, requests[0]); err != nil {
		return err
	}
	nerRequest := NERrequest{
		Owner:  fmt.Sprintf("%v", requests[0].CustomerID),
		Source: "we-made-streamer",
		Data:   csvMap,
	}
	log.Print("Getting NER responses")
	nerResponse := getNERresponse(nerRequest)

	nerResult := make(map[string]map[string]float64)
	for _, col := range nerResponse.Columns {
		nerResult[col.ColumnName] = col.NEREntities
	}
	log.Printf("%v", nerResult)
	nerEntry := getNERentry(nerResponse)
	nerIKey := datastore.IncompleteKey(nerKind.String(), nil)
	nerIKey.Namespace = recordNS.String()
	_, err = dsClient.Put(ctx, nerIKey, &nerEntry)
	if err != nil {
		log.Fatalf("Error storing NER data: %v", err)
	}
	log.Print("Done with NER")

	log.Print("Starting with ERR")
	var errKind bytes.Buffer
	eKindtemplate, err := template.New("requests").Parse(ERRRecordTemplate)
	if err != nil {
		log.Fatalf("Unable to parse ERR kind template: %v", err)
		return nil
	}
	if err := eKindtemplate.Execute(&errKind, requests[0]); err != nil {
		return err
	}

	log.Print("Done with NER")

	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://104.198.136.122:9200",
		},
		Username: "elastic",
		Password: "TsLv8BtM",
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// let's pub/store the records
	for row, d := range records {
		output := Output{}
		output.Owner = requests[0].CustomerID
		output.Request = requestID
		output.Source = requests[0].Source
		output.Row = row
		output.TimeStamp = requests[0].SubmittedAt.String()
		var outputColumns []OutputColumn

		for j, y := range d {
			var outputColumn OutputColumn
			outputColumn.Name = headers[j]
			outputColumn.Value = y
			outputColumn.ERR = errResult[headers[j]]
			outputColumn.NER = nerResult[headers[j]]
			outputColumns = append(outputColumns, outputColumn)
		}
		output.Columns = outputColumns
		outputJSON, _ := json.Marshal(output)

		// push into pubsub
		psresult := pstopic.Publish(ctx, &pubsub.Message{
			Data: outputJSON,
		})

		psid, err := psresult.Get(ctx)
		_, err = psresult.Get(ctx)
		if err != nil {
			log.Fatalf("Could not pub to pubsub: %v", err)
		} else {
			log.Printf("pubbed record %v as message id %v", row, psid)
		}

		docID := requestID + "-" + strconv.Itoa(row)
		req := esapi.IndexRequest{
			Index:        "streamer",
			DocumentType: "record",
			DocumentID:   docID,
			Body:         bytes.NewReader(outputJSON),
			Refresh:      "true",
		}
		res, err := req.Do(ctx, es)
		if err != nil {
			log.Fatalf("Error getting response: %s", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			resB, _ := ioutil.ReadAll(res.Body)
			log.Printf("[%s] Error indexing document ID=%v, Message=%v", res.Status(), docID, string(resB))
		} else {
			resB, _ := ioutil.ReadAll(res.Body)
			log.Printf("[%s] document ID=%v, Message=%v", res.Status(), docID, string(resB))

			// // Deserialize the response into a map.
			// var r map[string]interface{}
			// if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			// 	log.Printf("Error parsing the response body: %s", err)
			// } else {
			// 	// Print the response status and indexed document version.
			// 	log.Printf("[%s] %s; version=%d, response=%v", res.Status(), r["result"], int(r["_version"].(float64)))
			// }
		}

	}

	pstopic.Stop()

	sbclient.Close()
	return nil
}

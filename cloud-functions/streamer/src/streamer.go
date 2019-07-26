// Package streamer contains a series of cloud functions for streamer
package streamer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/h2non/filetype"
	"github.com/jfyne/csvd"
	"github.com/tealeg/xlsx"
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

type UnbufferedReaderAt struct {
	R io.Reader
	N int64
}

func NewUnbufferedReaderAt(r io.Reader) io.ReaderAt {
	return &UnbufferedReaderAt{R: r}
}

func (u *UnbufferedReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off < u.N {
		return 0, errors.New("invalid offset")
	}
	diff := off - u.N
	written, err := io.CopyN(ioutil.Discard, u.R, diff)
	u.N += written
	if err != nil {
		return 0, err
	}

	n, err = u.R.Read(p)
	u.N += int64(n)
	return
}
func getCsvMap(headers []string, data [][]string) map[string][]string {
	csvMap := make(map[string][]string)
	for j, col := range headers {
		for index := 1; index < len(data); index++ {
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
	log.Printf("GS triggerred on file named %v created on %v\n", e.Name, e.TimeCreated)

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

	if fileKind.Extension == ".xlsx" {
		readerAt := NewUnbufferedReaderAt(bytes.NewReader(slurp))
		xlsxFile, err := xlsx.OpenReaderAt(readerAt, fileSize)
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

	log.Printf("headers: %v", headers)
	log.Printf("records: %v", records)

	headers = RenameDuplicateColumns(headers)
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

	psclient, err := pubsub.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
		return nil
	}
	pstopic := psclient.Topic(PubsubTopic)

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
		if _, err := dsClient.Put(ctx, sourceKey, &record); err != nil {

			log.Fatalf("Error storing source record: %v.  record is %v", err, record)
		}

		// pub to pubsub
		recordJSON, err := json.Marshal(record)
		if err != nil {
			log.Fatalf("Could not convert record to json: %v", err)
			return nil
		}

		// push into pubsub
		psresult := pstopic.Publish(ctx, &pubsub.Message{
			Data: recordJSON,
		})

		// psid, err := psresult.Get(ctx)
		_, err = psresult.Get(ctx)
		if err != nil {
			log.Fatalf("Could not pub to pubsub: %v", err)
			return nil
		}
	}
	pstopic.Stop()
	log.Print("Done storing source records")
	// Heuristics and NER are handled here
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
	log.Print(profile)
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
	log.Print("Getting NER entry")
	nerEntry := getNERentry(nerResponse)
	nerIKey := datastore.IncompleteKey(nerKind.String(), nil)
	nerIKey.Namespace = recordNS.String()
	_, err = dsClient.Put(ctx, nerIKey, &nerEntry)
	if err != nil {
		log.Fatalf("Error storing NER data: %v", err)
	}
	log.Print("Done with NER")

	sbclient.Close()
	return nil
}

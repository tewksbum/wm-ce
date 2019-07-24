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
	"os"
	"path"
	"path/filepath"
	"reflect"
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
func getCsvMap(csvData [][]string) map[string][]string {
	columns := csvData[0]
	csvMap := make(map[string][]string)
	for j, col := range columns {
		for index := 1; index < len(csvData); index++ {
			csvMap[col] = append(csvMap[col], csvData[index][j])
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

func getColumnStats(column []string) ColumnStats {
	stats := ColumnStats{rows: len(column), unique: len(uniqueElements(column))}
	//stats should be a struct!

	emptyCounter := 0
	for i, v := range column {
		if i == 0 || v < stats.min {
			stats.min = v
		}
		if i == 0 || v > stats.max {
			stats.max = v
		}
		if v == "" {
			emptyCounter++
		}
	}
	stats.populated = float64(emptyCounter / len(column))
	return stats
}
func flattenStats(colStats map[string]ColumnStats) map[string]interface{} {
	flatenned := make(map[string]interface{})
	for colName, stats := range colStats {
		v := reflect.ValueOf(stats)
		typeOfS := v.Type()
		for i := 0; i < v.NumField(); i++ {
			key := typeOfS.Field(i).Name
			value := v.Field(i).Interface()
			flatKey := strings.Join([]string{colName, key}, ".")
			flatenned[flatKey] = value
		}
	}
	return flatenned
}

func saveProfilerStats(ctx context.Context, kind bytes.Buffer, namespace string, file string, columns []string, columnHeaders []string, colStats map[string]ColumnStats) error {
	flatColStats := flattenStats(colStats)
	dsClient, err := datastore.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatalf("Error accessing datastore: %v", err)
		return err
	}

	owner, requestFile := path.Split(file)
	request := strings.Trim(requestFile, path.Ext(requestFile))

	profile := map[string]string{
		"file":           file,
		"request":        request,
		"owner":          owner,
		"columns":        strings.Join(columns, ","),
		"column_headers": strings.Join(columnHeaders, ","),
	}
	for key, value := range flatColStats {
		profile[key] = fmt.Sprintf("%f", value)
	}
	log.Print(profile)
	incompleteKey := datastore.IncompleteKey(kind.String(), nil)
	incompleteKey.Namespace = namespace
	_, putErr := dsClient.Put(ctx, incompleteKey, &profile)
	return putErr
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
	if 0 > fileSize {
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

	var heuristicsKind bytes.Buffer
	hKindtemplate, err := template.New("requests").Parse(HeuristicRecordTemplate)
	if err != nil {
		log.Fatalf("Unable to parse text template: %v", err)
		return nil
	}
	if err := hKindtemplate.Execute(&heuristicsKind, requests[0]); err != nil {
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
	colStats := make(map[string]ColumnStats)
	csvMap := getCsvMap(records)
	for i, col := range records[0] {
		if contains(headers, col) {
			colStats[col] = getColumnStats(csvMap[col])
		} else {
			colName := fmt.Sprintf("col_%d", i)
			colStats[colName] = getColumnStats(csvMap[col])
		}
	}
	savErr := saveProfilerStats(ctx, heuristicsKind, recordNS.String(), fileName, records[0], headers, colStats)
	if savErr != nil {
		log.Fatalf("Error storing profile: %v", err)
	}
	sbclient.Close()
	return nil
}
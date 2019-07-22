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

	for item := range m {
		result = append(result, item)
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

// FileStreamer is the main function
func FileStreamer(ctx context.Context, e GCSEvent) error {
	log.Printf("GS trigger filed on file named %v created on %v\n", e.Name, e.TimeCreated)

	sbclient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %v", err)
	}
	bucket := sbclient.Bucket(e.Bucket)
	file := bucket.Object(e.Name)

	reader, err := file.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("unable to open file from bucket %q, file %q: %v", e.Bucket, e.Name, err)
	}
	defer reader.Close()
	slurp, err := ioutil.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("readFile: unable to read data from bucket %q, file %q: %v", e.Bucket, e.Name, err)
	}

	fileKind, _ := filetype.Match(slurp)
	if fileKind == filetype.Unknown {
		log.Printf("filetype detection: unknown file type, treat as text")
	} else {
		log.Printf("filetype detection: detected file type: %v", fileKind.Extension)
	}

	fileSize := reader.Size()
	log.Printf("read %v bytes from the file", fileSize)

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
		if _, err := dsClient.Put(ctx, sourceKey, record); err != nil {
			log.Fatalf("Error storing source record: %v", err)
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
		psid, err := psresult.Get(ctx)
		if err != nil {
			log.Fatalf("Could not pub to pubsub: %v", err)
			return nil
		}
		fmt.Printf("Published msg %v; ID: %v\n", recordJSON, psid)
	}

	sbclient.Close()

	return nil
}

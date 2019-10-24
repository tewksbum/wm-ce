package fileprocessor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"

	"github.com/google/uuid"
	"github.com/h2non/filetype"
	"github.com/jfyne/csvd"
	"github.com/tealeg/xlsx"
)

// BLACKLIST is a list of json nodes that will be ignored
var BLACKLIST = []string{"VENDOR"}

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

type Signature struct {
	OwnerID   int64  `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	RecordID  string `json:"recordId"`
}

type Input struct {
	Signature   Signature              `json:"signature"`
	Passthrough map[string]string      `json:"passthrough"`
	Attributes  map[string]string      `json:"attributes"`
	EventData   map[string]interface{} `json:"eventData"`
}

type Output struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Fields      map[string]string `json:"fields"`
}

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")

var PubSubTopic = os.Getenv("PSOUTPUT")

// BucketName the GS storage bucket name
var BucketName = os.Getenv("GSBUCKET")

// global vars
var ctx context.Context
var ps *pubsub.Client
var topic *pubsub.Topic
var sb *storage.Client

func init() {
	ctx = context.Background()
	sb, _ = storage.NewClient(ctx)
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	log.Printf("init completed, pubsub topic name: %v", topic)
}

/* ProcessFile takes in a file
chops it into records
And Pubs to a topic
*/
func ProcessFile(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	// get the file
	if url, ok := input.EventData["fileUrl"]; ok {
		resp, err := http.Get(fmt.Sprintf("%v", url))
		if err != nil {
			log.Fatalf("File cannot be downloaded %V", url)
		}

		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			fileBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatal(err)
			}

			// store the file without an extension, the extension will be detected inside streamer
			fileName := strconv.FormatInt(input.Signature.OwnerID, 10) + "/" + input.Signature.EventID
			bucket := sb.Bucket(BucketName)
			file := bucket.Object(fileName)
			writer := file.NewWriter(ctx)

			if _, err := io.Copy(writer, bytes.NewReader(fileBytes)); err != nil {
				log.Fatalf("File cannot be copied to bucket %v", err)
			}
			if err := writer.Close(); err != nil {
				log.Fatalf("Failed to close bucket write stream %v", err)
			}

			// performs content type detection with http module as well as filetype
			contentType := http.DetectContentType(fileBytes)
			log.Printf("http detected file type as %v", contentType)

			fileKind, _ := filetype.Match(fileBytes)
			if fileKind == filetype.Unknown {
				log.Printf("filetype detection: unknown file type, treat as text")
			} else {
				log.Printf("filetype detection: detected file type: %v", fileKind.Extension)
			}

			fileSize := len(fileBytes)
			log.Printf("read %v bytes from the file", fileSize)
			if fileSize < 1 {
				log.Fatal("Unable to process an empty file.")
			}
			var headers []string
			var records [][]string
			var allrows [][]string

			if fileKind.Extension == "xlsx" || contentType == "application/zip" {
				xlsxFile, err := xlsx.OpenBinary(fileBytes)
				if err != nil {
					log.Fatalf("unable to parse xlsx: %v", err)
					return nil
				}
				sheetData, err := xlsxFile.ToSlice()
				if err != nil {
					return fmt.Errorf("unable to read excel data: %v", err)
				}

				// assume data is in sheet 0 of Excel workbook
				allrows = sheetData[0]
			} else {
				// open a csv reader
				fileReader := bytes.NewReader(fileBytes)

				// Use the custom sniffer to parse the CSV
				csvReader := csvd.NewReader(fileReader)
				csvReader.FieldsPerRecord = -1
				allrows, err = csvReader.ReadAll()
				if err != nil {
					log.Fatalf("unable to read header: %v", err)
					return nil
				}
			}
			log.Printf("found %v rows in file", len(allrows))
			// now scan through records
			var maxColumns int
			var maxColumnRowAt int
			for index, row := range allrows {
				cellCount := CountSparseArray(row)
				if cellCount > maxColumns {
					maxColumnRowAt = index
					maxColumns = cellCount
				}
			}

			headers = allrows[maxColumnRowAt]
			records = allrows[maxColumnRowAt+1:]

			headers = EnsureColumnsHaveNames(headers)
			headers = RenameDuplicateColumns(headers)

			// TODO: add NER here

			// push the records into pubsub
			var output Output
			output.Signature = input.Signature
			if len(output.Signature.RecordID) == 0 {
				output.Signature.RecordID = uuid.New().String()
			}
			output.Passthrough = input.Passthrough

			for _, d := range records {
				fields := make(map[string]string)
				for j, y := range d {
					fields[headers[j]] = y
				}
				// append attributes
				for key, value := range input.Attributes {
					fields["Attr."+key] = value
				}

				output.Fields = fields

				// let's pub it
				outputJSON, _ := json.Marshal(output)

				// push into pubsub
				psresult := topic.Publish(ctx, &pubsub.Message{
					Data: outputJSON,
				})

				psid, err := psresult.Get(ctx)
				_, err = psresult.Get(ctx)
				if err != nil {
					log.Fatalf("%v Could not pub to pubsub: %v", input.Signature.EventID, err)
				} else {
					log.Printf("%v pubbed record as message id %v: %v", input.Signature.EventID, psid, string(outputJSON))
				}
			}
		} else {
			log.Fatalf("Unable to fetch file %v, response code %v", url, resp.StatusCode)
		}

	}
	return nil
}

// CountSparseArray count sparse array
func CountSparseArray(inputArray []string) int {
	var counter int
	for _, c := range inputArray {
		if len(c) > 0 {
			counter++
		}
	}
	return counter
}

// EnsureColumnsHaveNames ensures the columns have a name
func EnsureColumnsHaveNames(s []string) []string {
	var result []string
	for _, item := range s {
		if len(item) == 0 {
			result = append(result, "Empty")
		} else {
			result = append(result, item)
		}
	}
	return result
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

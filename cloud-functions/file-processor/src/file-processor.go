package fileprocessor

// yuck...

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"

	"github.com/gomodule/redigo/redis"
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
	OwnerID   string `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	RecordID  string `json:"recordId"`
	RowNumber int    `json:"rowNum"`
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
	Attributes  map[string]string `json:"attributes"`
}

// NERcolumns coloumns for NER
type NERcolumns struct {
	ColumnName  string             `json:"ColumnName"`
	NEREntities map[string]float64 `json:"NEREntities"`
}

//NERresponse response
type NERresponse struct {
	Columns     []NERcolumns `json:"Columns"`
	ElapsedTime float64      `json:"ElapsedTime"`
	Owner       string       `json:"Owner"`
	Source      string       `json:"Source"`
	TimeStamp   string       `json:"TimeStamp"`
}

type NERCache struct {
	Columns      []NERcolumns `json:"columns"`
	TimeStamp    time.Time    `json:"time"`
	ApplyCounter int          `json:"counter"`
	Recompute    bool         `json:"dirty"`
	Source       string       `json:"source"`
}

// NERrequest request
type NERrequest struct {
	Owner  string
	Source string
	Data   map[string][]string
}

// NERentry entry
type NERentry map[string]interface{}

type FileReport struct {
	ID              string                `json:"id,omitempty"`
	ProcessingBegin time.Time             `json:"processingBegin,omitempty"`
	StatusLabel     string                `json:"statusLabel,omitempty"`
	StatusBy        string                `json:"statusBy,omitempty"`
	StatusTime      time.Time             `json:"statusTime,omitempty"`
	Errors          []ReportError         `json:"errors"`
	Warnings        []ReportError         `json:"warnings"`
	Audits          []ReportError         `json:"audits"`
	Counters        []ReportCounter       `json:"counters"`
	InputStatistics map[string]ColumnStat `json:"inputStats"`
	Columns         []string              `json:"columns,omitempty"`
	RecordList      []RecordDetail        `json:"recordList,omitempty"`
}

// ReportError stores errors and warnings
type ReportError struct {
	FileLevel bool   `json:"file_level,omitempty"`
	Row       int    `json:"row,omitempty"`
	RecordID  string `json:"record_id,omitempty"`
	Field     string `json:"field,omitempty"`
	Value     string `json:"value,omitempty"`
	Message   string `json:"message,omitempty"`
}

// RecordDetail stores detail about a record
type RecordDetail struct {
	ID          string    `json:"id,omitempty"`
	RowNumber   int       `json:"row,omitempty"`
	CreatedOn   time.Time `json:"createdOn,omitempty"`
	Disposition string    `json:"disposition,omitempty"`
	Fibers      []string  `json:"fibers"`
	Sets        []string  `json:"sets"`
}

// ReportCounter stores record, purge, murge
type ReportCounter struct {
	Type      string `json:"type,omitempty"`
	Name      string `json:"name,omitempty"`
	Count     int    `json:"count,omitempty"`
	Increment bool   `json:"inc,omitempty"`
}

// ReportStat stores metric such as sparsity
type ReportStat struct {
	Field  string `json:"field,omitempty"`
	Metric string `json:"metric,omitempty"`
	Value  int    `json:"value,omitempty"`
}

type ColumnStat struct {
	Name     string  `json:"name"`
	Min      string  `json:"min"`
	Max      string  `json:"max"`
	Sparsity float32 `json:"sparsity"`
}
type MatchKeyStat struct {
	Name     string  `json:"name"`
	Sparsity float32 `json:"sparsity"`
}

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")

// BucketName the GS storage bucket name
var BucketName = os.Getenv("GSBUCKET")

var NERApi = os.Getenv("NERAPI")

var reNewline = regexp.MustCompile(`\r?\n`)
var reNewline2 = regexp.MustCompile(`_x000d_`)
var reStartsWithNumber = regexp.MustCompile(`^[0-9]`)
var reStartsWithOrdinalNumber = regexp.MustCompile(`^(?i)(1st|2nd|3rd)`)

var redisTransientExpiration = 3600 * 24

// global vars
var ctx context.Context
var ps *pubsub.Client
var topic *pubsub.Topic
var status *pubsub.Topic
var sb *storage.Client
var msp *redis.Pool
var topicR *pubsub.Topic
var cfName = os.Getenv("FUNCTION_NAME")

func init() {
	ctx = context.Background()
	sb, _ = storage.NewClient(ctx)
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(os.Getenv("PSOUTPUT"))
	topicR = ps.Topic(os.Getenv("PSREPORT"))
	// topic.PublishSettings.DelayThreshold = 200 * time.Millisecond
	status = ps.Topic(os.Getenv("PSSTATUS"))
	// status.PublishSettings.DelayThreshold = 5 * time.Minute
	msp = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", os.Getenv("MEMSTORE")) },
	}

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
	log.Printf("Input %v", string(m.Data))

	RowLimit := 0
	if row, ok := input.EventData["maxRows"]; ok {
		RowLimit = int(row.(float64))
	}

	// populate report
	if len(cfName) == 0 {
		cfName = "file-processor"
	}
	report := FileReport{
		ID:              input.Signature.EventID,
		ProcessingBegin: time.Now(),
		StatusLabel:     "processing file began",
		StatusBy:        cfName,
		StatusTime:      time.Now(),
	}
	publishReport(&report, cfName)

	// get the file
	if fileURL, ok := input.EventData["fileUrl"]; ok {
		log.Printf("fetching file %v", fileURL)
		resp, err := http.Get(fmt.Sprintf("%v", fileURL))
		if err != nil {
			input.EventData["message"] = "File cannot be downloaded"
			input.EventData["status"] = "Error"
			report := FileReport{
				ID:          input.Signature.EventID,
				StatusLabel: "error: file cannot be downloaded",
				StatusBy:    cfName,
				StatusTime:  time.Now(),
				Errors: []ReportError{
					ReportError{
						FileLevel: true,
						Value:     fmt.Sprintf("%v", fileURL),
						Message:   "file cannot be downloaded",
					},
				},
			}
			publishReport(&report, cfName)

			statusJSON, _ := json.Marshal(input)
			psresult := status.Publish(ctx, &pubsub.Message{
				Data: statusJSON,
			})
			_, err = psresult.Get(ctx)
			if err != nil {
				log.Printf("ERROR %v Could not pub status to pubsub: %v", input.Signature.EventID, err)
			}

			log.Fatalf("File cannot be downloaded %v", fileURL)
		}

		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			fileBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatal(err)
			}

			// store the file without an extension, the extension will be detected inside streamer
			fileName := input.Signature.OwnerID + "/" + input.Signature.EventID
			bucket := sb.Bucket(BucketName)
			file := bucket.Object(fileName)
			writer := file.NewWriter(ctx)

			if _, err := io.Copy(writer, bytes.NewReader(fileBytes)); err != nil {
				report := FileReport{
					ID:          input.Signature.EventID,
					StatusLabel: "error: file cannot be copied to wemade storage",
					StatusBy:    cfName,
					StatusTime:  time.Now(),
					Errors: []ReportError{
						ReportError{
							FileLevel: true,
							Value:     fmt.Sprintf("%v", fileURL),
							Message:   "file cannot be copied to wemade storage",
						},
					},
				}
				publishReport(&report, cfName)

				input.EventData["message"] = "File cannot be copied to bucket"
				input.EventData["status"] = "Error"
				statusJSON, _ := json.Marshal(input)
				psresult := status.Publish(ctx, &pubsub.Message{
					Data: statusJSON,
				})
				_, err = psresult.Get(ctx)
				if err != nil {
					log.Printf("ERROR %v Could not pub status to pubsub: %v", input.Signature.EventID, err)
				}
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
				report := FileReport{
					ID:          input.Signature.EventID,
					StatusLabel: "error: file is empty",
					StatusBy:    cfName,
					StatusTime:  time.Now(),
					Errors: []ReportError{
						ReportError{
							FileLevel: true,
							Value:     fmt.Sprintf("%v", fileURL),
							Message:   "file is empty",
						},
					},
				}
				publishReport(&report, cfName)

				input.EventData["message"] = "File is empty"
				input.EventData["status"] = "Error"
				statusJSON, _ := json.Marshal(input)
				psresult := status.Publish(ctx, &pubsub.Message{
					Data: statusJSON,
				})
				_, err = psresult.Get(ctx)
				if err != nil {
					log.Printf("ERROR %v Could not pub status to pubsub: %v", input.Signature.EventID, err)
				}

				log.Printf("ERROR file is empty")
				return fmt.Errorf("file is empty")
			}

			var headers []string
			var records [][]string
			var allrows [][]string

			parsedFileName := ""
			parsedFileURL, err := url.Parse(fmt.Sprintf("%v", fileURL))
			if err != nil {
			} else {
				parsedFileName = strings.ToLower(filepath.Base(parsedFileURL.Path))
			}

			{
				report := FileReport{
					ID: input.Signature.EventID,
					Audits: []ReportError{
						ReportError{
							FileLevel: true,
							Value:     fmt.Sprintf("%v", fileURL),
							Message:   fmt.Sprintf("detected file format is %v", fileKind.Extension),
						},
						ReportError{
							FileLevel: true,
							Value:     fmt.Sprintf("%v", fileURL),
							Message:   fmt.Sprintf("detected content type is %v", contentType),
						},
					},
				}
				publishReport(&report, cfName)
			}

			if fileKind.Extension == "xlsx" || contentType == "application/zip" || strings.HasSuffix(parsedFileName, ".xlsx") {
				xlsxFile, err := xlsx.OpenBinary(fileBytes)
				if err != nil {
					report := FileReport{
						ID:          input.Signature.EventID,
						StatusLabel: "error: cannot parse as excel",
						StatusBy:    cfName,
						StatusTime:  time.Now(),
						Errors: []ReportError{
							ReportError{
								FileLevel: true,
								Value:     fmt.Sprintf("%v", fileURL),
								Message:   "cannot parse as excel",
							},
						},
					}
					publishReport(&report, cfName)

					log.Printf("ERROR unable to parse xlsx: %v", err)
					return fmt.Errorf("unable to parse xlsx: %v", err)
				}
				sheetData, err := xlsxFile.ToSlice()
				if err != nil {
					report := FileReport{
						ID:          input.Signature.EventID,
						StatusLabel: "error: cannot read excel sheets",
						StatusBy:    cfName,
						StatusTime:  time.Now(),
						Errors: []ReportError{
							ReportError{
								FileLevel: true,
								Value:     fmt.Sprintf("%v", fileURL),
								Message:   "cannot read excel sheets",
							},
						},
					}
					publishReport(&report, cfName)

					input.EventData["message"] = "Unable to read excel data"
					input.EventData["status"] = "Error"
					statusJSON, _ := json.Marshal(input)
					psresult := status.Publish(ctx, &pubsub.Message{
						Data: statusJSON,
					})
					_, pserr := psresult.Get(ctx)
					if pserr != nil {
						log.Fatalf("%v Could not pub status to pubsub: %v", input.Signature.EventID, err)
					}

					return fmt.Errorf("unable to read excel data: %v", err)
				}

				origSheet, wcSheet, cpSheet := -1, -1, -1
				for i, sheet := range xlsxFile.Sheets {
					switch strings.ToLower(sheet.Name) {
					case "original", "page1", "sheet1":
						origSheet = i
					case "wc", "working copy", "workingcopy", "working":
						wcSheet = i
					case "cp", "upload", "cp upload", "dm_list":
						cpSheet = i
					}
				}

				if cpSheet > -1 {
					log.Printf("Processing cp sheet %v", cpSheet)
					allrows = sheetData[cpSheet]
				} else if wcSheet > -1 {
					log.Printf("Processing wc sheet %v", wcSheet)
					allrows = sheetData[wcSheet]
				} else if origSheet > -1 {
					log.Printf("Processing original sheet %v", origSheet)
					allrows = sheetData[origSheet]
				} else {
					// take the first sheet if we don't find something more interesting
					log.Printf("processing first sheet")
					allrows = sheetData[0]
				}

				if len(allrows) < 5 {
					// read the first sheet
					allrows = sheetData[0]
				}
				// allrows = sheetData[0]
			} else {
				// open a csv reader
				fileReader := bytes.NewReader(fileBytes)

				// Use the custom sniffer to parse the CSV
				csvReader := csvd.NewReader(fileReader)
				csvReader.FieldsPerRecord = -1
				allrows, err = csvReader.ReadAll()
				if err != nil {
					report := FileReport{
						ID:          input.Signature.EventID,
						StatusLabel: "error: cannot parse delimited text",
						StatusBy:    cfName,
						StatusTime:  time.Now(),
						Errors: []ReportError{
							ReportError{
								FileLevel: true,
								Value:     fmt.Sprintf("%v", fileURL),
								Message:   "cannot parse delimited text",
							},
						},
					}
					publishReport(&report, cfName)

					input.EventData["message"] = "Unable to read csv header"
					input.EventData["status"] = "Error"
					statusJSON, _ := json.Marshal(input)
					psresult := status.Publish(ctx, &pubsub.Message{
						Data: statusJSON,
					})
					_, err = psresult.Get(ctx)
					if err != nil {
						log.Fatalf("%v Could not pub status to pubsub: %v", input.Signature.EventID, err)
					}

					log.Printf("unable to read header: %v", err)
					return nil
				}
			}
			log.Printf("found %v rows in file", len(allrows))

			// now scan through records
			// method 1, find the row with the most number of columns, scan the first 20 rows for this
			var maxColumns int
			var maxColumnRowAt int
			for index, row := range allrows {
				cellCount := CountSparseArray(row)
				if cellCount > maxColumns {
					maxColumnRowAt = index
					maxColumns = cellCount
				}
				if index == 20 {
					break
				}
			}
			log.Printf("maxColumnRowAt is %v", maxColumnRowAt)
			// let's back track a little and see if we have just one extra column
			for i := maxColumnRowAt - 1; i >= 0; i-- {
				cellCount := CountSparseArray(allrows[i])
				if maxColumns-cellCount == 1 {
					maxColumnRowAt = i
				}
			}
			log.Printf("maxColumnRowAt is %v", maxColumnRowAt)
			// method 2, scan for a row that "looks like a header"
			var maxHeaderlikeColumns int
			var maxHeaderlikeColumnsRowAt int
			for index, row := range allrows {

				headerCount := CountHeaderlikeCells(row)
				log.Printf("Row %v headerlike column count %v", index, headerCount)
				if headerCount > maxHeaderlikeColumns {
					maxHeaderlikeColumnsRowAt = index
					maxHeaderlikeColumns = headerCount
				}
				if index == 20 {
					break
				}
			}
			log.Printf("headerlike match identified row  %v", maxHeaderlikeColumnsRowAt)
			// no back track for this one

			// // use the lower number
			// if maxHeaderlikeColumnsRowAt < maxColumnRowAt {
			// 	headers = allrows[maxHeaderlikeColumnsRowAt]
			// 	log.Printf("Header row identified by maxHeaderlikeColumnsRowAt is %v", headers)
			// 	records = allrows[maxHeaderlikeColumnsRowAt+1:]
			// } else {
			// 	headers = allrows[maxColumnRowAt]
			// 	log.Printf("Header row identified by maxColumnRowAt is %v", headers)
			// 	records = allrows[maxColumnRowAt+1:]
			// }

			headers = allrows[maxColumnRowAt]
			log.Printf("Header row is %v", headers)
			records = allrows[maxColumnRowAt+1:]

			// attempt to detect if file has no header
			// a. if the header has any column that contains same value that is not blank as the rest of the rows
			// b. if the header contains any column that starts with a number
			headerlessTest2 := false
			headerlessTest1 := false
			for _, h := range headers {
				if len(h) > 0 && reStartsWithNumber.MatchString(h) && !reStartsWithOrdinalNumber.MatchString(h) {
					log.Printf("The header column starts with a number: %v", h)
					headerlessTest2 = true
					break
				}
			}

			if headerlessTest2 {
				log.Printf("%v is headerless (header column starts with a number), stop processing", input.Signature.EventID)
				report := FileReport{
					ID:          input.Signature.EventID,
					StatusLabel: "error: headerless file detected",
					StatusBy:    cfName,
					StatusTime:  time.Now(),
					Errors: []ReportError{
						ReportError{
							FileLevel: true,
							Value:     fmt.Sprintf("%v", fileURL),
							Message:   "headerless file detected",
						},
					},
				}
				publishReport(&report, cfName)

				input.EventData["message"] = "File appears to contain no header row - 1"
				input.EventData["status"] = "Error"
				statusJSON, _ := json.Marshal(input)
				psresult := status.Publish(ctx, &pubsub.Message{
					Data: statusJSON,
				})
				_, err = psresult.Get(ctx)
				if err != nil {
					log.Fatalf("%v Could not pub status to pubsub: %v", input.Signature.EventID, err)
				}

				return nil
			}
			repeatedValueCount := 0
			for i, h := range headers {
				if len(h) > 0 {
					for y, r := range records {
						if len(r) > i && h == r[i] {
							repeatedValueCount++
							log.Printf("%v file has a repeated value row column value: %v %v %v", input.Signature.EventID, y, i, r[i])
						}
					}
				}
			}
			if repeatedValueCount > 10 {
				// trying to spit out specific thing that was an issue...
				headerlessTest1 = true
			}
			if headerlessTest1 {
				log.Printf("%v is headerless (header row value is repeated in records %v times), stop processing", input.Signature.EventID, repeatedValueCount)

				report := FileReport{
					ID:          input.Signature.EventID,
					StatusLabel: "error: headerless file detected",
					StatusBy:    cfName,
					StatusTime:  time.Now(),
					Errors: []ReportError{
						ReportError{
							FileLevel: true,
							Value:     fmt.Sprintf("%v", fileURL),
							Message:   "headerless file detected",
						},
					},
				}
				publishReport(&report, cfName)

				input.EventData["message"] = "File appears to contain no header row - 2"
				input.EventData["status"] = "Error"
				statusJSON, _ := json.Marshal(input)
				psresult := status.Publish(ctx, &pubsub.Message{
					Data: statusJSON,
				})
				_, err = psresult.Get(ctx)
				if err != nil {
					log.Fatalf("%v Could not pub status to pubsub: %v", input.Signature.EventID, err)
				}

				return nil
			}
			headers = EnsureColumnsHaveNames(RenameDuplicateColumns(headers))

			report0 := FileReport{
				ID:          input.Signature.EventID,
				Columns:     headers,
				StatusBy:    cfName,
				StatusTime:  time.Now(),
				StatusLabel: "finished parsing",
				Counters: []ReportCounter{
					ReportCounter{
						Type:      "fileprocessor",
						Name:      "Columns",
						Count:     len(headers),
						Increment: false,
					},
				},
			}
			publishReport(&report0, cfName)
			columnStats := make(map[string]ColumnStat)

			// Call NER API
			NerRequest := NERrequest{
				Owner:  input.Signature.OwnerID,
				Source: "wemade",
				Data:   GetColumnars(headers, records),
			}
			log.Printf("%v Getting NER responses", input.Signature.EventID)
			NerResponse := GetNERresponse(NerRequest)

			// Store NER in Redis if we have a NER
			NerKey := GetNERKey(input.Signature, headers)
			if len(NerResponse.Owner) > 0 {
				PersistNER(NerKey, NerResponse)
			}

			report1 := FileReport{
				ID:          input.Signature.EventID,
				StatusBy:    cfName,
				StatusTime:  time.Now(),
				StatusLabel: "finished name entity recognition",
			}
			publishReport(&report1, cfName)

			// push the records into pubsub
			var output Output
			output.Signature = input.Signature
			output.Attributes = input.Attributes
			output.Passthrough = input.Passthrough

			SetRedisValueWithExpiration([]string{input.Signature.EventID, "records-completed"}, 0)
			SetRedisValueWithExpiration([]string{input.Signature.EventID, "records-deleted"}, 0)
			SetRedisValueWithExpiration([]string{input.Signature.EventID, "fibers-completed"}, 0)
			SetRedisValueWithExpiration([]string{input.Signature.EventID, "fibers-deleted"}, 0)

			recordCount := 0

			// do not randomize record sequence anymore
			// sort.Slice(records, func(i, j int) bool {
			// 	return rand.Int() < rand.Int()
			// })

			report := FileReport{
				ID: input.Signature.EventID,
				Counters: []ReportCounter{
					ReportCounter{
						Type:      "fileprocessor",
						Name:      "Raw",
						Count:     len(records),
						Increment: false,
					},
				},
			}
			publishReport(&report, cfName)

			for r, d := range records {

				output.Signature.RecordID = uuid.New().String()
				output.Signature.RowNumber = r + 1

				if output.Signature.RowNumber == 1 {
					report := FileReport{
						ID:              input.Signature.EventID,
						ProcessingBegin: time.Now(),
						StatusLabel:     "processing records",
						StatusBy:        cfName,
						StatusTime:      time.Now(),
					}
					publishReport(&report, cfName)
				}

				// detect blank or pretty blank lines
				if CountUniqueValues(d) <= 2 && maxColumns >= 4 {
					report := FileReport{
						ID: input.Signature.EventID,
						Counters: []ReportCounter{
							ReportCounter{
								Type:      "FileProcessor",
								Name:      "Purge",
								Count:     1,
								Increment: true,
							},
						},
					}
					publishReport(&report, cfName)

					continue
				}

				if RowLimit > 1 && r > RowLimit-1 {
					break
				}

				fields := make(map[string]string)
				for j, y := range d {
					if len(headers) > j {
						fields[headers[j]] = reNewline2.ReplaceAllString(reNewline.ReplaceAllString(y, " "), "")
					}
				}

				// // do not append attributes until after record processor, otherwise interferes with NER lookup
				// for key, value := range input.Attributes {
				// 	fields["Attr."+key] = value
				// }
				output.Fields = fields

				for k, v := range output.Fields {
					name := k
					value := strings.TrimSpace(v)
					stat := ColumnStat{Name: name}
					if val, ok := columnStats[name]; ok {
						stat = val
					}
					if len(value) > 0 {
						stat.Sparsity++
						if len(stat.Min) == 0 || strings.Compare(stat.Min, value) > 0 {
							stat.Min = value
						}
						if len(stat.Max) == 0 || strings.Compare(stat.Max, value) < 0 {
							stat.Max = value
						}
					}

					columnStats[name] = stat
				}

				// let's pub it
				outputJSON, _ := json.Marshal(output)
				psresult := topic.Publish(ctx, &pubsub.Message{
					Data: outputJSON,
				})
				_, err := psresult.Get(ctx)
				if err != nil {
					log.Fatalf("%v Could not pub record to pubsub: %v", input.Signature.EventID, err)
				} else {
					// log.Printf("%v pubbed record as message id %v: %v", input.Signature.EventID, psid, string(outputJSON))
					recordCount++
					report := FileReport{
						ID: input.Signature.EventID,
						RecordList: []RecordDetail{
							RecordDetail{
								ID:        output.Signature.RecordID,
								RowNumber: output.Signature.RowNumber,
								CreatedOn: time.Now(),
							},
						},
					}
					publishReport(&report, cfName)
				}
			}
			report2 := FileReport{
				ID:              input.Signature.EventID,
				InputStatistics: columnStats,
				StatusBy:        cfName,
				StatusTime:      time.Now(),
				StatusLabel:     "finished streaming",
				Counters: []ReportCounter{
					ReportCounter{
						Type:      "fileprocessor",
						Name:      "Outputted",
						Count:     recordCount,
						Increment: false,
					},
				},
			}
			publishReport(&report2, cfName)

			input.EventData["status"] = "Streamed"
			input.EventData["message"] = fmt.Sprintf("Record count %v", len(records))
			input.EventData["recordcount"] = len(records)
			statusJSON, _ := json.Marshal(input)
			psresult := status.Publish(ctx, &pubsub.Message{
				Data: statusJSON,
			})
			_, err = psresult.Get(ctx)
			if err != nil {
				log.Fatalf("%v Could not pub status to pubsub: %v", input.Signature.EventID, err)
			}

			SetRedisValueWithExpiration([]string{input.Signature.EventID, "records-total"}, recordCount)

		} else {
			report := FileReport{
				ID:          input.Signature.EventID,
				StatusLabel: "error: file cannot be fetched",
				StatusBy:    cfName,
				StatusTime:  time.Now(),
				Errors: []ReportError{
					ReportError{
						FileLevel: true,
						Value:     fmt.Sprintf("%v", fileURL),
						Message:   "file cannot be fetched",
					},
				},
			}
			publishReport(&report, cfName)

			input.EventData["message"] = "Unable to fetch file"
			input.EventData["status"] = "Error"
			statusJSON, _ := json.Marshal(input)
			psresult := status.Publish(ctx, &pubsub.Message{
				Data: statusJSON,
			})
			_, err = psresult.Get(ctx)
			if err != nil {
				log.Fatalf("%v Could not pub status to pubsub: %v", input.Signature.EventID, err)
			}

			log.Fatalf("Unable to fetch file %v, response code %v", fileURL, resp.StatusCode)
		}

	}
	return nil
}

func publishReport(report *FileReport, cfName string) {
	reportJSON, _ := json.Marshal(report)
	reportPub := topicR.Publish(ctx, &pubsub.Message{
		Data: reportJSON,
		Attributes: map[string]string{
			"source": cfName,
		},
	})
	_, err := reportPub.Get(ctx)
	if err != nil {
		log.Printf("ERROR Could not pub to reporting pubsub: %v", err)
	}
}

// CountSparseArray count sparse array
func CountSparseArray(inputArray []string) int {
	var counter int
	for _, c := range inputArray {
		// ignore files
		if len(c) > 0 && !strings.HasPrefix(c, "__EMPTY") {
			counter++
		}
	}
	return counter
}

func CountHeaderlikeCells(inputArray []string) int {
	var counter int
	var startsWithANumber int
	for _, key := range inputArray {
		if (strings.Contains(key, "first") && strings.Contains(key, "name")) || (strings.Contains(key, "nick") && strings.Contains(key, "name")) || strings.Contains(key, "fname") {
			counter++
		} else if (strings.Contains(key, "last") && strings.Contains(key, "name")) || strings.Contains(key, "lname") {
			counter++
		} else if strings.Contains(key, "name") {
			counter++
		} else if strings.Contains(key, "email") || strings.Contains(key, "e-mail") {
			counter++
		} else if (strings.Contains(key, "address") || strings.Contains(key, "addr") || strings.Contains(key, "addrss") || strings.Contains(key, "street 1")) && (!strings.Contains(key, "room") && !strings.Contains(key, "hall")) {
			counter++
		} else if strings.Contains(key, "street 2") || strings.Contains(key, "streetcd2") || strings.Contains(key, "address 2") || strings.Contains(key, "address2") {
			counter++
		} else if strings.Contains(key, "city") { // not sure about this one, think "Twin City"
			counter++
		} else if strings.Contains(key, "state") || key == "st" {
			counter++
		} else if strings.Contains(key, "zip") || strings.Contains(key, "postalcode") || strings.Contains(key, "postal code") {
			counter++
		}

		if reStartsWithNumber.MatchString(key) && !reStartsWithOrdinalNumber.MatchString(key) {
			startsWithANumber++
		}
	}
	if startsWithANumber > 0 {
		return 0
	}
	return counter
}

func CountUniqueValues(inputArray []string) int {
	unique := make(map[string]bool)
	for _, entry := range inputArray {
		if _, value := unique[entry]; !value {
			unique[entry] = true
		}
	}
	return len(unique)
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

func GetNERresponse(nerData NERrequest) NERresponse {
	jsonValue, err := json.Marshal(nerData)
	log.Printf("calling NER endpoint with %v", nerData)
	if err != nil {
		log.Panicf("Could not convert the NERrequest to json: %v", err)
	}
	var structResponse NERresponse
	response, err := http.Post(NERApi, "application/json", bytes.NewBuffer(jsonValue))
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

func GetNERentry(structResponse NERresponse) NERentry {
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

func GetColumnars(headers []string, data [][]string) map[string][]string {
	dataColumns := make(map[string][]string)
	//log.Printf("getcsvmap header %v data %v", headers, data)
	for j, col := range headers {
		for index := 0; index < len(data); index++ {
			if len(data[index]) > j {
				// skip empty values
				if len(data[index][j]) > 0 {
					dataColumns[col] = append(dataColumns[col], data[index][j])
				}
			} else {
				//dataColumns[col] = append(dataColumns[col], "")
			}
			// only calculates for up to 100 records
			if len(dataColumns[col]) > 100 {
				break
			}
		}
	}
	return dataColumns
}

func GetLowerCaseSorted(m []string) []string {
	var result []string
	for _, k := range m {
		result = append(result, strings.ToLower(k))
	}
	sort.Strings(result)
	return result
}

func GetNERKey(sig Signature, columns []string) string {
	// concatenate all columnn headers together, in lower case
	keys := strings.Join(GetLowerCaseSorted(columns[:]), "-")
	hasher := sha1.New()
	io.WriteString(hasher, keys)
	return fmt.Sprintf("ner:%v:%v:%v:%x", sig.OwnerID, strings.ToLower(sig.Source), strings.ToLower(sig.EventType), hasher.Sum(nil))
}

func PersistNER(key string, ner NERresponse) {
	var cache NERCache
	cache.Columns = ner.Columns
	cache.TimeStamp = time.Now()
	cache.Recompute = false
	cache.Source = "FILE"

	cacheJSON, _ := json.Marshal(cache)

	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("SET", key, string(cacheJSON))
	if err != nil {
		log.Fatalf("error storing NER %v", err)
	}
}

func SetRedisValueWithExpiration(keyparts []string, value int) {
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("SETEX", strings.Join(keyparts, ":"), redisTransientExpiration, value)
	if err != nil {
		log.Printf("Error setting redis value %v to %v, error %v", strings.Join(keyparts, ":"), value, err)
	}
}

func IncrRedisValue(keyparts []string) { // no need to update expiration
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("INCR", strings.Join(keyparts, ":"))
	if err != nil {
		log.Printf("Error incrementing redis value %v, error %v", strings.Join(keyparts, ":"), err)
	}
}

func SetRedisKeyWithExpiration(keyparts []string) {
	SetRedisValueWithExpiration(keyparts, 1)
}

func GetRedisIntValue(keyparts []string) int {
	ms := msp.Get()
	defer ms.Close()
	value, err := redis.Int(ms.Do("GET", strings.Join(keyparts, ":")))
	if err != nil {
		log.Printf("Error getting redis value %v, error %v", strings.Join(keyparts, ":"), err)
	}
	return value
}

func GetRedisIntValues(keys [][]string) []int {
	ms := msp.Get()
	defer ms.Close()

	formattedKeys := []string{}
	for _, key := range keys {
		formattedKeys = append(formattedKeys, strings.Join(key, ":"))
	}

	values, err := redis.Ints(ms.Do("MGET", formattedKeys))
	if err != nil {
		log.Printf("Error getting redis values %v, error %v", formattedKeys, err)
	}
	return values
}

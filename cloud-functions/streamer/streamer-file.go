// Package streamer contains a series of cloud functions for streamer
package streamer

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
)

// GCSEvent contains GS event
type GCSEvent struct {
	Bucket         string    `json:"bucket"`
	Name           string    `json:"name"`
	Metageneration string    `json:"metageneration"`
	ResourceState  string    `json:"resourceState"`
	TimeCreated    time.Time `json:"timeCreated"`
	Updated        time.Time `json:"updated"`
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

// FileStreamer is the main function
func FileStreamer(ctx context.Context, e GCSEvent) error {
	log.Printf("GS trigger filed on file named %v created on %v\n", e.Name, e.TimeCreated)

	txtSuffix := ".txt"

	pubsubTopic := "streamer-output"

	if strings.HasSuffix(e.Name, txtSuffix) {
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

		// open a csv reader
		fileReader := bytes.NewReader(slurp)

		csvReader := csv.NewReader(fileReader)
		csvReader.Comma = '\t'
		csvReader.FieldsPerRecord = -1
		csvHeader, err := csvReader.Read()
		if err != nil {
			log.Fatalf("unable to read header: %v", err)
			return nil
		}
		csvData, err := csvReader.ReadAll()
		if err != nil {
			log.Fatalf("unable to read file content: %v", err)
			return nil
		}

		csvHeader = RenameDuplicateColumns(csvHeader)

		// jsonHeader, err := json.Marshal(csvHeader)
		// jsonData, err := json.Marshal(csvData)
		// log.Printf("read csv header %v", string(jsonHeader))
		// log.Printf("read csv content %v", string(jsonData))

		psclient, err := pubsub.NewClient(ctx, ProjectID)
		if err != nil {
			log.Fatalf("Could not create pubsub Client: %v", err)
			return nil
		}
		pstopic := psclient.Topic(pubsubTopic)

		// TODO: redo this portion using json.Marshal
		for row, d := range csvData {
			var buffer bytes.Buffer
			buffer.WriteString("{")

			fileName := strings.TrimSuffix(e.Name, filepath.Ext(e.Name))
			fileDetail := strings.Split(fileName, "/")
			ownerID, requestID := fileDetail[0], fileDetail[1]

			// append owner id
			buffer.WriteString(`"_wm_owner":` + ownerID + `,`)

			// append request id
			buffer.WriteString(`"_wm_request":"` + requestID + `",`)
			buffer.WriteString(`"_wm_row":"` + strconv.Itoa(row+1) + `",`)
			buffer.WriteString(`"_wm_record":"` + uuid.New().String() + `",`)
			buffer.WriteString(`"_wm_timestamp": "` + time.Now().Format(time.RFC3339) + `",`)

			for j, y := range d {
				buffer.WriteString(`"` + csvHeader[j] + `":`)
				buffer.WriteString((`"` + y + `"`))
				//end of property
				if j < len(d)-1 {
					buffer.WriteString(",")
				}
			}
			buffer.WriteString("}")

			msg := buffer.String()
			// push into pubsub
			psresult := pstopic.Publish(ctx, &pubsub.Message{
				Data: []byte(msg),
			})

			// psid, err := psresult.Get(ctx)
			psid, err := psresult.Get(ctx)
			if err != nil {
				log.Fatalf("Could not pub to pubsub: %v", err)
				return nil
			}
			fmt.Printf("Published msg %v; ID: %v\n", msg, psid)
		}

		sbclient.Close()
	}
	return nil
}

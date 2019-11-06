// Package zendesk contains an HTTP Cloud Function.
package zendesk

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type Test struct {
	FistName   string
	LastName   string
	Address1   string
	Address2   string
	Email      string
	Phone      string
	CreateddAt time.Time
}

const NameSpace = "testing"
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var ES *elasticsearch.Client

func RandASCIIBytes(n int) []byte {
	output := make([]byte, n)
	// We will take n bytes, one byte for each character of output.
	randomness := make([]byte, n)
	// read all random
	_, err := rand.Read(randomness)
	if err != nil {
		panic(err)
	}
	l := len(letterBytes)
	// fill output
	for pos := range output {
		// get random item
		random := uint8(randomness[pos])
		// random % 64
		randomPos := random % uint8(l)
		// put into output
		output[pos] = letterBytes[randomPos]
	}
	return output
}

func TestES(w http.ResponseWriter, r *http.Request) {
	// generate 100K records
	tests := []*Test{}
	log.Printf("generating test records")
	count := 1000
	for i := 1; i < count; i++ {
		test := new(Test)
		test.FistName = string(RandASCIIBytes(20))
		test.LastName = string(RandASCIIBytes(20))
		test.Address1 = string(RandASCIIBytes(30))
		test.Address2 = string(RandASCIIBytes(30))
		test.Email = string(RandASCIIBytes(40))
		test.Phone = string(RandASCIIBytes(40))
		test.CreateddAt = time.Now()
		tests = append(tests, test)
	}
	log.Printf("generated test records")

	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://104.198.136.122:9200",
		},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	log.Printf("start es insert")
	for index, test := range tests {
		json, _ := json.Marshal(test)
		req := esapi.IndexRequest{
			Index:      "test",
			DocumentID: strconv.Itoa(index + 1),
			Body:       bytes.NewReader(json),
			Refresh:    "true",
		}
		req.Do(context.Background(), es)
	}
	log.Printf("finish es insert")

	log.Printf("start es update")
	// next run update
	for index, test := range tests {
		if index%10 == 0 {
			json, _ := json.Marshal(test)
			req := esapi.IndexRequest{
				Index:      "test",
				DocumentID: strconv.Itoa(index + 1),
				Body:       bytes.NewReader(json),
				Refresh:    "true",
			}
			req.Do(context.Background(), es)
		}
	}
	log.Printf("finish es update")

	// last delete all
	log.Printf("start es delete")
	for index, _ := range tests {
		req := esapi.DeleteRequest{
			Index:      "test",
			DocumentID: strconv.Itoa(index + 1),
			Refresh:    "true",
		}
		req.Do(context.Background(), es)
	}
	log.Printf("finish es delete")
}

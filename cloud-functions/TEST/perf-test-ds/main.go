// Package zendesk contains an HTTP Cloud Function.
package zendesk

import (
	"context"
	"crypto/rand"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/datastore"
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

func TestDS(w http.ResponseWriter, r *http.Request) {
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
	ctx := context.Background()
	dsClient, _ := datastore.NewClient(ctx, "wemade-core")

	log.Printf("start ds insert")
	for index, test := range tests {
		recordKey := datastore.IDKey("Record", int64(index), nil)
		dsClient.Put(ctx, recordKey, test)
	}
	log.Printf("finish ds insert")

	log.Printf("start ds update")
	// next run update
	for index, test := range tests {
		if index%10 == 0 {
			recordKey := datastore.IDKey("Record", int64(index), nil)
			dsClient.Put(ctx, recordKey, test)
		}
	}
	log.Printf("finish ds update")

	// last delete all
	log.Printf("start ds delete")
	for index, _ := range tests {
		recordKey := datastore.IDKey("Record", int64(index), nil)
		dsClient.Delete(ctx, recordKey)
	}
	log.Printf("finish ds delete")
}

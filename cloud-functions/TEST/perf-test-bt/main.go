// Package zendesk contains an HTTP Cloud Function.
package zendesk

import (
	"context"
	"crypto/rand"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/bigtable"
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

func TestBT(w http.ResponseWriter, r *http.Request) {
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
	project := "wemade-core"
	instance := "perf-test"
	client, _ := bigtable.NewClient(ctx, project, instance)
	tbl := client.Open("test")

	log.Printf("start bt insert")
	for index, test := range tests {
		mutation := bigtable.NewMutation()
		mutation.Set("RAW", "FistName", bigtable.Now(), []byte(test.FistName))
		mutation.Set("RAW", "LastName", bigtable.Now(), []byte(test.LastName))
		mutation.Set("RAW", "Address1", bigtable.Now(), []byte(test.Address1))
		mutation.Set("RAW", "Address2", bigtable.Now(), []byte(test.Address2))
		mutation.Set("RAW", "Email", bigtable.Now(), []byte(test.Email))
		mutation.Set("RAW", "Phone", bigtable.Now(), []byte(test.Phone))
		tbl.Apply(ctx, string(index), mutation)
	}
	log.Printf("finish bt insert")

	log.Printf("start bt update")
	// next run update
	for index, test := range tests {
		if index%10 == 0 {
			mutation := bigtable.NewMutation()
			mutation.Set("RAW", "FistName", bigtable.Now(), []byte(test.FistName))
			tbl.Apply(ctx, string(index), mutation)
		}
	}
	log.Printf("finish bt update")

	// last delete all
	log.Printf("start bt delete")
	for index, _ := range tests {
		mutation := bigtable.NewMutation()
		mutation.DeleteRow()
		tbl.Apply(ctx, string(index), mutation)
	}
	log.Printf("finish bt delete")
}

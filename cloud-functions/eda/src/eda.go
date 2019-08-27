// Package eda contains a Pub/Sub Cloud Function and a big query client that creates a table when its needed.
package eda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"

	"cloud.google.com/go/bigquery"
)

// ProjectID is the GCP Project ID
var ProjectID = os.Getenv("PROJECTID")

// Dataset is the BigQuery dataset, per client
var Dataset = os.Getenv("DATASET")

// PeopleTable is the BigQuery dataset, per client
var PeopleTable = os.Getenv("PEOPLETABLE")

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

//People360 is a the representation of the people 360 input
type People360 struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	ID     string  `json:"_id"`
	Score  float64 `json:"_score"`
	Source struct {
		Owner   int64  `json:"Owner"`
		Source  string `json:"Source"`
		Request string `json:"Request"`
		Row     int    `json:"Row"`
		Columns []struct {
			Name  string `json:"Name"`
			Value string `json:"Value"`
			ERR   struct {
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
			} `json:"ERR"`
			NER map[string]float32 `json:"NER"`
		} `json:"Columns"`
		TimeStamp string `json:"TimeStamp"`
	} `json:"_source"`
}

type Record struct {
	Address1        string
	Address2        string
	Age             string
	Birthday        string
	City            string
	Country         string
	County          string
	Email           string
	FirstName       string
	FullName        string
	Gender          string
	LastName        string
	MiddleName      string
	ParentEmail     string
	ParentFirstName string
	ParentLastName  string
	ParentName      string
	Phone           string
	State           string
	Suffix          string
	ZipCode         string
	TrustedID       string
	Title           string
}

func setField(v interface{}, name string, value string) error {
	// v must be a pointer to a struct
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Struct {
		return errors.New("v must be pointer to struct")
	}

	// Dereference pointer
	rv = rv.Elem()

	// Lookup field by name
	fv := rv.FieldByName(name)
	if !fv.IsValid() {

		return fmt.Errorf("not a field name: %s", name)
	}

	// Field must be exported
	if !fv.CanSet() {
		return fmt.Errorf("cannot set field %s", name)
	}

	// We expect a string field
	if fv.Kind() != reflect.String {
		return fmt.Errorf("%s is not a string field", name)
	}

	// Set the value
	fv.SetString(value)
	return nil
}

//GetRecord gets an People360 structs and extracts the golden record
func GetRecord(input People360) Record {
	var output Record
	for _, column := range input.Source.Columns {
		v := reflect.ValueOf(column.ERR)
		typeOfS := v.Type()
		for i := 0; i < v.NumField(); i++ {
			key := typeOfS.Field(i).Name
			value := v.Field(i).Int()
			if value == 1 {
				setField(&output, key, column.Value)
			}
		}
	}
	return output
}

// Main is the EDA entrypoint of the function, it flattens the json data from pubsub
// and inserts the data into bigquery
func Main(ctx context.Context, m PubSubMessage) error {
	//logs pubsub message
	//m.Data

	//Flatten json
	var jsonMessage People360
	json.Unmarshal(m.Data, &jsonMessage)
	pubSubRecord := GetRecord(jsonMessage)
	//When we get the Record we should compare it with the same one on the database
	log.Print(pubSubRecord)
	client, err := bigquery.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatalf("Error connecting to big query: %v", err)
	}
	//Get dataset
	dataset := client.Dataset(Dataset)

	//Get table if it exists,create table if it doesn't
	table := dataset.Table(PeopleTable)
	//Insert flat data into BQ
	log.Print("Inserting into database")
	inserter := table.Inserter()
	if err := inserter.Put(ctx, pubSubRecord); err != nil {
		return err
	}
	// schema, err := bigquery.InferSchema(Record{})
	// if err != nil {
	// 	return err
	// }
	return nil
}

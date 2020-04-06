package listrakpost

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
)

var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")
var WMNamespace = os.Getenv("DATASTORENS")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"
var ListrakAuthEndpoint = os.Getenv("LISTRAKAUTHENDPOINT")
var ListrakEndpoint = os.Getenv("LISTRAKENDPOINT")

var ps *pubsub.Client
var topic *pubsub.Topic
var ds *datastore.Client
var fs *datastore.Client

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
}

func ListrakPost(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Printf("Unable to unmarshal message %v with error %v", string(m.Data), err)
		return nil
	}

	log.Printf("Log PubSubMessage %v", string(m.Data))

	//Authentication  map[string]string{"mostafa": "dahab"}
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", "g1mukhpg8gkbgrrb1vmz")
	data.Set("client_secret", "xriMvCqzXzewkIgUYuHXL33V08PbTAyUbS/a+NaF/jY")

	req, err := http.NewRequest("POST", ListrakAuthEndpoint, strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[ERROR] Listrak authentication: %v ", err)
		return nil
	}
	defer resp.Body.Close()

	var authResponse AuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&authResponse); err != nil {
		log.Printf("[ERROR]There was an issue decoding the message %v", resp.Body)
		return nil
	}

	for _, c := range input.Contacts {
		output := Output{
			EmailAddress:      c.Email,
			SubscriptionState: "Subscribed",
			ExternalContactID: "",
			SegmentationFieldValues: []SegmentationFieldValue{
				SegmentationFieldValue{
					"segmentationFieldId": "11755", //Filename
					"value":               c.FirstName,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11756", //Lastname
					"value":               c.Lastname,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11762", //Address1
					"value":               c.Address1,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11778", //Address2
					"value":               c.Address2,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11763", //City
					"value":               c.City,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11764", //State
					"value":               c.State,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11765", //Zip
					"value":               c.Zip,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11766", //Country
					"value":               c.Country,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11767", //ContactID
					"value":               c.ContactID,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11779", //RoleType
					"value":               c.RoleType,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11780", //Email
					"value":               c.Email,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11775", //SchoolCode
					"value":               c.SchoolCode,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11776", //SchoolColor
					"value":               c.SchoolColor,
				},
				SegmentationFieldValue{
					"segmentationFieldId": "11777", //SchoolName
					"value":               c.ShoolName,
				},
			},
		}
		jsonValue, _ := json.Marshal(output)

		req, err = http.NewRequest("POST", ListrakEndpoint, bytes.NewBuffer(jsonValue))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Add("Authorization", "Bearer "+authResponse.AccessToken)
		client = &http.Client{}
		resp, err = client.Do(req)
		if err != nil {
			log.Printf("[ERROR] Listrak contact list: %v ", err)
			return nil
		}
		defer resp.Body.Close()
		log.Printf("Listrak contact list OK")
	}
	return nil
}

package listrakpost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	LogDev(fmt.Sprintf("PubSubMessage %v", string(m.Data)))

	//Authentication  map[string]string{"mostafa": "dahab"}
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", "nfpszemtoo82g91a7ius")
	data.Set("client_secret", "YZgjhGpSZwskKeIRGQe/j38bpce0Y2pzZJ/ZOu/JEAQ")

	req, err := http.NewRequest("POST", ListrakAuthEndpoint, strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[ERROR] Listrak authentication: %v ", err)
		return nil
	}
	defer resp.Body.Close()
	LogDev(fmt.Sprintf("Authentication: %v", resp.Status))

	if resp.StatusCode == http.StatusOK {
		decoder := json.NewDecoder(resp.Body)
		var authResponse AuthResponse
		err = decoder.Decode(&authResponse)
		if err != nil {
			log.Printf("[ERROR] There was a problem decoding the output response %v", err)
			return nil
		}
		LogDev(fmt.Sprintf("Contacts count: %v", len(input.Contacts)))

		for _, c := range input.Contacts {
			output := Output{
				EmailAddress:      c.Email,
				SubscriptionState: "Subscribed",
				ExternalContactID: "",
				SegmentationFieldValues: []SegmentationFieldValue{
					SegmentationFieldValue{
						SegmentationFieldId: "12092", //Firstname
						Value:               c.FirstName,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12093", //Lastname
						Value:               c.LastName,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12087", //Address1
						Value:               c.Address1,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12088", //Address2
						Value:               c.Address2,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12089", //City
						Value:               c.City,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12095", //State
						Value:               c.State,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12096", //Zip
						Value:               c.Zip,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12091", //Country
						Value:               c.Country,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12090", //ContactID
						Value:               c.ContactID,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12094", //RoleType
						Value:               c.RoleType,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12084", //SchoolCode
						Value:               c.SchoolCode,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12085", //SchoolColor
						Value:               c.SchoolColor,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "12086", //SchoolName
						Value:               c.SchoolName,
					},
				},
			}
			jsonValue, _ := json.Marshal(output)
			req2, err2 := http.NewRequest("POST", ListrakEndpoint, bytes.NewBuffer(jsonValue))
			req2.Header.Set("Content-Type", "application/json")
			req2.Header.Add("Authorization", "Bearer "+authResponse.AccessToken)
			client2 := &http.Client{}
			resp2, err2 := client2.Do(req2)
			if err2 != nil {
				log.Printf("[ERROR] Listrak contact list: %v ", err2)
				return nil
			}
			defer resp2.Body.Close()
			LogDev(fmt.Sprintf("Contact add: %v", resp2.Status))
		}
	}
	return nil
}

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
}

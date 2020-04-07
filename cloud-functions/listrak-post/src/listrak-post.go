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
	log.Printf("Authentication: %v", resp.Status)
	if resp.StatusCode == http.StatusOK {
		decoder := json.NewDecoder(resp.Body)
		var authResponse AuthResponse
		err = decoder.Decode(&authResponse)
		if err != nil {
			log.Printf("[ERROR] There was a problem decoding the output response %v", err)
			return nil
		}
		log.Printf("Bearer: %v", authResponse.AccessToken)
		for _, c := range input.Contacts {
			output := Output{
				EmailAddress:      c.Email,
				SubscriptionState: "Subscribed",
				ExternalContactID: "",
				SegmentationFieldValues: []SegmentationFieldValue{
					SegmentationFieldValue{
						SegmentationFieldId: "11755", //Firstname
						Value:               c.FirstName,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11756", //Lastname
						Value:               c.LastName,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11762", //Address1
						Value:               c.Address1,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11778", //Address2
						Value:               c.Address2,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11763", //City
						Value:               c.City,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11764", //State
						Value:               c.State,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11765", //Zip
						Value:               c.Zip,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11766", //Country
						Value:               c.Country,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11767", //ContactID
						Value:               c.ContactID,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11779", //RoleType
						Value:               c.RoleType,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11780", //Email
						Value:               c.Email,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11775", //SchoolCode
						Value:               c.SchoolCode,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11776", //SchoolColor
						Value:               c.SchoolColor,
					},
					SegmentationFieldValue{
						SegmentationFieldId: "11777", //SchoolName
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
			log.Printf("Add contact: %v", resp2.Status)
			log.Printf("Add contact: %v", resp2.Body)
			log.Printf("Listrak contact list OK")
		}
	}
	return nil
}

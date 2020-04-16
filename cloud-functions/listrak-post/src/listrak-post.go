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

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
)

var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")
var WMNamespace = os.Getenv("DATASTORENS")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"
var ListrakAuthEndpoint = os.Getenv("LISTRAKAUTHENDPOINT")
var ListrakEndpoint = os.Getenv("LISTRAKENDPOINT")

var smClient *secretmanager.Client
var ps *pubsub.Client
var topic *pubsub.Topic
var ds *datastore.Client
var fs *datastore.Client
var secrets Secrets

var listrakSegment = map[string]int{
	"firstname":   os.Getenv("LISTRAKSEGMENT_FIRSTNAME"),
	"lastname":    os.Getenv("LISTRAKSEGMENT_LASTNAME"),
	"address1":    os.Getenv("LISTRAKSEGMENT_ADDRESS1"),
	"address2":    os.Getenv("LISTRAKSEGMENT_ADDRESS2"),
	"city":        os.Getenv("LISTRAKSEGMENT_CITY"),
	"state":       os.Getenv("LISTRAKSEGMENT_STATE"),
	"zip":         os.Getenv("LISTRAKSEGMENT_ZIP"),
	"country":     os.Getenv("LISTRAKSEGMENT_COUNTRY"),
	"contactid":   os.Getenv("LISTRAKSEGMENT_CONTACTID"),
	"roletype":    os.Getenv("LISTRAKSEGMENT_ROLETYPE"),
	"schoolcode":  os.Getenv("LISTRAKSEGMENT_SCHOOLCODE"),
	"schoolcolor": os.Getenv("LISTRAKSEGMENT_SCHOOLCOLOR"),
	"schoolname":  os.Getenv("LISTRAKSEGMENT_SCHOOLNAME"),
}

func init() {
	ctx := context.Background()

	smClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Printf("failed to setup client: %v", err)
		return nil
	}

	secretReq := &secretmanagerpb.AccessSecretVersionRequest{
		Name: os.Getenv("SECRETS_VERSION"),
	}

	secretresult, err := smClient.AccessSecretVersion(ctx, secretReq)
	if err != nil {
		log.Printf("Failed to get secret: %v", err)
		return nil
	}

	secretsData := secretresult.Payload.Data
	if err := json.Unmarshal(secretsData, &secrets); err != nil {
		log.Printf("Error decoding secrets %v", err)
		return nil
	}
	LogDev(fmt.Sprintf("Listrak Client Id %v, Secrets %v", secrets.Listtrack.ClientID, secrets.Listtrack.ClientSecret))

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
	eventID := m.Attributes["eventid"]

	//Authentication  map[string]string{"mostafa": "dahab"}
	data := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {secrets.Listtrack.ClientID},
		"client_secret": {secrets.Listtrack.ClientSecret},
	}

	req, err := http.NewRequest("POST", ListrakAuthEndpoint, strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[ERROR] EventID: [%v] Listrak authentication: [%v] ", eventID, err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		decoder := json.NewDecoder(resp.Body)
		var authResponse AuthResponse
		err = decoder.Decode(&authResponse)
		if err != nil {
			log.Printf("[ERROR] There was a problem decoding the output response %v", err)
			return nil
		}
		log.Printf("Event id : [%v] Contacts count: [%v]", eventID, len(input.Contacts))

		for _, c := range input.Contacts {
			output := Output{
				EmailAddress:      c.Email,
				SubscriptionState: "Subscribed",
				ExternalContactID: "",
				Segments: []SegmentationField{
					SegmentationField{
						ID:    listrakSegment["firstname"],
						Value: c.FirstName,
					},
					SegmentationField{
						ID:    listrakSegment["lastname"],
						Value: c.LastName,
					},
					SegmentationField{
						ID:    listrakSegment["address1"],
						Value: c.Address1,
					},
					SegmentationField{
						ID:    listrakSegment["address2"],
						Value: c.Address2,
					},
					SegmentationFieldValue{
						ID:    listrakSegment["city"],
						Value: c.City,
					},
					SegmentationField{
						ID:    listrakSegment["state"],
						Value: c.State,
					},
					SegmentationField{
						ID:    listrakSegment["zip"],
						Value: c.Zip,
					},
					SegmentationField{
						ID:    listrakSegment["country"],
						Value: c.Country,
					},
					SegmentationField{
						ID:    listrakSegment["contactid"],
						Value: c.ContactID,
					},
					SegmentationField{
						ID:    listrakSegment["roletype"],
						Value: c.RoleType,
					},
					SegmentationField{
						ID:    listrakSegment["schoolcode"],
						Value: c.SchoolCode,
					},
					SegmentationField{
						ID:    listrakSegment["schoolcolor"],
						Value: c.SchoolColor,
					},
					SegmentationField{
						ID:    listrakSegment["schoolname"],
						Value: c.SchoolName,
					},
				},
			}
			jsonValue, _ := json.Marshal(output)
			flag := false
			for {
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
				if resp2.StatusCode != http.StatusOK {
					log.Printf("EventID: [%v] Contact status: [%v] value:[%v]", eventID, resp2.Status, jsonValue)
					if flag {
						break
					} else {
						flag = true
					}
				} else {
					break
				}
			}
		}
	}
	return nil
}

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
}

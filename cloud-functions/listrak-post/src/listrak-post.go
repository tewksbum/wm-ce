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
	"strconv"
	"strings"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"github.com/fatih/structs"

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
	"firstname":   getenvInt("LISTRAKSEGMENT_FIRSTNAME"),
	"lastname":    getenvInt("LISTRAKSEGMENT_LASTNAME"),
	"address1":    getenvInt("LISTRAKSEGMENT_ADDRESS1"),
	"address2":    getenvInt("LISTRAKSEGMENT_ADDRESS2"),
	"city":        getenvInt("LISTRAKSEGMENT_CITY"),
	"state":       getenvInt("LISTRAKSEGMENT_STATE"),
	"zip":         getenvInt("LISTRAKSEGMENT_ZIP"),
	"country":     getenvInt("LISTRAKSEGMENT_COUNTRY"),
	"contactid":   getenvInt("LISTRAKSEGMENT_CONTACTID"),
	"roletype":    getenvInt("LISTRAKSEGMENT_ROLETYPE"),
	"schoolcode":  getenvInt("LISTRAKSEGMENT_SCHOOLCODE"),
	"schoolcolor": getenvInt("LISTRAKSEGMENT_SCHOOLCOLOR"),
	"schoolname":  getenvInt("LISTRAKSEGMENT_SCHOOLNAME"),
}

var devSegments = map[string]int{
	"FirstName":   17358,
	"LastName":    17359,
	"PortalLink":  17360,
	"SchoolCode":  17362,
	"SchoolName":  17364,
	"SchoolColor": 17363,
}

var fepSegments = map[string]int{
	"FirstName":   6241,
	"LastName":    6242,
	"PortalLink":  6256,
	"SchoolCode":  6275,
	"SchoolName":  6276,
	"SchoolColor": 6277,
}

var carSegments = map[string]int{
	"FirstName":   17329,
	"LastName":    17330,
	"PortalLink":  17331,
	"SchoolCode":  17333,
	"SchoolName":  17335,
	"SchoolColor": 17334,
}

var cwpSegments = map[string]int{
	"FirstName":   17343,
	"LastName":    17344,
	"PortalLink":  17345,
	"SchoolCode":  17347,
	"SchoolName":  17349,
	"SchoolColor": 17348,
}

var rhlSegments = map[string]int{
	"FirstName":   17336,
	"LastName":    17337,
	"PortalLink":  17338,
	"SchoolCode":  17340,
	"SchoolName":  17342,
	"SchoolColor": 17341,
}

var ddSegments = map[string]int{
	"FirstName":   17350,
	"LastName":    17351,
	"PortalLink":  17352,
	"SchoolCode":  17354,
	"SchoolName":  17356,
	"SchoolColor": 17355,
}

// Ambassador segmentation ids
var listrakAmbassadorSegment = map[string]int{
	"schoolcode":  getenvInt("LISTRAKSEGMENT_AMBASSADOR_SCHOOLCODE"),
	"schoolname":  getenvInt("LISTRAKSEGMENT_AMBASSADOR_SCHOOLNAME"),
	"schoolcolor": getenvInt("LISTRAKSEGMENT_AMBASSADOR_SCHOOLCOLOR"),
	"firstname":   getenvInt("LISTRAKSEGMENT_AMBASSADOR_FIRSTNAME"),
	"lastname":    getenvInt("LISTRAKSEGMENT_AMBASSADOR_LASTNAME"),
	"fbid":        getenvInt("LISTRAKSEGMENT_AMBASSADOR_FBID"),
	"instagram":   getenvInt("LISTRAKSEGMENT_AMBASSADOR_INSTAGRAM"),
	"social":      getenvInt("LISTRAKSEGMENT_AMBASSADOR_SOCIAL"),
	"why":         getenvInt("LISTRAKSEGMENT_AMBASSADOR_WHY"),
}

func getenvInt(key string) int {
	s := os.Getenv(key)
	if s == "" {
		s = "0"
	}

	v, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return v
}

func init() {
	ctx := context.Background()

	smClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Printf("failed to setup client: %v", err)
		return
	}

	secretReq := &secretmanagerpb.AccessSecretVersionRequest{
		Name: os.Getenv("SECRETS_VERSION"),
	}

	secretresult, err := smClient.AccessSecretVersion(ctx, secretReq)
	if err != nil {
		log.Printf("Failed to get secret: %v", err)
		return
	}

	secretsData := secretresult.Payload.Data
	if err := json.Unmarshal(secretsData, &secrets); err != nil {
		log.Printf("Error decoding secrets %v", err)
		return
	}

	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
}

func ListrakPost(ctx context.Context, m PubSubMessage) error {
	eventID := m.Attributes["eventid"]
	form := m.Attributes["form"]
	listid := m.Attributes["listid"]
	program := m.Attributes["program"]

	var input []ContactInfo
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Printf("Unable to unmarshal message %v with error %v", string(m.Data), err)
		return nil
	}
	LogDev(fmt.Sprintf("PubSubMessage %v", string(m.Data)))

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
	successCount := 0
	failCount := 0

	if resp.StatusCode == http.StatusOK {
		ListrakEndpoint := "https://api.listrak.com/email/v1/List/" + listid + "/Contact"
		decoder := json.NewDecoder(resp.Body)
		var authResponse AuthResponse
		err = decoder.Decode(&authResponse)
		if err != nil {
			log.Printf("[ERROR] There was a problem decoding the output response %v", err)
			return nil
		}
		log.Printf("Event id : [%v] Contacts count: [%v]", eventID, len(input))
		var output Output
		for _, c := range input {
			if form == "cp" {
				fields := structs.Names(&ContactInfo{})
				accessor := structs.New(c)
				var segments []SegmentationField

				for _, n := range fields {
					if n != "Address1" && n != "Address2" && n != "City" && n != "State " && n != "Zip" && n != "Country " && n != "RoleType" && n != "Email" && n != "ContactID" && n != "FbID" && n != "Instagram" && n != "Social" && n != "Why" { // skip fields that should not be sent as segments
						field := accessor.Field(n)
						value := field.Value().(string)

						if dev {
							if devSegments[n] != 0 {
								segments = append(segments, SegmentationField{
									ID:    devSegments[n],
									Value: value,
								})
							}
						} else if program == "FEP" {
							if fepSegments[n] != 0 {
								segments = append(segments, SegmentationField{
									ID:    fepSegments[n],
									Value: value,
								})
							}
						} else if program == "CWP" {
							if cwpSegments[n] != 0 {
								segments = append(segments, SegmentationField{
									ID:    cwpSegments[n],
									Value: value,
								})
							}
						} else if program == "CAR" {
							if carSegments[n] != 0 {
								segments = append(segments, SegmentationField{
									ID:    carSegments[n],
									Value: value,
								})
							}
						} else if program == "RHL" {
							if rhlSegments[n] != 0 {
								segments = append(segments, SegmentationField{
									ID:    rhlSegments[n],
									Value: value,
								})
							}
						} else if program == "DD" {
							if ddSegments[n] != 0 {
								segments = append(segments, SegmentationField{
									ID:    ddSegments[n],
									Value: value,
								})
							}
						}
					}
				}

				output = Output{
					EmailAddress:      c.Email,
					SubscriptionState: "Subscribed",
					ExternalContactID: "",
					Segments:          segments,
				}
			} else {
				output = Output{
					EmailAddress:      c.Email,
					SubscriptionState: "Subscribed",
					ExternalContactID: "",
					Segments: []SegmentationField{
						{
							ID:    listrakAmbassadorSegment["firstname"],
							Value: c.FirstName,
						},
						{
							ID:    listrakAmbassadorSegment["lastname"],
							Value: c.LastName,
						},
						{
							ID:    listrakAmbassadorSegment["schoolname"],
							Value: c.SchoolName,
						},
						{
							ID:    listrakAmbassadorSegment["schoolcode"],
							Value: c.SchoolCode,
						},
						{
							ID:    listrakAmbassadorSegment["schoolcolor"],
							Value: c.SchoolColor,
						},
						{
							ID:    listrakAmbassadorSegment["fbid"],
							Value: c.FbID,
						},
						{
							ID:    listrakAmbassadorSegment["instagram"],
							Value: c.Instagram,
						},
						{
							ID:    listrakAmbassadorSegment["social"],
							Value: c.Social,
						},
						{
							ID:    listrakAmbassadorSegment["why"],
							Value: c.Why,
						},
					},
				}

			}
			jsonValue, _ := json.Marshal(output)
			log.Printf("Posting to %v output: %v", ListrakEndpoint, string(jsonValue))
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
				if resp2.StatusCode != http.StatusOK && resp2.StatusCode != http.StatusCreated {
					log.Printf("EventID: [%v] Contact status: [%v] value: [%v]", eventID, resp2.Status, c.Email)
					if flag {
						failCount++
						break
					} else {
						flag = true
					}
				} else {
					successCount++
					break
				}
			}
		}
	}
	log.Printf("EventID: [%v] Success: [%v] Fail: [%v]", eventID, successCount, failCount)
	return nil
}

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
}

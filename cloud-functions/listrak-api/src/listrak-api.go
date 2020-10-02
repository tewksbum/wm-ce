package listrakapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
)

var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")
var WMNamespace = os.Getenv("DATASTORENS")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"
var DSKindSet = os.Getenv("DSKINDSET")
var DSKindGolden = os.Getenv("DSKINDGOLDEN")
var DSKindFiber = os.Getenv("DSKINDFIBER")
var PubSubTopic = os.Getenv("PSOUTPUT")
var Bucket = os.Getenv("DUMPBUCKET")

// global vars
var ps *pubsub.Client
var topic *pubsub.Topic
var ds *datastore.Client
var fs *datastore.Client
var cs *storage.Client
var ctx context.Context

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
	cs, _ = storage.NewClient(ctx)
	topic = ps.Topic(PubSubTopic)
}

// ListrakProcessRequest eceives a http event request
func ListrakProcessRequest(w http.ResponseWriter, r *http.Request) {
	var inputs []FileReady

	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "3600")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Set CORS headers for the main request.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Error decoding request\"}")
		log.Fatalf("error decoding request %v", err)
		return
	}

	for _, input := range inputs {

		// look up the event
		var events []Event
		var event Event
		eventQuery := datastore.NewQuery("Event").Namespace(WMNamespace).Filter("EventID =", input.EventID).Limit(1)
		if _, err := fs.GetAll(ctx, eventQuery, &events); err != nil {
			log.Fatalf("Error querying event: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "{\"success\": false, \"message\": \"Internal error occurred, -1\"}")
			return
		} else if len(events) > 0 {
			event = events[0]
		} else {
			log.Fatalf("Event ID not found: %v", input.EventID)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "{\"success\": false, \"message\": \"Internal error occurred, -2\"}")
			return
		}

		// get the set ids
		dsNameSpace := strings.ToLower(fmt.Sprintf("%v-%v", Env, input.OwnerID))
		setQueryTest := datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).Filter("eventid =", input.EventID).KeysOnly()
		setKeysTest, _ := fs.GetAll(ctx, setQueryTest, nil)
		log.Printf("Found %v matching sets", len(setKeysTest))

		// get the golden records
		var goldenKeys []*datastore.Key
		var goldenIDs []string
		var goldens []PeopleGoldenDS
		for _, setKey := range setKeysTest {
			if !Contains(goldenIDs, setKey.Name) {
				goldenIDs = append(goldenIDs, setKey.Name)
				dsGoldenGetKey := datastore.NameKey(DSKindGolden, setKey.Name, nil)
				dsGoldenGetKey.Namespace = dsNameSpace
				goldenKeys = append(goldenKeys, dsGoldenGetKey)
				goldens = append(goldens, PeopleGoldenDS{})
			}
		}
		if len(goldenKeys) > 0 {
			batchSize := 1000
			l := len(goldenKeys) / batchSize

			if len(goldenKeys)%batchSize > 0 {
				l++
			}
			for r := 0; r < l; r++ {
				s := r * 1000
				e := s + 1000

				if e > len(goldenKeys) {
					e = len(goldenKeys)
				}

				gk := goldenKeys[s:e]
				gd := goldens[s:e]

				if err := fs.GetMulti(ctx, gk, gd); err != nil && err != datastore.ErrNoSuchEntity {
					log.Printf("Error fetching golden records ns %v kind %v, key count %v: %v,", dsNameSpace, DSKindGolden, len(goldenKeys), err)
				}

			}
		}

		log.Printf("Loaded %v matching golden", len(goldens))

		if input.Type == "2" {
			header := []string{"First Name", "Last Name", "Street Address 1", "Street Address 2", "City", "State", "Zipcode", "Country", "Role Type", "Email", "Contact ID", "School Code", "School Color", "School Name"}
			records := [][]string{header}
			for _, g := range goldens {
				if len(g.EMAIL) > 0 {
					row := []string{
						g.FNAME,
						g.LNAME,
						g.AD1,
						g.AD2,
						g.CITY,
						g.STATE,
						g.ZIP,
						g.COUNTRY,
						strings.Split(g.EMAIL, "|")[0], // only write one email to CP
						"",
						"",
						"",
						"",
						"",
						"",
						"",
					}
					records = append(records, row)
				}
			}
			if len(records) > 1 {
				copyFileToBucket(ctx, event, records, Bucket)
				log.Printf("Writing %v records into output file", len(records)-1)
			} else {
				log.Printf("The event id %v doesn't have emails", input.EventID)
			}
		} else {
			// assemble contact list
			output := []ContactInfo{}
			for _, g := range goldens {
				if len(g.EMAIL) > 0 {
					emails := strings.Split(g.EMAIL, "|")
					if len(emails) > 0 {
						for _, email := range emails {
							contactInfo := ContactInfo{
								FirstName:   g.FNAME,
								LastName:    g.LNAME,
								Address1:    g.AD1,
								Address2:    g.AD2,
								City:        g.CITY,
								State:       g.STATE,
								Zip:         g.ZIP,
								Country:     g.COUNTRY,
								RoleType:    validateRole(g.ROLE),
								Email:       email,
								ContactID:   g.ID.Name,
								SchoolCode:  GetKVPValue(event.Passthrough, "schoolCode"),
								SchoolColor: GetKVPValue(event.Passthrough, "schoolColor"),
								SchoolName:  GetKVPValue(event.Passthrough, "schoolName"),
							}
							output = append(output, contactInfo)
						}
					}
				}
			}
			log.Printf("Writing %v records into listrak-post file", len(output))

			// push into pubsub contacts
			totalContacts := len(output)
			pageSize := 250
			batchCount := totalContacts / pageSize
			if totalContacts%pageSize > 0 {
				batchCount++
			}
			for i := 0; i < batchCount; i++ {
				startIndex := i * pageSize
				endIndex := (i + 1) * pageSize
				if endIndex > totalContacts {
					endIndex = totalContacts
				}
				contacts := output[startIndex:endIndex]
				outputJSON, _ := json.Marshal(contacts)
				psresult := topic.Publish(ctx, &pubsub.Message{
					Data: outputJSON,
					Attributes: map[string]string{
						"eventid": input.EventID,
						"listid":  os.Getenv("LISTRAKCP"),
						"form":    "cp",
					},
				})
				psid, err := psresult.Get(ctx)
				_, err = psresult.Get(ctx)
				if err != nil {
					log.Printf("%v Could not pub to pubsub: %v", input.EventID, err)
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprint(w, "{\"success\": false, \"message\": \"Internal error occurred, -3\"}")
					return
				}
				log.Printf("%v pubbed record as message id %v: %v", input.EventID, psid, string(outputJSON))
			}
		}
	}
	//Output
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "{success: true, message: \"Request queued to listrak\"}")
	return
}

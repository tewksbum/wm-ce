package cpfile

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
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

var reAlphaNumeric = regexp.MustCompile("[^a-zA-Z0-9]+")

var redisTransientExpiration = 3600 * 24
var redisTemporaryExpiration = 3600

var ps *pubsub.Client
var topic *pubsub.Topic
var ds *datastore.Client
var fs *datastore.Client
var cs *storage.Client
var sb *storage.BucketHandle

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
	cs, _ = storage.NewClient(ctx)
	sb = cs.Bucket(os.Getenv("BUCKET"))
	topic = ps.Topic(PubSubTopic)

	response, _ := http.Get("https://ifconfig.me/all")
	data, _ := ioutil.ReadAll(response.Body)

	// sshConfig := &ssh.ClientConfig{
	// 	HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	// 	// optional host key algo list
	// 	HostKeyAlgorithms: []string{
	// 		ssh.KeyAlgoRSA,
	// 		ssh.KeyAlgoDSA,
	// 		ssh.KeyAlgoECDSA256,
	// 		ssh.KeyAlgoECDSA384,
	// 		ssh.KeyAlgoECDSA521,
	// 		ssh.KeyAlgoED25519,
	// 	},
	// 	// optional tcp connect timeout
	// 	Timeout: 60 * time.Second,
	// }

	// sshConnection, err := ssh.Dial("tcp", "sshmyip.com:22", sshConfig)
	// if err != nil {
	// 	log.Fatalf("Error ssh dial: %v", err)
	// }
	// sshSession, err := sshConnection.NewSession()
	// var sshBuffer bytes.Buffer
	// sshSession.Stdout = &sshBuffer

	// if err != nil {
	// 	log.Fatalf("Error ssh session: %v", err)
	// }
	// defer sshSession.Close()
	// err = sshSession.Start("")
	// if err != nil {
	// 	log.Fatalf("Run failed:%v", err)
	// }
	// log.Printf(">%s", sshBuffer.Bytes())
	// sshConnection.Close()

	log.Printf("init complete in outgoing ip %v", string(data))
}

func GenerateCP(ctx context.Context, m PubSubMessage) error {
	var input FileReady
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	// // lookup the owner
	// var customers []Customer
	// var customer Customer
	// customerQuery := datastore.NewQuery("Customer").Namespace(WMNamespace).Filter("Owner =", input.OwnerID).Limit(1)

	// if _, err := ds.GetAll(ctx, customerQuery, &customers); err != nil {
	// 	log.Fatalf("Error querying customer: %v", err)
	// 	return nil
	// } else if len(customers) > 0 {
	// 	customer = customers[0]
	// } else {
	// 	log.Fatalf("Owner ID not found: %v", input.OwnerID)
	// 	return nil
	// }

	// look up the event
	var events []Event
	var event Event
	eventQuery := datastore.NewQuery("Event").Namespace(WMNamespace).Filter("EventID =", input.EventID).Limit(1)
	if _, err := fs.GetAll(ctx, eventQuery, &events); err != nil {
		log.Fatalf("Error querying event: %v", err)
		return nil
	} else if len(events) > 0 {
		event = events[0]
	} else {
		log.Fatalf("Event ID not found: %v", input.EventID)
		return nil
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

	// assemble the csv
	header := []string{
		"School Code", "Sponsor", "Input Type", "Class Year", "Program", "Adcode", "Date Uploaded", "Order By Date", "List Type", "Salutation",
		"Student First Name", "Student Last Name", "Street Address 1", "Street Address 2", "City", "State", "Zipcode", "Country", "Student's Email_1", "Student's Email_2",
		"Parent_1's First Name", "Parent_1's Last Name", "Parent_1's Email", "Parent_2's First Name", "Parent_2's Last Name", "Parent_2's Email"}
	records := [][]string{header}
	var contacts []ContactOutput
	for _, g := range goldens {
		school := School{
			SchoolCode:  GetKVPValue(event.Passthrough, "schoolCode"),
			SchoolColor: "Purple",
			SchoolName:  GetKVPValue(event.Passthrough, "schoolName"),
		}

		contactInfo := ContactInfo{
			FirstName: g.FNAME,
			LastName:  g.LNAME,
			Address1:  g.AD1,
			Address2:  g.AD2,
			City:      g.CITY,
			State:     g.STATE,
			Zip:       g.ZIP,
			Country:   g.COUNTRY,
			RoleType:  g.ROLE,
			Email:     g.EMAIL,
			ContactID: g.EMAIL,
		}

		contact := ContactOutput{
			School:      school,
			ContactInfo: contactInfo,
		}
		contacts = append(contacts, contact)

		if g.ROLE == "Parent" {
			continue
		}

		row := []string{
			GetKVPValue(event.Passthrough, "schoolCode"),
			GetKVPValue(event.Passthrough, "schoolName"),
			GetKVPValue(event.Passthrough, "inputType"),
			GetKVPValue(event.Passthrough, "schoolYear"),
			GetKVPValue(event.Passthrough, "masterProgramCode"),
			GetKVPValue(event.Passthrough, "ADCODE"),
			event.Created.Format("01/02/2006"),
			GetKVPValue(event.Passthrough, "orderByDate"),
			GetKVPValue(event.Passthrough, "listType"),
			GetKVPValue(event.Passthrough, "salutation"),

			g.FNAME,
			g.LNAME,
			g.AD1,
			g.AD2,
			g.CITY,
			g.STATE,
			g.ZIP,
			g.COUNTRY,
			g.EMAIL,
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

	log.Printf("Writing %v records into output file", len(records))
	// store it in bucket
	var buf bytes.Buffer

	csv := csv.NewWriter(&buf)
	csv.WriteAll(records)
	csv.Flush()

	csvBytes := buf.Bytes()

	file := sb.Object(GetKVPValue(event.Passthrough, "sponsorCode") + "." + GetKVPValue(event.Passthrough, "masterProgramCode") + "." + GetKVPValue(event.Passthrough, "schoolYear") + "." + input.EventID + "." + strconv.Itoa(len(records)-1) + ".csv")
	writer := file.NewWriter(ctx)
	if _, err := io.Copy(writer, bytes.NewReader(csvBytes)); err != nil {
		log.Fatalf("File cannot be copied to bucket %v", err)
	}
	if err := writer.Close(); err != nil {
		log.Fatalf("Failed to close bucket write stream %v", err)
	}

	// push into pubsub
	outputJSON, _ := json.Marshal(contacts)
	psresult := topic.Publish(ctx, &pubsub.Message{
		Data: outputJSON,
		Attributes: map[string]string{
			"filename": "test",
		},
	})
	psid, err := psresult.Get(ctx)
	_, err = psresult.Get(ctx)
	if err != nil {
		log.Printf("%v Could not pub to pubsub: %v", input.EventID, err)
		return nil
	} else {
		log.Printf("%v pubbed record as message id %v: %v", input.EventID, psid, string(outputJSON))
	}

	return nil
}
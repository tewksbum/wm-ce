package cpfile

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
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
var Bucket = os.Getenv("BUCKET")
var FEPBucket = os.Getenv("FEPBUCKET")
var BadBucket = os.Getenv("BADBUCKET")
var Threshold = getEnvFloat("THRESHOLD")
var PortalLink = os.Getenv("PORTALLINK")
var FEPOptions = strings.Split(os.Getenv("FEPOPTIONS"), ",")
var CAROptions = strings.Split(os.Getenv("CAROPTIONS"), ",")
var CWPOptions = strings.Split(os.Getenv("CWPOPTIONS"), ",")
var RHLOptions = strings.Split(os.Getenv("RHLOPTIONS"), ",")
var DDOptions = strings.Split(os.Getenv("DDOPTIONS"), ",")

var reAlphaNumeric = regexp.MustCompile("[^a-zA-Z0-9]+")

var redisTransientExpiration = 3600 * 24
var redisTemporaryExpiration = 3600

var ps *pubsub.Client
var topic *pubsub.Topic
var ds *datastore.Client
var fs *datastore.Client
var cs *storage.Client
var status *pubsub.Topic

var adcodeList = map[string]string{
	"UAZ": "UAZ1R0LQQW",
	"UNL": "UNL1R0LQQW",
	"MSO": "MSO1R0LQQW",
	"ITH": "ITH1R0LQQW",
	"TCU": "TCU1R0LQQW",
	"PSU": "PSU7R0LQQW",
	"COB": "COB1R0LQQW",
	"UFL": "UFL1R0LQQW",
	"TAM": "TAM1R0LQQW",
	"MAA": "MAA1R0LQQW",
	"NCT": "NCT1R0LQQW",
	"NDS": "NDS1R0LQQW",
	"CMI": "CMI1R0LQQW",
	"CSL": "CSL1R0LQQW",
	"UNC": "UNC1R0LQQW",
	"RUT": "RUT1R0LQQW",
	"YLU": "YLU1R0LQQW",
	"FGC": "FGC1R0LQQW",
	"UTO": "UTO1R0LQQW",
	"RDR": "RDR1R0LQQW",
	"CHC": "CHC1R0LQQW",
	"HMP": "HMP7R0LQQW",
	"CAU": "CAU7R0LQQW",
	"SMI": "SMI1R0LQQW",
	"MIT": "MIT1R0LQQW",
	"USE": "USE1R0LQQW",
	"TSM": "TSM1R0LQQW",
	"UNT": "UNT1R0LQQW",
	"FHS": "FHS7R0LQQW",
	"HNU": "HNU1R0LQQW",
	"DRU": "DRU7R0LQQW",
	"SCT": "SCT1R0LQQW",
	"MDL": "MDL7R0LQQW",
	"WDN": "WDN1R0LQQW",
	"CMU": "CMU7R0LQQW",
	"SFL": "SFL7R0LQQW",
	"KNS": "KNS1R0LQQW",
}

var seasonCodeList = []string{"Spring", "Summer", "Fall", "Winter"}

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
	cs, _ = storage.NewClient(ctx)
	topic = ps.Topic(PubSubTopic)
	status = ps.Topic(os.Getenv("PSSTATUS"))

	response, _ := http.Get("https://wtfismyip.com/text")
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
		log.Printf("Event ID not found: %v", input.EventID)
		return nil
	}
	listOutput := GetKVPValue(event.Passthrough, "listOutput")
	if strings.ToLower(listOutput) != "skip" {

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

		output := []ContactInfo{}
		if event.EventType != "Form Submission" {
			countStudentEmails := 0
			countParentEmails := 0

			// assemble the csv
			header := []string{
				"School Code", "CRM", "Processor", "Sponsor", "Input Type", "Class Year", "Program", "Adcode", "Date Uploaded", "Order By Date", "List Type", "Salutation",
				"Student First Name", "Student Last Name", "Street Address 1", "Street Address 2", "City", "State", "Zipcode", "Country", "Student's Email_1", "Student's Email_2",
				"Parent_1's First Name", "Parent_1's Last Name", "Parent_1's Email", "Parent_2's First Name", "Parent_2's Last Name", "Parent_2's Email"}
			records := [][]string{header}
			header = append(header, "ADVALID")
			badrecords := [][]string{header}
			badAD1 := 0
			goodAD := 0
			studentsUS := 0
			international := 0
			for _, g := range goldens {
				if len(g.EMAIL) > 0 {
					emails := strings.Split(g.EMAIL, "|")
					if len(emails) > 0 {
						for _, email := range emails {
							if validateRole(g.ROLE) == "Student" {
								countStudentEmails++
							} else {
								countParentEmails++
							}
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
								PortalLink:  PortalLink + GetKVPValue(event.Passthrough, "sponsorCode"),
							}
							output = append(output, contactInfo)
						}
					}
				}
				//only students
				if g.ROLE == "Parent" {
					continue
				}
				if g.COUNTRY == "US" {
					studentsUS++
				} else {
					international++
				}
				masterProgramCode := GetKVPValue(event.Passthrough, "masterProgramCode")
				adcode := GetKVPValue(event.Passthrough, "ADCODE")
				seasonUpload := GetKVPValue(event.Passthrough, "seasonUpload")
				if Contains(RHLOptions, GetKVPValue(event.Passthrough, "masterProgramCode")) && Contains(seasonCodeList, seasonUpload) {
					masterProgramCode = masterProgramCode + strings.ToUpper(seasonUpload)
					adcode = adcodeList[GetKVPValue(event.Passthrough, "schoolCode")]
					if len(adcode) == 0 {
						eventData := EventData{
							Signature: Signature{
								EventID: input.EventID,
								OwnerID: input.OwnerID,
							},
							EventData: make(map[string]interface{}),
						}
						eventData.EventData["status"] = "Error"
						eventData.EventData["message"] = "No campaign selected for this school"
						statusJSON, _ := json.Marshal(eventData)
						_ = status.Publish(ctx, &pubsub.Message{
							Data: statusJSON,
						})
						return nil
					}
				}

				row := []string{
					GetKVPValue(event.Passthrough, "schoolCode"),
					GetKVPValue(event.Attributes, "classStanding"), //This is only for frames.
					"",
					GetKVPValue(event.Passthrough, "schoolName"),
					GetKVPValue(event.Passthrough, "inputType"),
					schoolYearFormatter(GetKVPValue(event.Passthrough, "schoolYear"), GetKVPValue(event.Attributes, "classStanding")),
					masterProgramCode,
					adcode,
					event.Created.Format("01/02/2006"),
					GetKVPValue(event.Passthrough, "orderByDate"),
					roleFormatter(GetKVPValue(event.Attributes, "role")),
					GetKVPValue(event.Passthrough, "salutation"),
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

				//only students with name and lastname
				if !Contains(FEPOptions, GetKVPValue(event.Passthrough, "masterProgramCode")) {
					if (g.ADVALID == "TRUE" || g.COUNTRY != "US") && (len(g.FNAME) > 0 && len(g.LNAME) > 0) {
						goodAD++
						records = append(records, row)
					} else {
						badAD1++
						row = append(row, "FALSE")
						badrecords = append(badrecords, row)
					}
				} else {
					records = append(records, row)
				}
			}

			if Contains(FEPOptions, GetKVPValue(event.Passthrough, "masterProgramCode")) {
				filename := copyFileToBucket(ctx, event, records, FEPBucket)
				log.Printf("Writing %v records into output file %v", len(records)-1, filename)

				eventData := EventData{
					Signature: Signature{
						EventID: input.EventID,
						OwnerID: input.OwnerID,
					},
					EventData: make(map[string]interface{}),
				}

				eventData.EventData["status"] = "PROCESSED"
				eventData.EventData["message"] = "Emails subscribed to listrak and file generated successfully " + filename
				eventData.EventData["parent-emails"] = countParentEmails
				eventData.EventData["student-emails"] = countStudentEmails
				eventData.EventData["row-count"] = len(records) - 1

				statusJSON, _ := json.Marshal(eventData)
				_ = status.Publish(ctx, &pubsub.Message{
					Data: statusJSON,
				})

			} else if goodAD >= int(float64((studentsUS))*Threshold) {
				// good to go
				filename := copyFileToBucket(ctx, event, records, Bucket)
				log.Printf("Writing %v records into output file", len(records)-1)
				if len(badrecords) > 1 {
					// store it bad bucket
					copyFileToBucket(ctx, event, badrecords, BadBucket)
					log.Printf("Writing %v records into bad bucket output file", len(badrecords)-1)
				}
				eventData := EventData{
					Signature: Signature{
						EventID: input.EventID,
						OwnerID: input.OwnerID,
					},
					EventData: make(map[string]interface{}),
				}

				eventData.EventData["status"] = "File Generated"
				eventData.EventData["message"] = "CP file generated successfully " + filename
				eventData.EventData["parent-emails"] = countParentEmails
				eventData.EventData["student-emails"] = countStudentEmails
				eventData.EventData["advalid-count"] = goodAD
				eventData.EventData["bad-addresses"] = badAD1
				eventData.EventData["row-count"] = len(records) - 1

				statusJSON, _ := json.Marshal(eventData)
				_ = status.Publish(ctx, &pubsub.Message{
					Data: statusJSON,
				})

			} else { // more than 20% of bad record
				eventData := EventData{
					Signature: Signature{
						EventID: input.EventID,
						OwnerID: input.OwnerID,
					},
					EventData: make(map[string]interface{}),
				}
				eventData.EventData["status"] = "Error"
				eventData.EventData["message"] = "AdValid threshold exceeded, source file needs to be reviewed"
				eventData.EventData["parent-emails"] = countParentEmails
				eventData.EventData["student-emails"] = countStudentEmails
				statusJSON, _ := json.Marshal(eventData)
				_ = status.Publish(ctx, &pubsub.Message{
					Data: statusJSON,
				})

				for r, record := range records {
					if r != 0 {
						record = append(record, "TRUE")
						badrecords = append(badrecords, record)
					}
				}
				copyFileToBucket(ctx, event, badrecords, BadBucket)
				log.Printf("ERROR ADVALID threshold reached, output in bad bucket")
			}

			// push into pubsub contacts
			listrakid := ""
			program := ""
			if Contains(FEPOptions, GetKVPValue(event.Passthrough, "masterProgramCode")) {
				listrakid = os.Getenv("FEPLISTRAK")
				program = "FEP"
			} else if Contains(CAROptions, GetKVPValue(event.Passthrough, "masterProgramCode")) {
				listrakid = os.Getenv("CARLISTRAK")
				program = "CAR"
			} else if Contains(CWPOptions, GetKVPValue(event.Passthrough, "masterProgramCode")) {
				listrakid = os.Getenv("CWPLISTRAK")
				program = "CWP"
			} else if Contains(RHLOptions, GetKVPValue(event.Passthrough, "masterProgramCode")) {
				listrakid = os.Getenv("RHLLISTRAK")
				program = "RHL"
			} else if Contains(DDOptions, GetKVPValue(event.Passthrough, "masterProgramCode")) {
				listrakid = os.Getenv("DDLISTRAK")
				program = "DD"
			}

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
						"listid":  listrakid,
						"form":    "cp",
						"program": program,
					},
				})
				psid, err := psresult.Get(ctx)
				_, err = psresult.Get(ctx)
				if err != nil {
					log.Printf("%v Could not pub to pubsub: %v", input.EventID, err)
					return nil
				}
				log.Printf("%v pubbed record as message id %v: %v", input.EventID, psid, string(outputJSON))
			}

		} else {

			for _, g := range goldens {
				contactInfo := ContactInfo{
					Email:       g.EMAIL,
					FirstName:   g.FNAME,
					LastName:    g.LNAME,
					SchoolCode:  GetKVPValue(event.Passthrough, "schoolCode"),
					SchoolColor: GetKVPValue(event.Passthrough, "schoolColor"),
					SchoolName:  GetKVPValue(event.Passthrough, "schoolName"),
					FbID:        GetKVPValue(event.Passthrough, "fbid"),
					Instagram:   GetKVPValue(event.Passthrough, "instagram"),
					Social:      GetKVPValue(event.Passthrough, "social"),
					Why:         GetKVPValue(event.Passthrough, "why"),
				}
				output = append(output, contactInfo)
			}

			outputJSON, _ := json.Marshal(output)
			fmt.Println(string(outputJSON))
			psresult := topic.Publish(ctx, &pubsub.Message{
				Data: outputJSON,
				Attributes: map[string]string{
					"eventid": input.EventID,
					"listid":  GetKVPValue(event.Passthrough, "listid"),
					"form":    GetKVPValue(event.Passthrough, "form"),
				},
			})
			psid, err := psresult.Get(ctx)
			_, err = psresult.Get(ctx)
			if err != nil {
				log.Printf("%v Could not pub to pubsub: %v", input.EventID, err)
				return nil
			}
			log.Printf("%v pubbed record as message id %v: %v", input.EventID, psid, string(outputJSON))
		}
	}
	return nil
}

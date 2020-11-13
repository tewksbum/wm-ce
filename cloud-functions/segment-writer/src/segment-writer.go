package segmentwriter

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"

	// cloud sql driver
	_ "github.com/go-sql-driver/mysql"

	secretmanager "cloud.google.com/go/secretmanager/apiv1beta1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1beta1"
)

var projectID = os.Getenv("GCP_PROJECT")
var cfRegion = os.Getenv("FUNCTION_REGION")

var (
	smClient *secretmanager.Client
	dbsecret dbSecrets
	db *sql.DB
	psPeople *sql.Stmt
	psPeopleLink *sql.Stmt
	psPeopleSig *sql.Stmt
	psPeopleDelete *sql.Stmt
	ctx context.Context
)

func init() {
	ctx = context.Background()
	smClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to setup client: %v", err)
	}
	secretReq := &secretmanagerpb.AccessSecretVersionRequest{
		Name: os.Getenv("MYSQL_DSN"),
	}
	secretresult, err := smClient.AccessSecretVersion(ctx, secretReq)
	if err != nil {
		log.Fatalf("failed to get secret: %v", err)
	}
	secretsData := secretresult.Payload.Data

	if err := json.Unmarshal(secretsData, &dbsecret); err != nil {
		log.Fatalf("error decoding secrets %v", err)
		return
	}

	db, err = sql.Open("mysql", string(secretsData))
	if err != nil {
		log.Printf("error opening db %v", err)
	}

	psPeople, err = db.Prepare(`
	replace into dim_wm_people (people_key, sponsor, salutation, nickname, fname, finitial, mname, lname, ad1, ad1no, 
	ad2, ad3, city, state, zip, zip5, country, mailroute, adtype, ziptype, 
	recordtype, adbook, adparser, adcorrect, advalid, email, phone, trustedid, clientid, gender, 
	age, dob, organization, title, role, status, perme, permm, perms)
	values (
		?,?,?,?,?,?,?,?,?,?,
		?,?,?,?,?,?,?,?,?,?,
		?,?,?,?,?,?,?,?,?,?,
		?,?,?,?,?,?,?,?,?
	)
	`)
	if err != nil {
		log.Printf("error preparing statement %v", err)
	}

	psPeopleLink, err = db.Prepare(`
	replace into link_wm_people (people_key, external_source, external_key) values (?,?,?)
	`)
	if err != nil {
		log.Printf("error preparing statement %v", err)
	}

	psPeopleSig, err = db.Prepare(`
	replace into link_wm_people_sig (people_key, owner_id, source, event_id, event_type, fiber_type, record_id, fiber_id) values (?,?,?,?,?,?,?,?)
	`)
	if err != nil {
		log.Printf("error preparing statement %v", err)
	}

	psPeopleDelete, err = db.Prepare(`
	delete from dim_wm_people where people_key = ?
	`)
	if err != nil {
		log.Printf("error preparing statement %v", err)
	}

}

func ProcessOutput(ctx context.Context, m PubSubMessage) error {
	var input GoldenOutput
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Error: Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	if len(input.ID) > 0 {
		{
			_, err := psPeople.Exec(
				input.ID, input.Sponsor, input.People.SALUTATION, input.People.NICKNAME, input.People.FNAME,
				input.People.FINITIAL, input.People.MNAME, input.People.LNAME, input.People.AD1, input.People.AD1NO,
				input.People.AD2, input.People.AD3, input.People.CITY, input.People.STATE, input.People.ZIP,
				input.People.ZIP5, input.People.COUNTRY, input.People.MAILROUTE, input.People.ADTYPE, input.People.ZIPTYPE,
				input.People.RECORDTYPE, input.People.ADBOOK, input.People.ADPARSER, input.People.ADCORRECT, input.People.ADVALID,
				input.People.EMAIL, input.People.PHONE, input.People.TRUSTEDID, input.People.CLIENTID, input.People.GENDER,
				input.People.AGE, input.People.DOB, input.People.ORGANIZATION, input.People.TITLE, input.People.ROLE,
				input.People.STATUS, input.People.PermE, input.People.PermM, input.People.PermS, 
			)
			if err != nil {
				log.Printf("Error running insertFiberSet: %v", err)
			} else {
				if len(input.ExternalIDs) > 0 {
					for k, vl := range input.ExternalIDs {
						for _, v := range vl {
							_, err := psPeopleLink.Exec(input.ID, k, v) // this will cascade delete link_wm_people
							if err != nil {
								log.Printf("Error running psPeopleLink: %v", err)
							}
						}
			
					}
				}
		
				if len(input.Signatures) > 0 {
					for _, sig := range input.Signatures {
						_, err := psPeopleSig.Exec(input.ID, sig.OwnerID, sig.Source, sig.EventID, sig.EventType, sig.FiberType, sig.RecordID, sig.EventID) // this will cascade delete link_wm_people
						if err != nil {
							log.Printf("Error running psPeopleSig: %v", err)
						}
					}
				}
			
				if len(input.ExpiredIDs) > 0 {
					for _, e := range input.ExpiredIDs {
						_, err := psPeopleDelete.Exec(e) // this will cascade delete link_wm_people
						if err != nil {
							log.Printf("Error running psPeopleDelete: %v", err)
						}
					}
				}
			}
		}
	
	}

	

	return nil
}

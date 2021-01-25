// Package dsdumper is a CF that dumps all golden records out of DS into MySQL
package dsdumper

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"regexp"
	"time"

	"cloud.google.com/go/datastore"

	// cloud sql driver
	_ "github.com/go-sql-driver/mysql"

	secretmanager "cloud.google.com/go/secretmanager/apiv1beta1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1beta1"
)

// ProjectID is the env var of project id
var pid = os.Getenv("PROJECTID")
var did = os.Getenv("DSPROJECTID")
var env = os.Getenv("ENVIRONMENT")
var uid = os.Getenv("CLIENTID")
var pwd = os.Getenv("CLIENTSECRET")
var kind = "people-golden"

var (
	smClient *secretmanager.Client
	dbsecret dbSecrets
	db       *sql.DB
	psPeople *sql.Stmt
	ctx      context.Context
	fs       *datastore.Client
)

var ds *datastore.Client

func init() {
	ctx = context.Background()
	ds, _ = datastore.NewClient(ctx, pid)
	fs, _ = datastore.NewClient(ctx, did)

	smClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to setup client: %v", err)
	}
	secretReq := &secretmanagerpb.AccessSecretVersionRequest{
		Name: os.Getenv("SEGMENT_MYSQL_SECRET"),
	}
	secretresult, err := smClient.AccessSecretVersion(ctx, secretReq)
	if err != nil {
		log.Fatalf("failed to get secret: %v", err)
	}
	secretsData := secretresult.Payload.Data

	db, err = sql.Open("mysql", string(secretsData))
	if err != nil {
		log.Printf("error opening db %v", err)
	}

	psPeople, err = db.Prepare(`
	replace into wm_people (people_key, sponsor, salutation, nickname, fname, finitial, mname, lname, ad1, ad1no, 
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
	log.Printf("init completed")
}

// ProcessRequest Receives a http event request
func ProcessRequest(w http.ResponseWriter, r *http.Request) {
	query := datastore.NewQuery("__namespace__").KeysOnly()
	namespaces, err := fs.GetAll(ctx, query, nil)
	if err != nil {
		log.Printf("error getting firestore namespaces: %v", err)
	}
	rens, _ := regexp.Compile("^" + env + "-")
	for _, n := range namespaces {
		if rens.MatchString(n.Name) {
			dumpGolden(n.Name)
		}
	}
}

func dumpGolden(ns string) {
	query := datastore.NewQuery(kind).Namespace(ns).KeysOnly()
	goldenKeys, _ := fs.GetAll(ctx, query, nil)

	// get the golden records
	goldens := make([]PeopleGoldenDS, len(goldenKeys), len(goldenKeys))
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
				log.Printf("Error fetching golden records %v", err)
			}

		}

		for _, input := range goldens {
			_, err := psPeople.Exec(
				input.ID.Name, ns[5:], input.SALUTATION, input.NICKNAME, input.FNAME,
				input.FINITIAL, input.MNAME, input.LNAME, input.AD1, input.AD1NO,
				input.AD2, input.AD3, input.CITY, input.STATE, input.ZIP,
				input.ZIP5, input.COUNTRY, input.MAILROUTE, input.ADTYPE, input.ZIPTYPE,
				input.RECORDTYPE, input.ADBOOK, input.ADPARSER, input.ADCORRECT, input.ADVALID,
				input.EMAIL, input.PHONE, input.TRUSTEDID, input.CLIENTID, input.GENDER,
				input.AGE, input.DOB, input.ORGANIZATION, input.TITLE, input.ROLE,
				input.STATUS, input.PermE, input.PermM, input.PermS,
			)
			if err != nil {
				log.Printf("error inserting into db %v", err)
			}

		}
	}

}

type PeopleGoldenDS struct {
	ID           *datastore.Key `datastore:"__key__"`
	CreatedAt    time.Time      `datastore:"createdat"`
	Search       []string       `datastore:"search"`
	SALUTATION   string         `datastore:"salutation"`
	NICKNAME     string         `datastore:"nickname"`
	FNAME        string         `datastore:"fname"`
	FINITIAL     string         `datastore:"finitial"`
	LNAME        string         `datastore:"lname"`
	MNAME        string         `datastore:"mname"`
	AD1          string         `datastore:"ad1"`
	AD1NO        string         `datastore:"ad1no"`
	AD2          string         `datastore:"ad2"`
	AD3          string         `datastore:"ad3"`
	CITY         string         `datastore:"city"`
	STATE        string         `datastore:"state"`
	ZIP          string         `datastore:"zip"`
	ZIP5         string         `datastore:"zip5"`
	COUNTRY      string         `datastore:"country"`
	MAILROUTE    string         `datastore:"mailroute"`
	ADTYPE       string         `datastore:"adtype"`
	ZIPTYPE      string         `datastore:"ziptype"`
	RECORDTYPE   string         `datastore:"recordtype"`
	ADBOOK       string         `datastore:"adbook"`
	ADPARSER     string         `datastore:"adparser"`
	ADCORRECT    string         `datastore:"adcorrect"`
	ADVALID      string         `datastore:"advalid"`
	EMAIL        string         `datastore:"email"`
	PHONE        string         `datastore:"phone"`
	TRUSTEDID    string         `datastore:"trustedid"`
	CLIENTID     string         `datastore:"clientid"`
	GENDER       string         `datastore:"gender"`
	AGE          string         `datastore:"age"`
	DOB          string         `datastore:"dob"`
	ORGANIZATION string         `datastore:"organization"`
	TITLE        string         `datastore:"title"`
	ROLE         string         `datastore:"role"`
	STATUS       string         `datastore:"status"`
	PermE        string         `datastore:"perme"`
	PermM        string         `datastore:"permm"`
	PermS        string         `datastore:"perms"`
}

type dbSecrets struct {
	MySQLDSN string `json:"mysql"`
}

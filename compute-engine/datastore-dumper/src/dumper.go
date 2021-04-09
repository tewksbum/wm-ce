// Package dsdumper is a CF that dumps all golden records out of DS into MySQL
package dsdumper

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"strings"
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
var kindFiber = "people-fiber"

var (
	db            *sql.DB
	psPeople      *sql.Stmt
	psPeopleFiber *sql.Stmt
	ctx           context.Context
	fs            *datastore.Client
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

	psPeopleFiber, err = db.Prepare(`
	replace into wm_people_fiber (people_fiber_key, sponsor, salutation, nickname, fname, finitial, mname, lname, ad1, ad1no, 
	ad2, ad3, city, state, zip, zip5, country, mailroute, adtype, ziptype, 
	recordtype, adbook, adparser, adcorrect, advalid, email, phone, trustedid, clientid, gender, 
	age, dob, organization, title, role, status, perme, permm, perms, recordid, eventid, created_at)
	values (
		?,?,?,?,?,?,?,?,?,?,
		?,?,?,?,?,?,?,?,?,?,
		?,?,?,?,?,?,?,?,?,?,
		?,?,?,?,?,?,?,?,?,?,?,?
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

func dumpFiber(ns string) {
	fiberQuery := datastore.NewQuery(kindFiber).Namespace(ns).KeysOnly()
	fiberKeys, _ := fs.GetAll(ctx, fiberQuery, nil)

	// get the fibers records
	fibers := make([]PeopleFiberDS, len(fiberKeys), len(fiberKeys))
	if len(fiberKeys) > 0 {
		batchSize := 1000
		l := len(fiberKeys) / batchSize

		if len(fiberKeys)%batchSize > 0 {
			l++
		}
		for r := 0; r < l; r++ {
			s := r * 1000
			e := s + 1000

			if e > len(fiberKeys) {
				e = len(fiberKeys)
			}

			gk := fiberKeys[s:e]
			gd := fibers[s:e]

			if err := fs.GetMulti(ctx, gk, gd); err != nil && err != datastore.ErrNoSuchEntity {
				log.Printf("Error fetching fibers records %v", err)
			}

		}

		log.Printf("Namespace %v Length Fiber %v", ns, len(fibers))

			for _, input := range fibers {
				_, err := psPeopleFiber.Exec(
					input.ID.Name, ns[5:], strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "SALUTATION").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "NICKNAME").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "FNAME").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "FINITIAL").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "MNAME").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "LNAME").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "AD1").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "AD1NO").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "AD2").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "AD3").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "CITY").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "STATE").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "ZIP").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "ZIP5").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "COUNTRY").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "MAILROUTE").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "ADTYPE").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "ZIPTYPE").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "RECORDTYPE").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "ADBOOK").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "ADPARSER").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "ADCORRECT").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "ADVALID").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "EMAIL").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "PHONE").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "TRUSTEDID").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "CLIENTID").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "GENDER").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "AGE").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "DOB").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "ORGANIZATION").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "TITLE").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "ROLE").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "STATUS").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "PermE").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "PermM").Value),
					strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&input, "PermS").Value),
					input.RecordID,
					input.EventID,
					input.CreatedAt,
				)
			if err != nil {
				log.Printf("error inserting into db %v", err)
			}
		}
	}
}

func GetMatchKeyFieldFromDSFiber(v *PeopleFiberDS, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
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

type PeopleFiberDS struct {
	ID           *datastore.Key   `datastore:"__key__"`
	CreatedAt    time.Time        `datastore:"createdat"`
	OwnerID      string           `datastore:"ownerid"`
	Source       string           `datastore:"source"`
	EventID      string           `datastore:"eventid"`
	EventType    string           `datastore:"eventtype"`
	RecordID     string           `datastore:"recordid"`
	FiberType    string           `datastore:"fibertype"`
	Disposition  string           `datastore:"disposition"`
	Passthrough  []Passthrough360 `datastore:"passthrough"`
	Search       []string         `datastore:"search"`
	SALUTATION   MatchKeyField    `datastore:"salutation"`
	NICKNAME     MatchKeyField    `datastore:"nickname"`
	FNAME        MatchKeyField    `datastore:"fname"`
	FINITIAL     MatchKeyField    `datastore:"finitial"`
	LNAME        MatchKeyField    `datastore:"lname"`
	MNAME        MatchKeyField    `datastore:"mname"`
	AD1          MatchKeyField    `datastore:"ad1"`
	AD1NO        MatchKeyField    `datastore:"ad1no"`
	AD2          MatchKeyField    `datastore:"ad2"`
	AD3          MatchKeyField    `datastore:"ad3"`
	CITY         MatchKeyField    `datastore:"city"`
	STATE        MatchKeyField    `datastore:"state"`
	ZIP          MatchKeyField    `datastore:"zip"`
	ZIP5         MatchKeyField    `datastore:"zip5"`
	COUNTRY      MatchKeyField    `datastore:"country"`
	MAILROUTE    MatchKeyField    `datastore:"mailroute"`
	ADTYPE       MatchKeyField    `datastore:"adtype"`
	ZIPTYPE      MatchKeyField    `datastore:"ziptype"`
	RECORDTYPE   MatchKeyField    `datastore:"recordtype"`
	ADBOOK       MatchKeyField    `datastore:"adbook"`
	ADPARSER     MatchKeyField    `datastore:"adparser"`
	ADCORRECT    MatchKeyField    `datastore:"adcorrect"`
	ADVALID      MatchKeyField    `datastore:"advalid"`
	EMAIL        MatchKeyField    `datastore:"email"`
	PHONE        MatchKeyField    `datastore:"phone"`
	TRUSTEDID    MatchKeyField    `datastore:"trustedid"`
	CLIENTID     MatchKeyField    `datastore:"clientid"`
	GENDER       MatchKeyField    `datastore:"gender"`
	AGE          MatchKeyField    `datastore:"age"`
	DOB          MatchKeyField    `datastore:"dob"`
	ORGANIZATION MatchKeyField    `datastore:"organization"`
	TITLE        MatchKeyField    `datastore:"title"`
	ROLE         MatchKeyField    `datastore:"role"`
	STATUS       MatchKeyField    `datastore:"status"`
	PermE        MatchKeyField    `datastore:"perme"`
	PermM        MatchKeyField    `datastore:"permm"`
	PermS        MatchKeyField    `datastore:"perms"`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
	Type   string `json:"type"`
}

type Passthrough360 struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type dbSecrets struct {
	MySQLDSN string `json:"mysql"`
}

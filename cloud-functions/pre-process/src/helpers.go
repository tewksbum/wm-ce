package preprocess

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"sort"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/gomodule/redigo/redis"
	"github.com/xojoc/useragent"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

func publishReport(report *FileReport, cfName string) {
	reportJSON, _ := json.Marshal(report)
	reportPub := topicR.Publish(ctx, &pubsub.Message{
		Data: reportJSON,
		Attributes: map[string]string{
			"source": cfName,
		},
	})
	_, err := reportPub.Get(ctx)
	if err != nil {
		log.Printf("ERROR Could not pub to reporting pubsub: %v", err)
	}
}

func MapNER(column InputColumn, ner map[string]float64) {
	for k, v := range ner {
		switch k {
		case "FAC":
			column.NER.FAC = v
			break
		case "GPE":
			column.NER.GPE = v
			break
		case "LOC":
			column.NER.LOC = v
			break
		case "NORP":
			column.NER.NORP = v
			break
		case "ORG":
			column.NER.ORG = v
			break
		case "PERSON":
			column.NER.PERSON = v
			break
		case "PRODUCT":
			column.NER.PRODUCT = v
			break
		case "EVENT":
			column.NER.EVENT = v
			break
		case "WORKOFART":
			column.NER.WORKOFART = v
			break
		case "LAW":
			column.NER.LAW = v
			break
		case "LANGUAGE":
			column.NER.LANGUAGE = v
			break
		case "DATE":
			column.NER.DATE = v
			break
		case "TIME":
			column.NER.TIME = v
			break
		case "PERCENT":
			column.NER.PERCENT = v
			break
		case "MONEY":
			column.NER.MONEY = v
			break
		case "QUANTITY":
			column.NER.QUANTITY = v
			break
		case "ORDINAL":
			column.NER.ORDINAL = v
			break
		case "CARDINAL":
			column.NER.CARDINAL = v
			break

		}
	}
}

// this runs per column passed to it...
func GetPeopleERR(column string) PeopleERR {
	var err PeopleERR

	key := strings.TrimSpace(strings.ToLower(column))
	//TODO: go through and take anything ownerspecific out of this list... and make it cached dynamic
	switch key {
	case "fname", "f name", "f_name", "first name", "firstname", "name first", "namefirst", "name_first", "first_name", "first", "nickname", "given name", "given_name", "student first name", "student first", "student-first", "preferred name", "name preferred", "chosen name", "patron.first name", "firstpreferredname", "prei_name", "std first", "fn", "cx first name", "applicant: preferred name mailing", "greeting_name", "preferred", "irst name", "nick/first", "first name (given name)", "first name preferred", "legal name", "student_first name", "first nam":
		err.FirstName = 1
	case "lname", "lname ", "l name ", "l_name", "last name", "last_name", "name last", "namelast", "name_last", "last", "surname", "student last name", "patron.last name", "keyname", "student-last", "student last", "std last", "ln", "cx last name", "lash name", "lastname", "last ame", "last name (surname)", "student_last":
		err.LastName = 1
	case "mi", "mi ", "mname", "m", "middle name", "middle_name", "student middle name", "mid", "middlename", "middle":
		err.MiddleName = 1
	case "suffix", "jr., iii, etc.", "sfix", "student_suffix":
		err.Suffix = 1
	case "ad", "ad1", "ad1 ", "add1", "add 1", "address 1", "ad 1", "address line 1", "street line 1", "street address 1", "streetaddress1", "address1", "street", "street_line1", "street address line 1", "addr_line_1", "address street line 1", "street 1", "street address", "permanent street 1", "parent street", "home street", "home address line 1", "hom address line 1", "number_and_street", "street1_pr", "pa street address line 1", "line1", "line 1", "delivery address", "current delivery address", "address line1", "addr1", "permanent address street 1", "prstr1", "home addr svsu", "home addr1", "address (preferred one line)", "parent street 1", "add line1", "active street 1", "home mailing 1", "addrline1", "home address (street)", "street1_line1", "str1":
		err.Address1 = 1
	case "ad2", "add2", "ad 2", "address 2", "address line 2", "street line 2", "street address 2", "streetaddress2", "address2", "street_line2", "street 2", "street address line 2", "addr_line_2", "address1b", "permanent street 2", "home street 2", "home address line 2", "hom address line 2", "parent street 2", "street2_pr", "pa street address line 2", "line2", "line 2", "address street line 2", "address line2", "home addr2", "street2", "add line2", "active street 2", "home mailing 2", "addrline2", "street1_line2", "apt #", "str2":
		err.Address2 = 1
	case "ad3", "add3", "ad 3", "address 3", "address line 3", "street line 3", "street address 3", "address3", "street_line3", "street 3", "street address line 3", "addr_line_3", "line3", "line 3", "address street line 3", "address line3", "home addr3", "street3", "str3":
		err.Address3 = 1
	case "ad4", "add4", "ad 4", "address 4", "address line 4", "street line 4", "street address 4", "address4", "street_line4", "street 4", "street address line 4", "addr_line_4", "address street line 4", "home addr4":
		err.Address4 = 1
	case "mailing street", "mailing_street", "mailing address street", "mailing state", "mailing province":
		err.Address1 = 1
	case "city", "city ", "street city", "home city", "city_pr", "pa city", "current city", "parent city", "scity", "active city", "home mailing city", "address city", "city1":
		err.City = 1
	case "state", "st", "state ", "state_province", "st ", "state province", "street state", "parent state", "home state province", "state/province", "state_territory_cd", "state_pr", "pa state", "stateorprovince", "home state", "state_code", "active region", "home mailing state", "address state", "state code", "region", "state (u.s.)", "state1":
		err.State = 1
	case "zip", "zip1", "zip code", "zip_code", "zipcode", "zip ", "postal_code", "postal code", "postalcode", "zip postcode", "street zip", "postcode", "post code", "postal", "home_postal", "perm_zip", "permanenthomezippostalcode", "home zip postcode", "parent zip", "zip_pr", "pa zipcode", "zip5", "zip+4", "home zip", "active postal", "home mailing zip", "address zip", "zip/postal code":
		err.ZipCode = 1
	case "citystzip", "city/st/zip ", "city/state/zip", "city/state", "city, state zip", "city, state, zip", "address line 2 - calculated", "address line 3 - calculated", "Home Csz Svsu", "csz", "home (preferred) csz", "citystatezip", "city  state  zip (include country if not us)":
		err.City = 1
		err.State = 1
		err.ZipCode = 1
	case "county":
		err.County = 1
	case "country", "country (blank for us)", "home_country", "home country", "address country", "address country name", "pa nation", "nation", "country description", "parent nation", "active country", "country description 4", "natn_desc1":
		err.Country = 1
	case "address", "student address", "parent address", "home address", "permanent address":
		err.FullAddress = 1
	case "email", "student email", "email ", "email1", "email address", "stu_email", "student e mail", "studentemail", "student personal email address", "student emails", "student e-mail", "student personal email", "student email address", "email2", "email_address_2", "student school email", "naz_email", "student school email_1", "student school email_2", "email address:", "winthrop email":
		err.Email = 1
	case "par_email", "par_email1", "parent e-mail", "par email", "parent email", "parent email address", "par_email2", "father_email", "mother_email", "parent_1's email", "parent_2's email", "parent's e-mail address", "parent/guardian email address:", "parent/guardian email", "p1_email":
		// err.Email = 1
		err.ParentEmail = 1
	case "gender", "m/f", "sex", "student sex", "student gender", "gender description", "gender description 3":
		err.Gender = 1
	case "pfname", "pfname1", "pfname2", "parent first name", "parent_first_name", "parent fname", "parent_fname", "father_first", "mother_first", "father first", "mother first", "parent/guardian first name", "sp_first":
		err.ParentFirstName = 1
	case "plname", "plname1", "plname2", "parent last name", "parent_last_name", "parent lname", "parent_lname", "father_last", "mother_last", "father last", "mother last", "parent/guardian last name", "sp_last":
		err.ParentLastName = 1
	case "phone", "phone1", "hphone", "cphone", "mphone", "phone mobile cell", "mobile":
		err.Phone = 1
	case "bday", "birthday":
		err.Birthday = 1
	case "age":
		err.Age = 1
	case "pname", "pname1", "pname2", "pname 1", "pname 2", "purchaser", "guardian", "guardian name", "guardian_name", "parent", "parent name", "parent_name", "parents names", "parent full name", "sp_name":
		err.ParentFirstName = 1
		err.ParentLastName = 1
		err.ParentName = 1
	case "fullname", "full name", "full_name", "full name (last, first)", "student name", "students name", "application: applicant", "last, first", "ekuname", "name", "individual name", "student name - last, first, middle", "lfm name", "preferredname", "entry name", "name lfm", "resident: full name", "studentname", "person name", "student full name", "student":
		err.FullName = 1
		err.FirstName = 1
		err.LastName = 1
	case "dorm", "hall", "building", "building name", "dormitory", "apartment", "fraternity", "residence", "hall assignment":
		err.Dorm = 1
	case "room", "room number", "room #":
		err.Room = 1
	case "organization":
		err.Organization = 1
	case "title", "course year", "grad date", "class", "class year", "grade", "admit status", "student status", "student type", "studenttype", "yr_cde", "enrollment class", "classification description", "classification description 6", "student_classificaiton", "classlvl", "class status", "classstanding", "yos", "incoming classification code":
		// also see contains logic...
		// lots of thing mashed up here... could we / should we split out SchoolYear & ClassStanding?  From a detection standpoint...
		err.Title = 1
	case "status":
		err.Status = 1
	case "schoolyear":
		err.SchoolYear = 1
	case "studentid", "student id", "student_id", "id", "applicant", "pkid", "student number", "student no", "studentnumber", "student id #", "uin", "student g#", "ps_id", "tech id", "tech id #", "idnumber", "bannerid", "splash id", "gid", "wmid", "gclid":
		err.TrustedID = 1
	case "role":
		err.ContainsStudentRole = 1
	case "parent(s) of", "v-lookup", "vlookup", "unique", "institution_descr", "mailer type", "file output date", "crm", "com", "distribution designation", "q distribution", "b distribution", "c distribution", "salutation slug", "program", "adcode", "empty", "school code", "addressee", "addr_type_cd", "salutation", "degr. stat", "degree sou", "degree", "gpa", "major1", "major2", "major3", "minor1", "minor2", "minor3", "residence type", "return code", "bldg_cde", "current enrollment status code", "planned enrollment session code", "application type", "restrict flag", "address type", "contact owner", "agreed to be listed", "communication: release: permaddress", "staff assigned name", "a2s attribute", "act score", "adms attribute", "admt-code", "registered", "sat score", "spad attribute", "ucol attribute", "current status", "meal plan", "insurance company", "disability or medical condition", "medical condition", "medication", "child", "child first", "child last":
		err.Junk = 1
	case "level", "room location description 1":
		// may want to unjunk the degree level things...
		err.Junk = 1
	case "perme", "permission email":
		err.PermE = 1
	case "permm", "permission mail":
		err.PermM = 1
	case "perms", "permission share":
		err.PermS = 1
	}

	if (strings.Contains(key, "first") && strings.Contains(key, "name")) || (strings.Contains(key, "nick") && strings.Contains(key, "name")) || strings.Contains(key, "fname") || (strings.Contains(key, "preferred") && strings.Contains(key, "name")) {
		err.ContainsFirstName = 1
	}
	if (strings.Contains(key, "last") && strings.Contains(key, "name")) || strings.Contains(key, "lname") || strings.Contains(key, "surname") {
		err.ContainsLastName = 1
	}
	if strings.Contains(key, "name") {
		err.ContainsName = 1
	}
	if strings.Contains(key, "email") || strings.Contains(key, "e-mail") {
		err.ContainsEmail = 1
	}
	if (strings.Contains(key, "address") || strings.Contains(key, "addr") || strings.Contains(key, "addrss") || strings.Contains(key, "street 1")) && (!strings.Contains(key, "room") && !strings.Contains(key, "hall")) || strings.Contains(key, "line 1") || strings.Contains(key, "line1") {
		// TODO: unpack this room & hall when we fix MAR
		err.ContainsAddress = 1
	}
	if strings.Contains(key, "street 2") || strings.Contains(key, "streetcd2") || strings.Contains(key, "address 2") || strings.Contains(key, "address2") || strings.Contains(key, "line2") || strings.Contains(key, "line 2") {
		err.Address2 = 1
	}
	if err.Address2 == 0 && err.ContainsAddress == 0 && strings.Contains(key, "street") {
		err.ContainsAddress = 1
	}

	if strings.Contains(key, "city") || strings.Contains(key, "town") || strings.Contains(key, "municipal") {
		err.ContainsCity = 1
	}
	if strings.Contains(key, "state") || strings.Contains(key, "province") {
		err.ContainsState = 1
	}
	if strings.Contains(key, "zip") || strings.Contains(key, "postalcode") || strings.Contains(key, "postal code") || strings.Contains(key, "postcode") {
		err.ContainsZipCode = 1
		err.ZipCode = 1
	}
	if strings.Contains(key, "country") || strings.Contains(key, "nation") || strings.Contains(key, "territory") || strings.Contains(key, "homeland") {
		err.ContainsCountry = 1
	}
	if strings.Contains(key, "phone") || strings.Contains(key, "mobile") {
		err.ContainsPhone = 1
	}
	if strings.Contains(key, "email status") || strings.Contains(key, "parent(s) of") || strings.HasPrefix(key, "to the ") || strings.HasPrefix(key, "v-lookup") || strings.HasPrefix(key, "campus") {
		// strings.Contains(key, "description") ||
		err.Junk = 1
	}
	// these should be looked up on a per owner basis
	if strings.Contains(key, "class") || strings.Contains(key, "year") || strings.Contains(key, "class year") || strings.Contains(key, "classification description") {
		// this is REALLY broad, dangerous Contains...
		err.ContainsTitle = 1
	}
	if strings.Contains(key, "parent") || strings.Contains(key, "emergency") || strings.Contains(key, "contact") || strings.Contains(key, "father") || strings.Contains(key, "mother") || strings.Contains(key, "purchaser") || strings.Contains(key, "gaurdian") || strings.Contains(key, "guardian") || strings.Contains(key, "related") || strings.HasPrefix(key, "p1_") {
		err.ContainsRole = 1
	}
	// correct some assignments
	if err.City == 1 || err.State == 1 || err.ZipCode == 1 || err.Email == 1 || err.Country == 1 {
		err.Address1 = 0
	}
	if strings.Contains(key, "first") && strings.Contains(key, "name") {
		err.Address1 = 0
	}
	if strings.Contains(key, "last") && strings.Contains(key, "name") {
		err.Address1 = 0
	}
	if err.Organization == 1 {
		err.FirstName = 0
		err.LastName = 0
	}
	if err.MiddleName == 1 {
		err.FirstName = 0
		err.LastName = 0
	}

	if err.Junk == 1 {
		err.FirstName = 0
		err.LastName = 0
		err.Address1 = 0
		err.ContainsRole = 0
		err.ContainsName = 0
		err.ParentFirstName = 0
		err.ParentLastName = 0
		err.ParentName = 0
		err.FullName = 0
	}

	// evaluate physical address
	err.AddressTypeBusiness = 0 // TODO: add logic to detect business address
	if err.Address1 == 1 || err.City == 1 || err.State == 1 || err.ZipCode == 1 || err.Email == 1 || err.ContainsAddress == 1 || err.FullAddress == 1 {
		err.AddressBookBill = 1      // default to home address
		err.AddressTypeResidence = 1 // TODO: this is a hack...
		if strings.Contains(key, "consignment") {
			err.AddressBookShip = 1
		} else if strings.Contains(key, "order") {
			err.AddressBookBill = 1
		} else if strings.Contains(key, "emergency") || strings.Contains(key, "permanent") || strings.Contains(key, "home") {
			err.AddressTypeResidence = 1
			err.AddressBookBill = 1
		} else if err.Dorm == 1 {
			err.AddressTypeCampus = 1
		}
	}
	return err
}

func GetCampaignERR(column string) CampaignERR {
	var err CampaignERR
	key := strings.ToLower(column)
	switch key {
	case "campaign id", "campaignid", "campaign.id":
		err.CampaignID = 1
	case "campaign", "campaign name", "campaignname", "campaign.name":
		err.Name = 1
	case "campaign type", "campaigntype", "campaign.type":
		err.Type = 1
	case "campaign budget", "campaignbudget", "budget", "campaign.budget":
		err.Budget = 1
	case "campaign channel", "campaignchannel", "campaign.channel":
		err.Channel = 1
	case "campaign start date", "campaign startdate", "campaignstartdate", "campaign.startdate":
		err.StartDate = 1
	case "campaign end date", "campaign enddate", "campaignenddate", "campaignend.date":
		err.EndDate = 1
	}
	return err
}

func GetConsignmentERR(column string) ConsignmentERR {
	var err ConsignmentERR
	key := strings.ToLower(column)
	switch key {
	case "ship date", "shipdate":
		err.ShipDate = 1
	case "shipment", "consignment", "consignment id", "consignmentid":
		err.ID = 1
	}

	// adding logic for flattened order source
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "consignments") && strings.Contains(key, ".id") {
		err.ID = 1
	}

	return err
}

func GetEventERR(column string) EventERR {
	var err EventERR
	key := strings.ToLower(column)
	switch key {
	case "event id", "eventid", "event.id":
		err.ID = 1
	case "event type", "eventtype", "event.type":
		err.Type = 1
	case "campaign id", "campaignid", "campaign.id":
		err.CampaignID = 1
	case "browser":
		err.Browser = 1
	case "channel":
		err.Channel = 1
	case "os":
		err.OS = 1
	case "domain":
		err.Domain = 1
	case "url":
		err.URL = 1
	case "geo", "lat", "long":
		err.Location = 1
	case "referrer":
		err.Referrer = 1
	case "searchterm":
		err.SearchTerm = 1
	}
	return err
}

func GetOrderERR(column string) OrderERR {
	var err OrderERR
	key := strings.ToLower(column)
	switch key {
	case "orderid", "order id", "invoiceid", "invoice id", "order.id":
		err.ID = 1
	case "order number", "ordernumber", "full order number", "full ordernumber",
		"fullorder number", "fullordernumber", "ecometryordernumber":
		err.Number = 1
	case "order date", "orderdate", "invoice date", "invoicedate",
		"placed date", "placeddate", "created at", "createdat":
		err.Date = 1
	case "order subtotal", "ordersubtotal", "subtotal":
		err.SubTotal = 1
	case "order discount", "orderdiscount", "discount":
		err.Discount = 1
	case "order shipping", "ordershipping", "shipping":
		err.Shipping = 1
	case "order tax", "ordertax", "tax":
		err.Tax = 1
	case "order total", "ordertotal", "total":
		err.Total = 1
	// for de-nested node case...
	case "order.ecometryordernumber":
		err.Number = 1
	case "order.ektronuserid":
		err.CustomerID = 1
	case "order.placedat":
		err.Date = 1
	case "order.ordersubtotal", "order.subtotal":
		err.SubTotal = 1
	case "order.orderdiscount", "order.discount":
		err.Discount = 1
	case "order.ordershipping", "order.shipping":
		err.Shipping = 1
	case "order.ordertax", "order.tax":
		err.Tax = 1
	case "order.total":
		err.Total = 1
	case "order.channel":
		err.Channel = 1
	}

	return err
}

func GetOrderDetailERR(column string) OrderDetailERR {
	var err OrderDetailERR
	key := strings.ToLower(column)
	switch key {
	case "order detail id", "orderdetail id", "orderdetailid", "row", "line":
		err.ID = 1
	}

	// adding logic for flattened order source
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "shipments") && strings.Contains(key, "shipitems") && strings.Contains(key, ".id") {
		err.ID = 1
	}
	if strings.Contains(key, "ordernumber") {
		err.OrderNumber = 1
	}
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "shipments") && strings.Contains(key, "shipitems") && strings.Contains(key, ".orderid") {
		err.OrderID = 1
	}
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "shipments") && strings.Contains(key, "shipitems") && strings.Contains(key, ".productid") {
		err.ProductID = 1
	}
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "shipments") && strings.Contains(key, "shipitems") && strings.Contains(key, ".itemsku") {
		err.ProductSKU = 1
	}
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "shipments") && strings.Contains(key, "shipitems") && strings.Contains(key, ".quantity") {
		err.ProductQuantity = 1
	}
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "shipments") && strings.Contains(key, "shipitems") && strings.Contains(key, ".itemlob") {
		err.MasterCategory = 1
	}

	return err
}

func GetProductERR(column string) ProductERR {
	var err ProductERR
	key := strings.ToLower(column)
	switch key {
	case "product name", "productname", "prod name", "prodname", "product.name":
		err.Name = 1
	case "product description", "productdescription", "prod description", "product.description",
		"proddescription", "product desc", "productdesc", "prod desc",
		"proddesc", "p desc", "pdesc":
		err.Description = 1
	case "product size", "productsize", "prod size", "product.size",
		"p size", "psize", "size":
		err.Size = 1
	case "product color", "productcolor", "prod color", "product.color",
		"p color", "pcolor", "color":
		err.Color = 1
	case "product unit price", "productunit price", "prod unit price", "product.unitprice",
		"product unitprice", "productunitprice", "prod unitprice",
		"p unit price", "punit price", "p unitprice", "punitprice",
		"unit price", "unitprice":
		err.UnitPrice = 1
	case "product type", "producttype", "prod type", "product.type",
		"p type", "ptype", "type":
		err.Type = 1
	case "product vendorid", "productvendorid", "prod vendorid",
		"p vendorid", "pvendorid", "vendorid":
		err.VendorID = 1
	case "product vendor", "productvendor", "prod vendor",
		"p vendor", "pvendor", "vendor":
		err.Vendor = 1
	case "product cost", "productcost", "prod cost", "product.cost",
		"p cost", "pcost", "cost":
		err.Cost = 1
	case "product stars", "productstars", "prod stars", "product.stars",
		"p stars", "pstars", "stars":
		err.Stars = 1
	case "product category", "productcategory", "product cat", "product.category",
		"productcat", "prod cat", "prodcat", "p cat", "pcat":
		err.Category = 1
	case "product margin", "productmargin", "prod margin", "product.margin",
		"p margin", "pmargin", "margin", "contibution":
		err.Margin = 1
	case "contains", "bundle items", "bundleitems", "bundled items", "bundleditems",
		"kit items", "kititems":
		err.Contains = 1
	}
	return err
}

func PubMessage(topic *pubsub.Topic, data []byte) {
	psresult := topic.Publish(ctx, &pubsub.Message{
		Data: data,
	})
	psid, err := psresult.Get(ctx)
	_, err = psresult.Get(ctx)
	if err != nil {
		log.Fatalf("Could not pub to pubsub: %v", err)
	} else {
		log.Printf("pubbed to %v as message id %v: %v", topic, psid, string(data))
	}
}

func GetColumnsFromInput(input Input) []InputColumn {
	var columns []InputColumn

	for k, v := range input.Fields {
		column := InputColumn{
			Name:      k,
			Value:     v,
			PeopleERR: PeopleERR{},
			NER:       NER{},
			PeopleVER: PeopleVER{},
		}
		columns = append(columns, column)
	}
	return columns
}

func GetMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func GetLowerCaseSorted(m []string) []string {
	var result []string
	for _, k := range m {
		result = append(result, strings.ToLower(k))
	}
	sort.Strings(result)
	return result
}

func GetNERKey(sig Signature, columns []string) string {
	// concatenate all columnn headers together, in lower case
	keys := strings.Join(GetLowerCaseSorted(columns[:]), "-")
	hasher := sha1.New()
	io.WriteString(hasher, keys)
	return fmt.Sprintf("ner:%v:%v:%v:%x", sig.OwnerID, strings.ToLower(sig.Source), strings.ToLower(sig.EventType), hasher.Sum(nil))
}

func FindNER(key string) NERCache {
	var ner NERCache
	ms := msp.Get()
	s, err := redis.String(ms.Do("GET", key))
	if err == nil {
		json.Unmarshal([]byte(s), &ner)
	}
	return ner
}

func GetPeopleVER(column *InputColumn) PeopleVER {
	var val = strings.TrimSpace(column.Value)
	log.Printf("features values is %v", val)
	val = RemoveDiacritics(val)
	result := PeopleVER{
		HASHCODE:     int64(GetHash(val)),
		IS_FIRSTNAME: ContainsBool(listFirstNames, val),
		IS_LASTNAME:  ContainsBool(listLastNames, val),
		IS_STREET1:   reStreet1.MatchString(val),
		IS_STREET2:   reStreet2.MatchString(val),
		IS_STREET3:   reStreet3.MatchString(val),
		IS_CITY:      ContainsBool(listCities, val),
		IS_STATE:     ContainsBool(listStates, val),
		IS_ZIPCODE:   reZipcode.MatchString(val),
		IS_COUNTRY:   ContainsBool(listCountries, val),
		IS_EMAIL:     reEmail.MatchString(val),
		IS_PHONE:     rePhone.MatchString(val) && len(val) >= 10,
	}
	columnJ, _ := json.Marshal(result)
	log.Printf("current VER %v", string(columnJ))
	return result
}

func GetEventVER(column *InputColumn) EventVER {
	var val = strings.TrimSpace(column.Value)
	// log.Printf("features Event values is %v", val)
	val = RemoveDiacritics(val)
	browser := useragent.Parse(val)
	isBrowser := true
	if browser == nil {
		isBrowser = false
	}
	result := EventVER{
		IS_BROWSER: isBrowser,
		IS_CHANNEL: ContainsBool(listChannels, val),
	}
	// columnJ, _ := json.Marshal(result)
	// log.Printf("current Event VER %v", string(columnJ))
	return result
}

func GetHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func BuildMLData(cols []InputColumn) MLInput {
	var instances [][]float64
	for _, column := range cols {
		var instance []float64
		instance = append(instance, float64(column.PeopleERR.FirstName))
		instance = append(instance, float64(column.PeopleERR.LastName))
		instance = append(instance, float64(column.PeopleERR.MiddleName))
		instance = append(instance, float64(column.PeopleERR.Suffix))
		instance = append(instance, float64(column.PeopleERR.FullName))
		instance = append(instance, float64(column.PeopleERR.Address1))
		instance = append(instance, float64(column.PeopleERR.Address2))
		instance = append(instance, float64(column.PeopleERR.City))
		instance = append(instance, float64(column.PeopleERR.State))
		instance = append(instance, float64(column.PeopleERR.ZipCode))
		instance = append(instance, float64(column.PeopleERR.County))
		instance = append(instance, float64(column.PeopleERR.Country))
		instance = append(instance, float64(column.PeopleERR.Email))
		instance = append(instance, float64(column.PeopleERR.ParentEmail))
		instance = append(instance, float64(column.PeopleERR.Gender))
		instance = append(instance, float64(column.PeopleERR.Phone))
		instance = append(instance, float64(column.PeopleERR.ParentFirstName))
		instance = append(instance, float64(column.PeopleERR.ParentLastName))
		instance = append(instance, float64(column.PeopleERR.Birthday))
		instance = append(instance, float64(column.PeopleERR.Age))
		instance = append(instance, float64(column.PeopleERR.ParentName))
		instance = append(instance, float64(column.NER.PERSON))
		instance = append(instance, float64(column.NER.NORP))
		instance = append(instance, float64(column.NER.FAC))
		instance = append(instance, float64(column.NER.ORG))
		instance = append(instance, float64(column.NER.GPE))
		instance = append(instance, float64(column.NER.LOC))
		instance = append(instance, float64(column.NER.PRODUCT))
		instance = append(instance, float64(column.NER.EVENT))
		instance = append(instance, float64(column.NER.WORKOFART))
		instance = append(instance, float64(column.NER.LAW))
		instance = append(instance, float64(column.NER.LANGUAGE))
		instance = append(instance, float64(column.NER.DATE))
		instance = append(instance, float64(column.NER.TIME))
		instance = append(instance, float64(column.NER.PERCENT))
		instance = append(instance, float64(column.NER.MONEY))
		instance = append(instance, float64(column.NER.QUANTITY))
		instance = append(instance, float64(column.NER.ORDINAL))
		instance = append(instance, float64(column.NER.CARDINAL))
		instance = append(instance, float64(column.PeopleVER.HASHCODE))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_FIRSTNAME)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_LASTNAME)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_STREET1)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_STREET2)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_CITY)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_STATE)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_ZIPCODE)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_COUNTRY)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_EMAIL)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_PHONE)))

		instances = append(instances, instance)
	}
	var mlInput MLInput
	mlInput.Instances = instances
	return mlInput
}

func ReadJSONArray(ctx context.Context, client *storage.Client, bucket, object string) (map[string]bool, error) {
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var intermediate []string
	json.Unmarshal(data, &intermediate)

	result := make(map[string]bool)
	for _, s := range intermediate {
		result[s] = true
	}
	return result, nil
}

func ReadJsonMap(ctx context.Context, client *storage.Client, bucket, object string) (map[string]string, error) {
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var result map[string]string
	json.Unmarshal(data, &result)

	return result, nil
}

func ReadCityStateZip(ctx context.Context, client *storage.Client, bucket, object string) ([]CityStateZip, error) {
	var result []CityStateZip
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(data, &result)
	return result, nil
}

func RemoveDiacritics(value string) string {
	t := transform.Chain(norm.NFD, transform.RemoveFunc(IsMn), norm.NFC)
	result, _, _ := transform.String(t, value)
	return result
}

func Contains(dict map[string]bool, key string) uint32 {
	if _, ok := dict[strings.ToUpper(key)]; ok {
		return 1
	}
	return 0
}

func ContainsBool(dict map[string]bool, key string) bool {
	if _, ok := dict[strings.ToUpper(key)]; ok {
		return true
	}
	return false
}

func ToUInt32(val bool) uint32 {
	if val {
		return 1
	}
	return 0
}

func IsMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
}

func NewPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

func ToKVPSlice(v *map[string]string) []KVP {
	var result []KVP
	for k, v := range *v {
		result = append(result, KVP{
			Key:   k,
			Value: v,
		})
	}
	return result
}

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
}
func SetRedisValueWithExpiration(keyparts []string, value int) {
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("SETEX", strings.Join(keyparts, ":"), redisTransientExpiration, value)
	if err != nil {
		log.Printf("Error setting redis value %v to %v, error %v", strings.Join(keyparts, ":"), value, err)
	}
}

func IncrRedisValue(keyparts []string) { // no need to update expiration
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("INCR", strings.Join(keyparts, ":"))
	if err != nil {
		log.Printf("Error incrementing redis value %v, error %v", strings.Join(keyparts, ":"), err)
	}
}

func SetRedisKeyWithExpiration(keyparts []string) {
	SetRedisValueWithExpiration(keyparts, 1)
}

func GetRedisIntValue(keyparts []string) int {
	ms := msp.Get()
	defer ms.Close()
	value, err := redis.Int(ms.Do("GET", strings.Join(keyparts, ":")))
	if err != nil {
		log.Printf("Error getting redis value %v, error %v", strings.Join(keyparts, ":"), err)
	}
	return value
}

func GetRedisIntValues(keys [][]string) []int {
	ms := msp.Get()
	defer ms.Close()

	formattedKeys := []string{}
	for _, key := range keys {
		formattedKeys = append(formattedKeys, strings.Join(key, ":"))
	}

	values, err := redis.Ints(ms.Do("MGET", formattedKeys))
	if err != nil {
		log.Printf("Error getting redis values %v, error %v", formattedKeys, err)
	}
	return values
}

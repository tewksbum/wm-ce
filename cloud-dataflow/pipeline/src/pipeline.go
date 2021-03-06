package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/daidokoro/beam/sdks/go/pkg/beam/x/debug"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
)

var PubsubTopic = "streamer-output-dev"
var OutputTopic = "pipeline-output-dev"
var ProjectID = "wemade-core"
var BucketData = "wemade-ai-platform"

var PredictionURL = "https://ml.googleapis.com/v1/projects/wemade-core/models/column_prediction_model:predict"

var listLabels map[string]string
var listCities map[string]bool
var listStates map[string]bool
var listCountries map[string]bool
var listFirstNames map[string]bool
var listLastNames map[string]bool

var reEmail = regexp.MustCompile("(?i)^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
var rePhone = regexp.MustCompile(`(?i)^(?:(?:\(?(?:00|\+)([1-4]\d\d|[1-9]\d?)\)?)?[\-\.\ \\\/]?)?((?:\(?\d{1,}\)?[\-\.\ \\\/]?){0,})(?:[\-\.\ \\\/]?(?:#|ext\.?|extension|x)[\-\.\ \\\/]?(\d+))?$`)
var reZipcode = regexp.MustCompile(`(?i)^\d{5}(?:[-\s]\d{4})?$`)
var reStreet1 = regexp.MustCompile(`(?i)\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?`)
var reStreet2 = regexp.MustCompile(`(?i)apartment|apt|unit|box`)

var reCleanupDigitsOnly = regexp.MustCompile("[^a-zA-Z0-9]+")

type InputRecord struct {
	Columns []struct {
		ERR struct {
			Address1        int `json:"Address1"`
			Address2        int `json:"Address2"`
			Age             int `json:"Age"`
			Birthday        int `json:"Birthday"`
			City            int `json:"City"`
			Country         int `json:"Country"`
			County          int `json:"County"`
			Email           int `json:"Email"`
			FirstName       int `json:"FirstName"`
			FullName        int `json:"FullName"`
			Gender          int `json:"Gender"`
			LastName        int `json:"LastName"`
			MiddleName      int `json:"MiddleName"`
			ParentEmail     int `json:"ParentEmail"`
			ParentFirstName int `json:"ParentFirstName"`
			ParentLastName  int `json:"ParentLastName"`
			ParentName      int `json:"ParentName"`
			Phone           int `json:"Phone"`
			State           int `json:"State"`
			Suffix          int `json:"Suffix"`
			ZipCode         int `json:"ZipCode"`
			TrustedID       int `json:"TrustedID"`
			Title           int `json:"Title"`
		} `json:"ERR"`
		NER struct {
			FAC       float64 `json:"FAC"`
			GPE       float64 `json:"GPE"`
			LOC       float64 `json:"LOC"`
			NORP      float64 `json:"NORP"`
			ORG       float64 `json:"ORG"`
			PERSON    float64 `json:"PERSON"`
			PRODUCT   float64 `json:"PRODUCT"`
			EVENT     float64 `json:"EVENT"`
			WORKOFART float64 `json:"WORK_OF_ART"`
			LAW       float64 `json:"LAW"`
			LANGUAGE  float64 `json:"LANGUAGE"`
			DATE      float64 `json:"DATE"`
			TIME      float64 `json:"TIME"`
			PERCENT   float64 `json:"PERCENT"`
			MONEY     float64 `json:"MONEY"`
			QUANTITY  float64 `json:"QUANTITY"`
			ORDINAL   float64 `json:"ORDINAL"`
			CARDINAL  float64 `json:"CARDINAL"`
		} `json:"NER"`
		Name     string `json:"Name"`
		Value    string `json:"Value"`
		MatchKey string `json:"MK"`
	} `json:"Columns"`
	Owner     int    `json:"Owner"`
	Request   string `json:"Request"`
	Row       int    `json:"Row"`
	Source    string `json:"Source"`
	TimeStamp string `json:"TimeStamp"`
}

type Prediction struct {
	Predictions []float64 `json:"predictions"`
}

type IdentifiedRecord struct {
	FNAME   string `json:"fname"`
	LNAME   string `json:"lname"`
	CITY    string `json:"city"`
	STATE   string `json:"statte"`
	ZIP     string `json:"zip"`
	COUNTRY string `json:"counttry"`
	EMAIL   string `json:"email"`
	PHONE   string `json:"phone"`
	AD1     string `json:"ad1"`
	AD2     string `json:"ad2"`
}

type OutputAddress struct {
	Add1                string  `json:"Add1"`
	Add2                string  `json:"Add2"`
	City                string  `json:"City"`
	Country             string  `json:"Country"`
	DMA                 string  `json:"DMA"`
	Directional         string  `json:"Directional"`
	Lat                 float64 `json:"Lat"`
	Long                float64 `json:"Long"`
	MailRoute           string  `json:"MailRoute"`
	Number              string  `json:"Number"`
	OccupancyIdentifier string  `json:"OccupancyIdentifier"`
	OccupancyType       string  `json:"OccupancyType"`
	PostType            string  `json:"PostType"`
	Postal              string  `json:"Postal"`
	State               string  `json:"State"`
	StreetName          string  `json:"StreetName"`
}

type OutputBackground struct {
	Age    int    `json:"Age"`
	DOB    string `json:"DOB"`
	Gender string `json:"Gender"`
	Race   string `json:"Race"`
}

type OutputEmail struct {
	Address   string `json:"Address"`
	Confirmed bool   `json:"Confirmed"`
	Domain    string `json:"Domain"`
	Type      string `json:"Type"`
}

type OutputName struct {
	First      string `json:"First"`
	Full       string `json:"Full"`
	Last       string `json:"Last"`
	Middle     string `json:"Middle"`
	Nick       string `json:"Nick"`
	Salutation string `json:"Salutation"`
	Suffix     string `json:"Suffix"`
}

type OutputOrganization struct {
	Location string `json:"Location"`
	Name     string `json:"Name"`
	Role     string `json:"Role"`
	SIC      string `json:"SIC"`
	Status   string `json:"Status"`
	Title    string `json:"Title"`
}

type OutputPhone struct {
	Area      string `json:"Area"`
	Confirmed bool   `json:"Confirmed"`
	Country   string `json:"Country"`
	Exchange  string `json:"Exchange"`
	Provider  string `json:"Provider"`
	Station   string `json:"Station"`
	Type      string `json:"Type"`
}

type OutputTrustedID struct {
	Source   string `json:"Source"`
	SourceID string `json:"SourceId"`
}

type OutputRecord struct {
	Address      []OutputAddress      `json:"Address"`
	Background   OutputBackground     `json:"Background"`
	Email        []OutputEmail        `json:"Email"`
	Name         OutputName           `json:"Name"`
	Organization []OutputOrganization `json:"Organization"`
	Phone        []OutputPhone        `json:"Phone"`
	TrustedID    []OutputTrustedID    `json:"TrustedId"`
}

type Address struct {
	House           int
	StreetDirection string
	StreetName      string
	StreetType      string
	SuiteType       string
	SuiteNumber     string
}

var StreetTypeAbbreviations map[string]string = map[string]string{
	"alley": "aly",
	"allee": "aly",
	"ally":  "aly",
	"aly":   "aly",

	"anex":  "anx",
	"annex": "anx",
	"annx":  "anx",
	"anx":   "anx",

	"arcade": "arc",
	"arc":    "arc",

	"avenue": "ave",
	"av":     "ave",
	"aven":   "ave",
	"avn":    "ave",
	"anvue":  "ave",
	"ave":    "ave",

	"bayou": "byu",
	"bayoo": "byu",
	"byu":   "byu",

	"beach": "bch",
	"bch":   "bch",

	"bend": "bnd",
	"bnd":  "bnd",

	"bluff": "blf",
	"bluf":  "blf",
	"blf":   "blf",

	"bottom": "btm",
	"bot":    "btm",
	"bottm":  "btm",
	"btm":    "btm",

	"boulevard": "blvd",
	"boulv":     "blvd",
	"boul":      "blvd",
	"blvd":      "blvd",

	"branch": "br",
	"brnch":  "br",
	"br":     "br",

	"bridge": "brg",
	"brdge":  "brg",
	"brg":    "brg",

	"brook": "brk",
	"brk":   "brk",

	"brooks": "brks",
	"brks":   "brks",

	"burg": "bg",
	"bg":   "bg",

	"burgs": "bgs",
	"bgs":   "bgs",

	"bypass": "byp",
	"bypa":   "byp",
	"bypas":  "byp",
	"byps":   "byp",
	"byp":    "byp",

	"camp": "cp",
	"cmp":  "cp",
	"cp":   "cp",

	"canyon": "cyn",
	"canyn":  "cyn",
	"cnyn":   "cyn",
	"cyn":    "cyn",

	"cape": "cpe",
	"cpe":  "cpe",

	"causeway": "cswy",
	"causwa":   "cswy",
	"cswy":     "cswy",

	"center": "ctr",
	"cen":    "ctr",
	"cent":   "ctr",
	"centr":  "ctr",
	"centre": "ctr",
	"cnter":  "ctr",
	"ctr":    "ctr",

	"centers": "ctrs",
	"ctrs":    "ctrs",

	"circle": "cir",
	"circ":   "cir",
	"circl":  "cir",
	"crcl":   "cir",
	"crcle":  "cir",
	"cir":    "cir",

	"cliff": "clf",
	"clf":   "clf",

	"cliffs": "clfs",
	"clfs":   "clfs",

	"club": "clb",
	"clb":  "clb",

	"common": "cmn",
	"cmn":    "cmn",

	"commons": "cmns",
	"cmns":    "cmns",

	"corner": "cor",
	"cor":    "cor",

	"corners": "cors",
	"cors":    "cors",

	"course": "crse",
	"crse":   "crse",

	"court": "ct",
	"ct":    "ct",

	"courts": "cts",
	"cts":    "cts",

	"cove": "cv",
	"cv":   "cv",

	"coves": "cvs",
	"cvs":   "cvs",

	"cr":          "county road",
	"county road": "county road",

	"creek": "crk",
	"crk":   "crk",

	"crescent": "cres",
	"crsent":   "cres",
	"crsnt":    "cres",
	"cres":     "cres",

	"crest": "crst",
	"crst":  "crst",

	"crossing": "xing",
	"crssing":  "xing",
	"xing":     "xing",

	"crossroad": "xrd",
	"xrd":       "xrd",

	"crossroads": "xrds",
	"xrds":       "xrds",

	"curve": "curv",
	"curv":  "curv",

	"dale": "dl",
	"dl":   "dl",

	"dam": "dm",
	"dm":  "dm",

	"divide": "dv",
	"div":    "dv",
	"dvd":    "dv",
	"dv":     "dv",

	"drive": "dr",
	"driv":  "dr",
	"drv":   "dr",
	"dr":    "dr",

	"drives": "drs",
	"drs":    "drs",

	"estate": "est",
	"est":    "est",

	"estates": "ests",
	"ests":    "ests",

	"expressway": "expy",
	"expr":       "expy",
	"express":    "expy",
	"expw":       "expy",
	"expy":       "expy",

	"extension": "ext",
	"extn":      "ext",
	"extnsn":    "ext",
	"ext":       "ext",

	"fall": "fall",

	"falls": "fls",
	"fls":   "fls",

	"ferry": "fry",
	"frry":  "fry",
	"fry":   "fry",

	"field": "fld",
	"fld":   "fld",

	"fields": "flds",
	"flds":   "flds",

	"flat": "flt",
	"flt":  "flt",

	"flats": "flts",
	"flts":  "flts",

	"ford": "frd",
	"frd":  "frd",

	"fords": "frds",
	"frds":  "frds",

	"forest": "frst",
	"frst":   "frst",

	"forge": "frg",
	"forg":  "frg",
	"frg":   "frg",

	"forges": "frgs",
	"frgs":   "frgs",

	"fork": "frk",
	"frk":  "frk",

	"fort": "ft",
	"frt":  "ft",
	"ft":   "ft",

	"freeway": "fwy",
	"freewy":  "fwy",
	"frway":   "fwy",
	"frwy":    "fwy",
	"fwy":     "fwy",

	"garden": "gdn",
	"gardn":  "gdn",
	"grden":  "gdn",
	"grdn":   "gdn",
	"gdn":    "gdn",

	"gardens": "gdns",
	"grdns":   "gdns",
	"gdns":    "gdns",

	"gateway": "gtwy",
	"gatewy":  "gtwy",
	"gatway":  "gtwy",
	"gtway":   "gtwy",
	"gtwy":    "gtwy",

	"glen": "gln",
	"gln":  "gln",

	"glens": "glns",
	"glns":  "glns",

	"green": "grn",
	"grn":   "grn",

	"greens": "grns",
	"grns":   "grns",

	"grove": "grv",
	"grov":  "grv",
	"grv":   "grv",

	"harbor": "hbr",
	"harb":   "hbr",
	"harbr":  "hbr",
	"hrbr":   "hbr",
	"hbr":    "hbr",

	"harbors": "hbrs",
	"hbrs":    "hbrs",

	"haven": "hvn",
	"hvn":   "hvn",

	"heights": "hts",
	"ht":      "hts",
	"hts":     "hts",

	"highway": "hwy",
	"highwy":  "hwy",
	"hiway":   "hwy",
	"hiwy":    "hwy",
	"hway":    "hwy",
	"hwy":     "hwy",

	"hill": "hl",
	"hl":   "hl",

	"hills": "hls",
	"hls":   "hls",

	"hollow":  "holw",
	"hllw":    "holw",
	"hollows": "holw",
	"holws":   "holw",
	"holw":    "holw",

	"hodor": "hodor",

	"inlet": "inlt",
	"inlt":  "inlt",

	"island": "is",
	"islnd":  "is",
	"is":     "is",

	"isle":  "isle",
	"isles": "isle",

	"junction": "jct",
	"jction":   "jct",
	"jctn":     "jct",
	"junctn":   "jct",
	"juncton":  "jct",
	"jct":      "jct",

	"junctions": "jcts",
	"jctns":     "jcts",
	"jcts":      "jcts",

	"key": "ky",
	"ky":  "ky",

	"keys": "kys",
	"kys":  "kys",

	"knoll": "knl",
	"knol":  "knl",
	"knl":   "knl",

	"knolls": "knls",
	"knls":   "knls",

	"lake": "lk",
	"lk":   "lk",

	"lakes": "lks",
	"lks":   "lks",

	"land": "land",

	"landing": "lndg",
	"lndng":   "lndg",
	"lndg":    "lndg",

	"lane": "ln",
	"ln":   "ln",

	"light": "lgt",
	"lgt":   "lgt",

	"lights": "lgts",
	"lgts":   "lgts",

	"loaf": "lf",
	"lf":   "lf",

	"lock": "lck",
	"lck":  "lck",

	"locks": "lcks",
	"lcks":  "lcks",

	"lodge": "ldg",
	"ldge":  "ldg",
	"lodg":  "ldg",
	"ldg":   "ldg",

	"loop":  "loop",
	"loops": "loop",

	"mall": "mall",

	"manor": "mnr",
	"mnr":   "mnr",

	"manors": "mnrs",
	"mnrs":   "mnrs",

	// Accoding to that table...:
	// "meadow": "mdw",
	// "mdw": "mdw",

	// But on the following line...
	"meadows": "mdws",
	"mdw":     "mdws",
	"meadow":  "mdws",
	"medows":  "mdws",
	"mdws":    "mdws",

	"mews": "mews",

	"mill": "ml",
	"ml":   "ml",

	"mission": "msn",
	"misn":    "msn",
	"msn":     "msn",

	"motorway": "mtwy",
	"mtwy":     "mtwy",

	"mountain": "mtn",
	"mntain":   "mtn",
	"mt":       "mtn",
	"mountin":  "mtn",
	"mtin":     "mtn",
	"mtn":      "mtn",

	"mountains": "mtns",
	"mtns":      "mtns",

	"neck": "nck",
	"nck":  "nck",

	"orchard": "orch",
	"orchrd":  "orch",
	"orch":    "orch",

	"oval": "ovl",
	"ovl":  "ovl",

	"overpass": "opas",
	"opas":     "opas",

	"park":  "park",
	"parks": "park",
	"prk":   "park",

	"parkway":  "pkwy",
	"parkwy":   "pkwy",
	"pkway":    "pkwy",
	"pky":      "pkwy",
	"parkways": "pkwy",
	"pkwys":    "pkwy",
	"pkwy":     "pkwy",

	"pass": "pass",

	"passage": "psge",
	"psge":    "psge",

	"path":  "path",
	"paths": "paths",

	"pike":  "pike",
	"pikes": "pike",

	"pine": "pne",
	"pne":  "pne",

	"pines": "pnes",
	"pnes":  "pnes",

	"place": "pl",
	"pl":    "pl",

	"plain": "pln",
	"pln":   "pln",

	"plains": "plns",
	"plns":   "plns",

	"plaza": "plz",
	"plza":  "plz",
	"plz":   "plz",

	"point": "pt",
	"pt":    "pt",

	"points": "pts",
	"pts":    "pts",

	"port": "prt",
	"prt":  "prt",

	"ports": "prts",
	"prts":  "prts",

	"prarie": "pr",
	"prr":    "pr",
	"pr":     "pr",

	"radial": "radl",
	"rad":    "radl",
	"radiel": "radl",
	"radl":   "radl",

	"ramp": "ramp",

	"ranch":   "rnch",
	"ranches": "rnch",
	"rnchs":   "rnch",
	"rnch":    "rnch",

	"rapid": "rpd",
	"rpd":   "rpd",

	"ridge": "rdg",
	"rdge":  "rdg",
	"rdg":   "rdg",

	"rest": "rst",
	"rst":  "rst",

	"ridges": "rdgs",
	"rdgs":   "rdgs",

	"road": "road",
	"rd":   "rd",

	"route": "rte",
	"rte":   "rte",

	"row": "row",

	"rue": "rue",

	"run": "run",

	"shoal": "shl",
	"shl":   "shl",

	"shoals": "shls",
	"shls":   "shls",

	"shore":  "shr",
	"shores": "shr",
	"shrs":   "shr",
	"shr":    "shr",

	"skyway": "skwy",

	"spring": "spg",
	"spng":   "spg",
	"sprng":  "spg",
	"spg":    "spg",

	"springs": "spgs",
	"spngs":   "spgs",
	"sprngs":  "spgs",
	"spgs":    "spgs",

	"spur":  "spur",
	"spurs": "spur",

	"square": "sq",
	"sqr":    "sq",
	"sqre":   "sq",
	"squ":    "sq",
	"sq":     "sq",

	"squares": "sqs",
	"sqrs":    "sqs",
	"sqs":     "sqs",

	"station": "sta",
	"statn":   "sta",
	"stn":     "sta",
	"sta":     "sta",

	"stravenue": "stra",
	"strav":     "stra",
	"straven":   "stra",
	"stravn":    "stra",
	"strvn":     "stra",
	"strvnue":   "stra",
	"stra":      "stra",

	"stream": "strm",
	"streme": "strm",
	"strm":   "strm",

	"street": "st",
	"strt":   "st",
	"str":    "st",
	"st":     "st",

	"streets": "sts",

	"summit": "smt",
	"sumit":  "smt",
	"sumitt": "smt",
	"smt":    "smt",

	"terrace": "ter",
	"terr":    "ter",
	"ter":     "ter",

	"throughway": "trwy",
	"trwy":       "trwy",

	"trace":  "trce",
	"traces": "trce",
	"trce":   "trce",

	"track":  "trak",
	"tracks": "trak",
	"trk":    "trak",
	"trks":   "trak",
	"trak":   "trak",

	"trailer": "trlr",
	"trlrs":   "trlr",
	"trlr":    "trlr",

	"tunnel":  "tunl",
	"tunls":   "tunl",
	"tunnels": "tunl",
	"tunnl":   "tunl",
	"tunl":    "tunl",

	"turnpike": "tpke",
	"trnp":     "tpke",
	"turnpk":   "tpke",
	"tpke":     "tpke",

	"underpass": "upas",
	"upas":      "upas",

	"union": "un",
	"un":    "un",

	"valley": "vly",
	"vally":  "vly",
	"vlly":   "vly",
	"vly":    "vly",

	"valleys": "vlys",
	"vlys":    "vlys",

	"viaduct": "via",
	"vdct":    "via",
	"viadct":  "via",
	"via":     "via",

	"view": "vw",
	"vw":   "vw",

	"views": "vws",
	"vws":   "vws",

	"village":  "vlg",
	"villag":   "vlg",
	"vill":     "vlg",
	"villg":    "vlg",
	"villiage": "vlg",
	"vlg":      "vlg",

	"villages": "vlgs",
	"vlgs":     "vlgs",

	"ville": "vl",
	"vl":    "vl",

	"vista": "vis",
	"vist":  "vis",
	"vst":   "vis",
	"vsta":  "vis",
	"vis":   "vis",

	"walk":  "walk",
	"walks": "walk",

	"wall": "wall",

	"way": "way",
	"wy":  "way",

	"ways": "ways",

	"well": "wl",
	"wl":   "wl",

	"wells": "wls",
	"wls":   "wls",
}

var SuiteTypeAbbreviations map[string]string = map[string]string{
	"apartment": "apt",
	"#":         "apt",
	"apt":       "apt",

	"building": "bldg",
	"bldg":     "bldg",

	"floor": "fl",
	"fl":    "fl",

	"suite": "ste",
	"ste":   "ste",

	"unit": "unit",

	"room": "rm",
	"rm":   "rm",

	"department": "dept",
	"dept":       "dept",

	"box": "box",
}

var CardinalDirectionAbbreviations map[string]string = map[string]string{
	"north": "n",
	"n":     "n",

	"northwest": "nw",
	"nw":        "nw",

	"northeast": "ne",
	"ne":        "ne",

	"south": "s",
	"s":     "s",

	"southwest": "sw",
	"sw":        "sw",

	"southeast": "se",
	"se":        "se",

	"east": "e",
	"e":    "e",

	"west": "w",
	"w":    "w",
}

var RuralBoxes map[string]string = map[string]string{
	"cr":          "county road",
	"county road": "county road",

	"rr": "rr",

	"po box": "po box",
}

var Pairs map[string]string = map[string]string{
	"po":     "box",
	"county": "road",
	// CR is in here because it's dumb, but this is the only street name value that _has_ to be converted.
	// "cr": "cr",
}

var NumberRequired []string = []string{
	"po box",
	"rr",
	"county road",
	"cr",
}

var httpClient *http.Client

const (
	MaxIdleConnections int = 20
	RequestTimeout     int = 5
)

func init() {
	httpClient = createHTTPClient()
}

// createHTTPClient for connection re-use
func createHTTPClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: MaxIdleConnections,
		},
		Timeout: time.Duration(RequestTimeout) * time.Second,
	}

	return client
}

func readLines(ctx context.Context, client *storage.Client, bucket, object string) ([]string, error) {
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(data), "\n"), nil
}

func readJsonArray(ctx context.Context, client *storage.Client, bucket, object string) (map[string]bool, error) {
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

func readJsonMap(ctx context.Context, client *storage.Client, bucket, object string) (map[string]string, error) {
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

func getHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func isMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
}

func removeDiacritics(value string) string {
	t := transform.Chain(norm.NFD, transform.RemoveFunc(isMn), norm.NFC)
	result, _, _ := transform.String(t, value)
	return result
}

func contains(dict map[string]bool, key string) uint32 {
	if _, ok := dict[key]; ok {
		return 1
	}
	return 0
}

func toUInt32(val bool) uint32 {
	if val {
		return 1
	}
	return 0
}

func toZeroIfNotInMap(dict map[string]int, key string) float64 {
	if _, ok := dict[key]; ok {
		return float64(dict[key])
	}
	return 0
}

func getFeatures(value string) []uint32 {
	var val = strings.TrimSpace(value)
	val = removeDiacritics(val)
	var result []uint32
	result[0] = getHash(val)
	result[1] = contains(listFirstNames, val)
	result[2] = contains(listLastNames, val)
	result[3] = toUInt32(reStreet1.MatchString(val))
	result[4] = toUInt32(reStreet2.MatchString(val))
	result[5] = contains(listCities, val)
	result[6] = contains(listStates, val)
	result[7] = toUInt32(reZipcode.MatchString(val))
	result[8] = contains(listCountries, val)
	result[9] = toUInt32(reEmail.MatchString(val))
	result[10] = toUInt32(rePhone.MatchString(val))
	return result
}

func main() {
	ctx := context.Background()

	// If beamx or Go flags are used, flags must be parsed first.
	flag.Parse()

	//read the lists
	sClient, err := storage.NewClient(ctx)
	listLabels, err := readJsonMap(ctx, sClient, BucketData, "data/labels.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/labels.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listLabels), "data/labels.json")
	}

	listCities, err := readJsonArray(ctx, sClient, BucketData, "data/cities.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/cities.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listCities), "data/cities.json")
	}

	listStates, err := readJsonArray(ctx, sClient, BucketData, "data/states.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/states.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listStates), "data/states.json")
	}

	listCountries, err := readJsonArray(ctx, sClient, BucketData, "data/countries.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/countries.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listCountries), "data/countries.json")
	}

	listFirstNames, err := readJsonArray(ctx, sClient, BucketData, "data/first_names.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/first_names.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listFirstNames), "data/first_names.json")
	}

	listLastNames, err := readJsonArray(ctx, sClient, BucketData, "data/last_names.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/last_names.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listLastNames), "data/last_names.json")
	}

	// beam.Init() is an initialization hook that must called on startup. On
	// distributed runners, it is used to intercept control.
	log.Infof(ctx, "Starting pipeline")

	log.Infof(ctx, "input project %v pubsub topic %v", ProjectID, PubsubTopic)
	log.Infof(ctx, "output project %v pubsub topic %v", ProjectID, OutputTopic)
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	buildPipeline(ctx, s)

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}

func getMkField(v *IdentifiedRecord, field string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.String()
}

func setMkField(v *IdentifiedRecord, field string, value string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.SetString(value)
	return value
}

func (a *Address) finalizeStreetAddress(s *[]string) {
	if len(*s) <= 0 {
		return
	}

	for i := 0; i < len(*s); i++ {
		st := strings.Join([]string{
			a.StreetName,
			(*s)[i],
		}, " ")
		a.StreetName = st
	}
}

func del(s *[]string, i int) {
	*s = append((*s)[:(i)], (*s)[(i)+1:]...)
}

func (a *Address) getHouseNumber(s *[]string) (int, error) {
	for i, e := range *s {
		if _, ok := SuiteTypeAbbreviations[e]; ok {
			return 0, fmt.Errorf("Found a suite type before a house number.  There must not be a house number.")
		}
		n, err := strconv.Atoi(e)
		if err != nil {
			continue
		}
		// Found a number!
		del(s, i)
		return n, nil
	}
	return 0, fmt.Errorf("Could not find a suitable house number.")
}

func (a *Address) getStreetDirection(s *[]string) (string, error) {
	for i, e := range *s {
		if n, ok := CardinalDirectionAbbreviations[e]; ok {
			del(s, i)
			return n, nil
		}
	}
	return "", fmt.Errorf("No suitable street direction was found in the address.")
}

func (a *Address) getStreetType(s *[]string) (string, error) {
	var t string
	var m int
	for i, e := range *s {
		if n, ok := StreetTypeAbbreviations[e]; ok {
			// Is there anything proceeding the street type?
			if len(*s) > i+1 {
				if isAbbreviation((*s)[i+1]) == true {
					// Is it "apt", "suite", "s"...?
					// then do nothing
				} else {
					return "", nil
				}
			}
			m = i
			t = n
			// return n, nil
		}
	}
	if t == "" {
		return "", fmt.Errorf("No suitable street type was found in the address.")
	}
	del(s, m)
	return t, nil
}

func isAbbreviation(s string) bool {
	if _, ok := StreetTypeAbbreviations[s]; ok {
		return true
	}
	if _, ok := CardinalDirectionAbbreviations[s]; ok {
		return true
	}
	if _, ok := RuralBoxes[s]; ok {
		return true
	}
	if _, ok := SuiteTypeAbbreviations[s]; ok {
		return true
	}
	return false
}

type queue []int

var last int

func (q *queue) Push(n int) {
	last = n
	*q = append(*q, n)
}

func (q *queue) Pop() (n int, e error) {
	if q.Len() <= 0 {
		e = fmt.Errorf("index out of bounds")
		return
	}

	n = (*q)[0]
	*q = (*q)[1:]
	return
}

func (q *queue) Peek() (n int) {
	n = (*q)[0]
	return
}
func (q *queue) PeekLast() (n int) {
	return last
}
func (q *queue) Len() int {
	return len(*q)
}

func (q *queue) Get() []int {
	return *q
}

func (a *Address) getStreetName(s *[]string) (r string, err error) {
	// The indices to remove at the end of the function.
	var removeQueue queue

	// Find the first non-number, non-abbreviation
	for i, e := range *s {
		_, err := strconv.Atoi(e)
		if err == nil {
			continue
		}

		if isAbbreviation(e) == true {
			continue
		}

		removeQueue.Push(i)
		r = e
		break
	}

	// If nothing was found then try again and grab the first
	// * Cardinal Direction, "PO"/"RR"/etc, or "Suite"/"Apartment"/etc.
	if r == "" {
		for i, e := range *s {
			_, err := strconv.Atoi(e)
			if err == nil {
				continue
			}
			if _, ok := CardinalDirectionAbbreviations[e]; ok {
				r = e
				removeQueue.Push(i)
				break
			}
			if n, ok := RuralBoxes[e]; ok {
				r = n
				removeQueue.Push(i)
				break
			}
			if _, ok := SuiteTypeAbbreviations[e]; ok {
				r = e
				removeQueue.Push(i)
				break
			}
		}
	}

	// Still nothing?
	// Then grab the first street direction or the first street type.
	if r == "" {
		for i, e := range *s {
			_, err := strconv.Atoi(e)
			if err == nil {
				continue
			}

			// Street type
			if _, ok := StreetTypeAbbreviations[e]; ok {
				r = e
				removeQueue.Push(i)
				break
			}

			// Street Direction
			if _, ok := CardinalDirectionAbbreviations[e]; ok {
				r = e
				removeQueue.Push(i)
				break
			}
		}
	}

	if p, ok := Pairs[r]; ok {
		var n string
		// Find our match, make sure it's in here.
		for i, e := range *s {
			if e == p {
				n = p
				removeQueue.Push(i)
			}
		}

		if n == "" {
			return "", fmt.Errorf("Badly formatted address.  Found a value that expected a pair, but the pair does not exist. (Example:  \"PO\" but no \"BOX\", \"County\" but no \"Road\")")
		}

		// You would think you could just join the 2 at this point and call it done, but nope. cr has to become "County road" at some point...
		if b, ok := RuralBoxes[n]; ok {
			r = b
		} else {
			// Join the strings.  rr, 23 becomes rr 23
			st := strings.Join([]string{
				r,
				n,
			}, " ")
			r = st
		}
	}

	if r == "" {
		return "", fmt.Errorf("Could not find a proper street name.")
	}

	// Now check to see if the street name requires a number
	for _, n := range NumberRequired {
		if r != n {
			continue
		}
		// The last index of a removed element (also read:  valid element).
		l := removeQueue.PeekLast()

		// That's an L, not a 1
		_, err = strconv.Atoi((*s)[l+1])

		// It's not a number so we don't want that crap.
		if err != nil {
			return "", fmt.Errorf("This kind of address requires a number in the street name.  Example:  CR 123;  County Road 100")
			continue
		}
		st := strings.Join([]string{
			r,
			(*s)[(l + 1)],
		}, " ")
		r = st
		removeQueue.Push(l + 1)
	}

	for {
		t, err := removeQueue.Pop()
		if err != nil {
			// end of queue
			break
		}
		del(s, t)

		ra := removeQueue.Get()
		for x, y := range ra {
			if y > t {
				ra[x] = y - 1
			}
		}
	}

	return r, nil
}

func (a *Address) getSuite(s *[]string) (suiteType string, suiteNumber string, err error) {
	for i, e := range *s {
		if n, ok := SuiteTypeAbbreviations[e]; ok {
			suiteType = n
			if len(*s) > (i + 1) {
				suiteNumber = (*s)[i+1]
				del(s, i)
				del(s, i)
				return suiteType, suiteNumber, nil
			} else {
				del(s, i)
				return suiteType, "", fmt.Errorf("Suite type was found, but a suite number was not.")
			}
		}
	}
	return "", "", fmt.Errorf("No suitable suite type was found in the address.")
}

func normalizeStreetAddress(s string) (a Address, err error) {
	if err != nil {
		return Address{}, err
	}

	t := strings.Fields(s)

	// Lowercase it all.
	for i := range t {
		t[i] = strings.ToLower(t[i])
	}

	// Strip out miscellaneous characters.
	regIsNumeric, err := regexp.Compile("^(\\d|\\.)+$")
	regMatchInvalid, err := regexp.Compile("[^a-zA-Z\\d\\s:]")

	for i := range t {
		// Match numeric fields and don't replace their decimals.
		if regIsNumeric.MatchString(t[i]) == false {
			// Remove stray periods and whatnot.
			t[i] = regMatchInvalid.ReplaceAllString(t[i], "")
		}
	}
	// Every address has a street name.  Start with that.
	a.StreetName, err = a.getStreetName(&t)
	if err != nil {
		a.finalizeStreetAddress(&t)
		return a, err
	}

	a.House, err = a.getHouseNumber(&t)
	if err != nil {
		a.finalizeStreetAddress(&t)
		return a, nil
	}

	a.StreetType, err = a.getStreetType(&t)

	a.SuiteType, a.SuiteNumber, err = a.getSuite(&t)

	a.StreetDirection, err = a.getStreetDirection(&t)

	a.finalizeStreetAddress(&t)
	return a, nil
}

func isInt(s string) bool {
	for _, c := range s {
		if !unicode.IsDigit(c) {
			return false
		}
	}
	return true
}

func buildPipeline(ctx context.Context, s beam.Scope) {
	// nothing -> PCollection<Painting>
	pubsubOptions := pubsubio.ReadOptions{WithAttributes: true}
	//pubsubOptions := pubsubio.ReadOptions{}
	messages := pubsubio.Read(s, ProjectID, PubsubTopic, &pubsubOptions)

	// process each record
	records := beam.ParDo(s, func(message *pb.PubsubMessage, emit func(string)) {
		var input InputRecord
		err := json.Unmarshal(message.Data, &input)
		if err != nil {
			log.Warnf(ctx, "error decoding json, %v", string(message.Data))
		}
		log.Infof(ctx, "input message %v", string(message.Data))
		var instances [][]float64
		for _, column := range input.Columns {
			var instance []float64
			instance = append(instance, float64(column.ERR.FirstName))
			instance = append(instance, float64(column.ERR.LastName))
			instance = append(instance, float64(column.ERR.MiddleName))
			instance = append(instance, float64(column.ERR.Suffix))
			instance = append(instance, float64(column.ERR.FullName))
			instance = append(instance, float64(column.ERR.Address1))
			instance = append(instance, float64(column.ERR.Address2))
			instance = append(instance, float64(column.ERR.City))
			instance = append(instance, float64(column.ERR.State))
			instance = append(instance, float64(column.ERR.ZipCode))
			instance = append(instance, float64(column.ERR.County))
			instance = append(instance, float64(column.ERR.Country))
			instance = append(instance, float64(column.ERR.Email))
			instance = append(instance, float64(column.ERR.ParentEmail))
			instance = append(instance, float64(column.ERR.Gender))
			instance = append(instance, float64(column.ERR.Phone))
			instance = append(instance, float64(column.ERR.ParentFirstName))
			instance = append(instance, float64(column.ERR.ParentLastName))
			instance = append(instance, float64(column.ERR.Birthday))
			instance = append(instance, float64(column.ERR.Age))
			instance = append(instance, float64(column.ERR.ParentName))
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

			features := getFeatures(column.Value)
			for _, feature := range features {
				instance = append(instance, float64(feature))
			}
			instances = append(instances, instance)
		}
		reqJSON, _ := json.Marshal(instances)
		req, err := http.NewRequest("POST", PredictionURL, bytes.NewBuffer(reqJSON))
		if err != nil {
			log.Fatalf(ctx, "Error Occured. %+v", err)
		}
		req.Header.Set("Content-Type", "application/json")

		response, err := httpClient.Do(req)
		if err != nil && response == nil {
			log.Fatalf(ctx, "Error sending request to API endpoint. %+v", err)
		}
		// Close the connection to reuse it
		defer response.Body.Close()

		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatalf(ctx, "Couldn't parse response body. %+v", err)
		}
		log.Infof(ctx, "Prediction returned %v", body)
		// we now have the prediction
		var prediction Prediction
		err = json.Unmarshal(body, &prediction)
		if err != nil {
			log.Warnf(ctx, "error decoding json, %v", body)
		}

		// assignn columns and build MK output
		var mkOutput IdentifiedRecord
		var trustedID string
		for index, column := range input.Columns {
			predictionValue := prediction.Predictions[index]
			predictionKey := strconv.Itoa(int(predictionValue))
			matchKey := listLabels[predictionKey]
			column.MatchKey = matchKey
			if matchKey != "" {
				// if it does not already have a value
				if getMkField(&mkOutput, matchKey) != "" {
					setMkField(&mkOutput, matchKey, column.Value)
				}
			}
			if column.ERR.TrustedID == 1 {
				trustedID = column.Value
			}
		}
		mkJSON, _ := json.Marshal(mkOutput)
		log.Infof(ctx, "MatchKey Columns %v", string(mkJSON))

		// assemble output
		output := new(OutputRecord)
		if trustedID != "" {
			outputTrustedId := OutputTrustedID{
				Source:   input.Source,
				SourceID: trustedID,
			}
			output.TrustedID = append(output.TrustedID, outputTrustedId)
		}
		if mkOutput.EMAIL != "" && strings.Contains(mkOutput.EMAIL, "@") {
			emailComponents := strings.Split(mkOutput.EMAIL, "@")
			outputEmail := OutputEmail{
				Address:   mkOutput.EMAIL,
				Domain:    emailComponents[1],
				Confirmed: false,
				Type:      "private",
			}
			output.Email = append(output.Email, outputEmail)
		}
		if mkOutput.PHONE != "" {
			mkOutput.PHONE = reCleanupDigitsOnly.ReplaceAllString(mkOutput.PHONE, "")
			if len(mkOutput.PHONE) > 10 {
				outputPhone := OutputPhone{
					Area:      mkOutput.PHONE[0:3],
					Exchange:  mkOutput.PHONE[3:6],
					Station:   mkOutput.PHONE[6:10],
					Confirmed: false,
					Country:   "1",
					Type:      "Unknown",
					Provider:  "Unknown",
				}
				output.Phone = append(output.Phone, outputPhone)
			}
		}
		if mkOutput.AD1 != "" {
			addressInput := mkOutput.AD1 + " " + mkOutput.AD2
			addressParsed, _ := normalizeStreetAddress(addressInput)
			outputAddress := OutputAddress{
				Add1:                mkOutput.AD1,
				Add2:                mkOutput.AD2,
				City:                mkOutput.CITY,
				Country:             mkOutput.COUNTRY,
				DMA:                 "",
				Directional:         addressParsed.StreetDirection,
				Lat:                 0,
				Long:                0,
				MailRoute:           "",
				Number:              strconv.Itoa(addressParsed.House),
				OccupancyIdentifier: addressParsed.SuiteNumber,
				OccupancyType:       addressParsed.SuiteType,
				PostType:            "",
				Postal:              mkOutput.ZIP,
				State:               mkOutput.STATE,
				StreetName:          addressParsed.StreetName,
			}
			output.Address = append(output.Address, outputAddress)
		}
		// TODO: add OutputBackground when the fields are returned by prediction -- Age, DOB, Gender, Race
		if mkOutput.FNAME != "" || mkOutput.LNAME != "" {
			outputName := OutputName{
				First: mkOutput.FNAME,
				Last:  mkOutput.LNAME,
				Full:  strings.TrimSpace(mkOutput.FNAME + " " + mkOutput.LNAME),
				//Middle
				//Nick
				//Salutation
				//Suffix
			}
			output.Name = outputName
		}
		// TODO: organization?

		outputJSON, _ := json.Marshal(output)

		log.Infof(ctx, "output message %v", string(outputJSON))
		emit(string(outputJSON))
	}, messages)

	debug.Print(s, records)
	//log.Infof(ctx, "PCollection size is %v", stats.Count(s, records))

	//pubsubio.Write(s, ProjectID, OutputTopic, records)

}

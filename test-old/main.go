package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"text/template"
)

type NERentry map[string]interface{}
type NER struct {
	Owner  string
	Source string
	Data   map[string][]string
}
type NERcolumns struct {
	ColumnName  string             `json:"ColumnName"`
	NEREntities map[string]float64 `json:"NEREntities"`
}
type NERresponse struct {
	Columns     []NERcolumns `json:"Columns"`
	ElapsedTime float64      `json:"ElapsedTime"`
	Owner       string       `json:"Owner"`
	Source      string       `json:"Source"`
	TimeStamp   string       `json:"TimeStamp"`
}
type PubSubMapping struct {
	Owner   string            `json:"owner"`
	Source  string            `json:"source"`
	Mapping map[string]string `json:"mapping"`
}

func main() {
	fmt.Println("Starting the application...")
	nerTest := false
	if nerTest {

		jsonData := NER{Owner: "OWNER1", Source: "SOURCE1", Data: map[string][]string{"Student_First_Name": {"Sergei", "Hidemi", "Pablo", "Daria", "Mishele", "Zakriya", "Dov Maxwell", "Harmeet", "Oliver", "Joao", "George", "Felita", "Michael", "Jason", "Samuel", "Karen", "David", "Paolo", "Colton", "Ethan"}, "Student_Last_Name": {"Agafonov", "Akaiwa", "Alhach", "Andreeva", "Arciniega", "Bashirhill", "Beck Levine", "Bhatia", "Bibby", "Boiajion", "Bromley", "Chandra", "Cheung", "Chon", "Chong", "Chou", "Cloobeck", "Cocci", "Colyer", "Davey"}, "Student_Email": {"sagafonov@berklee.edu", "hakaiwa@berklee.edu", "palhach@berklee.edu", "dandreeva@berklee.edu", "marciniega@berklee.edu", "zbashirhill@berklee.edu", "dbecklevine@berklee.edu", "hbhatia@berklee.edu", "obibby@berklee.edu", "jboiajion@berklee.edu", "gbromley@berklee.edu", "fchandra@berklee.edu", "mcheung@berklee.edu", "jchon2@berklee.edu", "schong@berklee.edu", "kchou@berklee.edu", "dcloobeck@berklee.edu", "pcocci@berklee.edu", "ccolyer@berklee.edu", "edavey@berklee.edu"}, "Adr_Home_Line_1": {"Dachnii Prospekt 368 Flat 215", "7-8-8-601MIDORIDAI", "Av.4a Oeste 3-95", "Avtozavodskaya 5 167", "Paseo de Los Virreyes 980", "1402 Nilda Ave", "9 Page Street", "Flat F 33rd Floor Block 20", "302-1515 Homer Mews", "Rua Major Freire 260 Apt 102", "13 St Johns Road", "Jelambar Baru Raya Number 53", "Suzhou Industrial Park", "717-302 Mapo-Gu Sangamsan-Ro", "Avenida Pozo Favorito 1862", "No. 220 19F Guanxin Road", "99 Ridgecrest Rd", "Via Xxv Aprile 23A", "Psc 303 Box 45", "St Quintin Avenue22"}, "Adr_Home_Line_2": {"", "", "", "", "Int.", "", "", "South Horizonsap Lei Chau", "", "", "", "B West Jakarta 11460", "Linglong Street 1 Bayside", "1-92SANGAMWORLDCUPPARK", "", "", "", "", "", ""}, "Adr_Home_City": {"Saint-Petersburg", "Kawanishi City", "Cali", "Moscow", "Guadalajara", "Mountain View", "Toronto", "Hong Kong", "Vancouver", "Sao Paulo", "Knutsford", "Jakarta", "China Jiangsu Suzhou", "Seoul", "Asuncion", "Hsinchu", "Stamford", "Arezzo", "Apo Armed Forces Pacific", "London"}, "Adr_Home_State": {"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}, "Adr_Home_Zip": {"198215", "666-0129", "760045", "115280", "45116", "94040", "M6G1J3", "", "V6Z 3E8", "04304-110", "WA16 0DL", "11460", "215021", "3903", "0", "300", "6903", "52100", "96204", "W10 6NU"}, "Adr_Home_Country_": {"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""}, "Resident_Hall_Student": {"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y"}, "Alien_Status": {"NI", "NI", "NI", "NI", "NI", "CI", "NI", "NI", "NI", "NI", "NI", "NI", "NI", "CI", "NI", "CI", "CI", "NI", "CI", "CI"}, "Parent_1": {"Agafonov, Sergei", "Akaiwa, Hidemi", "Alhach, Pablo", "Andreeva, Daria", "Arciniega, Mishele", "Bashirhill, Zakriya", "Beck Levine, Dov Maxwell", "Bhatia, Harmeet", "Bibby, Oliver", "Boiajion, Joao", "Bromley, George", "Chandra, Felita", "Cheung, Michael", "Chon, Jason", "Chong, Samuel", "Chou, Karen", "Cloobeck, David", "Cocci, Paolo", "Colyer, Colton", "Davey, Ethan"}, "Parent_1_Email": {"sergagafonov3@gmail.com", "hidemusiq@gmail.com", "pabloalhach@gmail.com", "darya_andreeva_2013@inbox.ru", "mishele.arciniega@gmail.com", "zakriyabh@gmail.com", "dbl0325@gmail.com", "harmeetbhatia76@gmail.com", "oliver.m.bibby@gmail.com", "joao.pedro.boiajion@gmail.com", "george.bromley1@gmail.com", "felita.kezia@gmail.com", "cheungmichael3@gmail.com", "1400008@dwight.or.kr", "samuelchong1553@hotmail.com", "karenachou0426@gmail.com", "davidcloobeck@gmail.com", "sunnyside146@yahoo.it", "coltoncol87@gmail.com", "ethan.davey@yahoo.com"}, "Parent_2": {"Agafonov, Leonid", "Akaiwa, Akira", "Gomez, Elizabeth", "Andreev, Alexander", "Lopez, Claudia", "Bashir, Shahzad", "Beck, Marcia", "Bhatia, Gurmeet", "Bibby, Andrew", "Boiajion, Marcio", "Bromley, Julien", "Tjen, Hong", "Cheung, Lewis", "Chon, David", "Chong, Yong", "Chou, Ming", "Cloobeck, Melinda", "Cocci, Damiano", "Colyer, Katherine", "Davey, Rachel"}, "Parent_2_Email": {"leon_33@mail.ru", "akachann99@gmail.com", "lichagoh@gmail.com", "ava@fgi.ru", "clauscorreccorre@hotmail.com", "sbashir@protonmail.com", "becklevine@gmail.com", "gbhatia1@gmail.com", "ajbibby@shaw.ca", "mboiajion@digitbrasil.com.br", "jb@trailermen.com", "rustanto07@yahoo.com", "lws_cheung@yahoo.ca", "daddychon@naver.com", "davidchong58@hotmail.com", "mchou01@msn.com", "melinda.d.cloobeck@morganstanley.com", "sunnyside146@yahoo.it", "kat.colyer@gmail.com", "narowlanskydavey@yahoo.com"}}}
		var structResponse NERresponse
		jsonValue, _ := json.Marshal(jsonData)
		response, err := http.Post("https://us-central1-on-campus-marketing.cloudfunctions.net/ner_api-v0r01", "application/json", bytes.NewBuffer(jsonValue))
		if err != nil {
			fmt.Printf("The HTTP request failed with error %s\n", err)
		} else {

			data, _ := ioutil.ReadAll(response.Body)
			json.Unmarshal(data, &structResponse)
		}
		var nerEntry = NERentry{
			"Owner": "meme",
		}
		for _, col := range structResponse.Columns {
			v := reflect.ValueOf(col)
			typeOfS := v.Type()
			for i := 0; i < v.NumField(); i++ {
				switch columnsElement := v.Field(i).Interface().(type) {
				case string:
					nerEntry["columns."+typeOfS.Field(i).Name] = columnsElement
				case map[string]float64:
					for key, value := range columnsElement {
						nerEntry["columns."+typeOfS.Field(i).Name+"."+key] = value
					}
				}
			}

		}
		fmt.Printf("%v", nerEntry)
	}
	templateTest := false
	if templateTest {

		var m PubSubMapping
		m.Owner = "fakeOwner"
		m.Source = "fakeSource"
		var recordKind bytes.Buffer
		dsKindtemplate, err := template.New("abmOwnerSource").Parse("wm-abm-{{.Owner}}-{{.Source}}")
		if err != nil {
			fmt.Printf("%v", err)
		}
		if err := dsKindtemplate.Execute(&recordKind, m); err != nil {
			fmt.Printf("%v", err)
		}
		fmt.Printf("%v", recordKind.String())
	}
	if true {
		type PubSubMapping struct {
			MatchKeys map[string]interface{} `json:"MatchKeys"`
		}
		var psb PubSubMapping
		jsonString := []byte(`{"MatchKeys": {"fname": {"value": "bob","source": "firstname"},"email": [{"value": "me@me.com","source": "email","type": "personal"},{"value": "me2@me2.com","source": "email2","type": "personal2"}]}}`)

		json.Unmarshal(jsonString, &psb)
		// fmt.Printf("%v", psb.MatchKeys)
		for _, MatchKey := range psb.MatchKeys {
			fmt.Println(reflect.TypeOf(MatchKey))
			switch MatchKey.(type) {
			case []interface{}:
				fmt.Println("This is a multiple field matchkey")
				fmt.Println(MatchKey.([]interface{})[0].(map[string]interface{})["source"])
			case map[string]interface{}:
				fmt.Println("This is a single field matchkey")
				fmt.Println(MatchKey.(map[string]interface{})["source"])

			}
		}
	}
	fmt.Println("Terminating the application...")
}

package purgeutil

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestProcessRequest run purge locally
// complete go.testEnvVars in vscode settings before running
// "go.testEnvVars": {
// 	"GOOGLE_APPLICATION_CREDENTIALS": "P:\\work\\wemade\\gcp\\wemade-core-192394ec89ff.json",
// 	"PROJECTID": "wemade-core",
// 	"ENVIRONMENT": "dev",
// 	"CLIENTID": "wemade",
// 	"CLIENTSECRET": "cool_works"
// },

func TestProcessRequestALL(t *testing.T) {
	json := `
	{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "namespace",
		"operation": "delete",
		"targetSelection": "",
		"targetSubSelection": ""
	}`

	req := httptest.NewRequest("POST", "/", strings.NewReader(json))
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	ProcessRequest(rr, req)

	got := rr.Body.String()

	fmt.Println(got)
}

func TestProcessRequest2(t *testing.T) {
	json := `
	{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "[default]",
		"targetSubSelection": ""
	}`

	req := httptest.NewRequest("POST", "/", strings.NewReader(json))
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	ProcessRequest(rr, req)

	got := rr.Body.String()

	fmt.Println(got)
}

func TestProcessRequest3(t *testing.T) {
	json := []string{
		`
		{
			"clientID": "wemade",
			"clientSecret": "cool_works",
			"targetType": "datastore",
			"targetLevel": "kind",
			"operation": "delete",
			"targetSelection": "[default]",
			"targetSubSelection": ""
		}`,
		`
		{
			"clientID": "wemade",
			"clientSecret": "cool_works",
			"targetType": "datastore",
			"targetLevel": "namespace",
			"operation": "delete",
			"targetSelection": "",
			"targetSubSelection": ""
		}`,
	}
	for _, j := range json {
		req := httptest.NewRequest("POST", "/", strings.NewReader(j))
		req.Header.Add("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		ProcessRequest(rr, req)

		got := rr.Body.String()

		fmt.Println(got)
	}
}

func TestProcessRequest4(t *testing.T) {
	json := []string{
		`
		{
			"clientID": "wemade",
			"clientSecret": "cool_works",
			"targetType": "datastore",
			"targetLevel": "kind",
			"operation": "delete",
			"targetSelection": "wemade.profiler",
			"targetSubSelection": ""
		}`,
		`
		{
			"clientID": "wemade",
			"clientSecret": "cool_works",
			"targetType": "datastore",
			"targetLevel": "kind",
			"operation": "delete",
			"targetSelection": "wemade.streamer",
			"targetSubSelection": ""
		}`,
		`
		{
			"clientID": "wemade",
			"clientSecret": "cool_works",
			"targetType": "datastore",
			"targetLevel": "kind",
			"operation": "delete",
			"targetSelection": "wemade.dev.5648883367542784",
			"targetSubSelection": ""
		}`,
		`
		{
			"clientID": "wemade",
			"clientSecret": "cool_works",
			"targetType": "datastore",
			"targetLevel": "kind",
			"operation": "delete",
			"targetSelection": "wemade.streamer-api.dev",
			"targetSubSelection": ""
		}`,
	}
	for _, j := range json {
		req := httptest.NewRequest("POST", "/", strings.NewReader(j))
		req.Header.Add("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		ProcessRequest(rr, req)

		got := rr.Body.String()

		fmt.Println(got)
	}
}

func TestProcessRequest5(t *testing.T) {
	json := []string{
		`
		{
			"clientID": "wemade",
			"clientSecret": "cool_works",
			"targetType": "datastore",
			"targetLevel": "kind",
			"operation": "delete",
			"targetSelection": "wemade-dev",
			"targetSubSelection": "2-Testing"
		}`,
		`
		{
			"clientID": "wemade",
			"clientSecret": "cool_works",
			"targetType": "datastore",
			"targetLevel": "kind",
			"operation": "delete",
			"targetSelection": "wemade-dev",
			"targetSubSelection": "5648073946562560-Admission"
		}`,
		`
		{
			"clientID": "wemade",
			"clientSecret": "cool_works",
			"targetType": "datastore",
			"targetLevel": "kind",
			"operation": "delete",
			"targetSelection": "wemade-dev",
			"targetSubSelection": "5648073946562560-Test"
		}`,
		`
		{
			"clientID": "wemade",
			"clientSecret": "cool_works",
			"targetType": "datastore",
			"targetLevel": "kind",
			"operation": "delete",
			"targetSelection": "wemade-dev",
			"targetSubSelection": "5648073946562560-Testing"
		}`,
	}
	for _, j := range json {
		req := httptest.NewRequest("POST", "/", strings.NewReader(j))
		req.Header.Add("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		ProcessRequest(rr, req)

		got := rr.Body.String()

		fmt.Println(got)
	}
}

func TestProcessRequestXXX(t *testing.T) {
	json := `
	{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "dev-utk-urhc",
		"targetSubSelection": ""
	}`

	req := httptest.NewRequest("POST", "/", strings.NewReader(json))
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	ProcessRequest(rr, req)

	got := rr.Body.String()

	fmt.Println(got)
}

func TestProcessRequestArkRl(t *testing.T) {
	json := `
	{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "count",
		"targetSelection": "dev-acv-cv",
		"targetSubSelection": "people-fiber"
	}`

	req := httptest.NewRequest("POST", "/", strings.NewReader(json))
	req.Header.Add("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	ProcessRequest(rr, req)

	got := rr.Body.String()

	fmt.Println(got)
}

func TestProcessRequestB(t *testing.T) {
	json := []string{
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-abr-sa",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-aam-rlh",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-acv-cv",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-adc-rl",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-agc-cl",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-ala-hrl",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-alb-rha",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-alf-aces",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-alv-rl",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-amu-fheg",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-ang-hrp",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-cmb-rha",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-cco-rha",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-amu-hrl",
		"targetSubSelection": ""
	}`,
		`{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-alm-afe",
		"targetSubSelection": ""
	}`,
	}
	for _, j := range json {
		req := httptest.NewRequest("POST", "/", strings.NewReader(j))
		req.Header.Add("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		ProcessRequest(rr, req)

		got := rr.Body.String()

		fmt.Println(got)
	}
}

func TestProcessRequestC(t *testing.T) {
	json := `{
		"clientID": "wemade",
		"clientSecret": "cool_works",
		"targetType": "datastore",
		"targetLevel": "kind",
		"operation": "delete",
		"targetSelection": "prod-NAMESPACE",
		"targetSubSelection": ""
	}`
	ns := []string{
		"ysu-rha",
		"ylu-yasa",
		"ycp-rlh",
		"xav-rsa",
		"wyc-fheg",
		"wwh-nrhh",
		"wvw-clo",
		"wvu-rha",
		"wum-rha",
		"wtx-rha",
		"wtt-rl",
		"wsy-rha",
		"wst-nsop",
		"wss-rha",
		"wsr-rl",
		"wsn-rl",
		"wsl-drl",
		"wsc-rlh",
		"wrt-cst",
		"wri-rlh",
		"wrf-nrhh",
		"wpl-rha",
		"wpk-rha",
		"wpi-rs",
		"wpc-fheg",
		"wos-rha",
		"wok-usrh",
		"wof-rl",
		"wml-shac",
		"wmd-uh",
		"wlu-rl",
		"wlm-rsa",
		"wlh-bn",
		"wlc-rhac",
		"wlb-bn",
		"wje-rl",
		"wiu-ihc",
		"wit-rlh",
		"wis-hrl",
		"win-nrhh",
		"wic-hrl",
		"whc-rlo",
		"wgb-rhaa",
		"wfu-rsa",
		"wec-rha",
		"wcn-wo",
		"wcm-fheg",
		"wcg-rl",
		"wcc-bn",
		"way-rha",
		"was-bn",
		"wal-rha",
		"wag-re",
		"vuu-rlh",
		"vld-rha",
		"vil-rl",
		"utm-rha",
		"utk-urhc",
		"uti-rha",
		"utc-sac",
		"usu-rha",
		"ust-rl",
		"usr-rha",
		"usp-rl",
		"una-fheg",
		"umu-rl",
		"ums-bn",
		"umr-bt",
		"umn-hrl",
		"uml-rl",
		"ume-sshs",
		"umd-rha",
		"umb-rsa",
		"ulo-rsa",
		"ucn-hfs",
		"ucl-us",
		"uci-rha",
		"ucg-rl",
		"ucf-hrl",
		"ucb-rha",
		"uca-rha",
		"ubr-rlsc",
		"uaz-rha",
		"uas-sl",
		"uak-bn",
		"syp-cs",
		"syo-rso",
		"syi-raab",
		"syg-nrhh",
		"syb-rha",
		"swy-rl",
		"swu-rlc",
		"swc-rla",
		"swb-rld",
		"svs-nrhh",
		"sit-rl",
		"sie-rha",
		"sia-cl",
		"shu-irc",
		"shd-reh",
		"shc-rl",
		"sfs-rha",
		"sfp-rha",
		"psc-rl",
		"prv-rl",
		"prt-rlh",
		"ppu-rhl",
		"ppc-bn",
		"pot-cs",
		"pos-hrl",
		"pls-rl",
		"pkv-fc",
		"pcs-si",
		"nku-rha",
		"nji-rha",
		"niu-rha",
		"nic-bn",
		"nfi-nfui",
		"nds-rha",
		"ndc-drl",
		"ncu-nsfp",
		"nct-rsa",
		"nch-rha",
		"ncg-asb",
		"nat-sd",
		"nas-rps",
		"myp-hrl",
		"mvl-cs",
		"mvc-rha",
		"muw-bn",
		"mtu-rl",
		"mts-pfa",
		"mth-hrl",
		"mtd-rha",
		"msv-dsa",
		"mst-uab",
		"mss-saa",
		"mso-rha",
		"msn-sl",
		"msm-rl",
		"msa-rha",
		"mrv-mcb",
		"mru-hrl",
		"mqu-rha",
		"mor-rha",
		"mid-rha",
		"mic-apr",
		"mhu-rl",
		"mhe-rha",
		"mhd-saa",
		"mer-rl",
		"mdn-rl",
		"mcy-rl",
		"mcm-rl",
		"mad-rsa",
		"maa-rha",
		"lyn-hrl",
		"lyd-raab",
		"lyc-rlo",
		"lwu-uh",
		"lui-rl",
		"lsu-rha",
		"lss-ho",
		"lrh-hrl",
		"lor-rl",
		"lom-rha",
		"lmu-sho",
		"liu-cl",
		"lip-ho",
		"lhu-rha",
		"lhi-rs",
		"lgu-fheg",
		"lfc-rl",
		"lee-rlh",
		"lec-rl",
		"lcn-rl",
		"law-lucc",
		"lat-ckc",
		"las-rsa",
		"laf-rhc",
		"ksc-rho",
		"kcn-rss",
		"jwu-fheg",
		"jwh-fheg",
		"jun-spa",
		"jul-rl",
		"joh-rl",
		"ith-rha",
		"isu-rha",
		"ion-rl",
		"imm-rlh",
		"ilw-rha",
		"ilu-arh",
		"hwc-rlh",
		"hsn-fheg",
		"gsa-uh",
		"grm-clh",
		"gra-rlo",
		"gon-rha",
		"gma-fheg",
		"gld-rlh",
		"glc-reh",
		"gfu-sl",
		"geo-rl",
		"gcp-rl",
		"gcc-mab",
		"gaw-hre",
		"gat-rha",
		"gan-rha",
		"fld-rha",
		"flc-rha",
		"fla-men",
		"fiu-nrhh",
		"fit-cs",
		"fgh-ad",
		"fgc-rha",
		"fer-rlh",
		"fel-rl",
		"fdu-rha",
		"fdm-rl",
		"fdk-ho",
		"fch-hrs",
		"fau-hre",
		"fai-rhcc",
		"evc-ho",
		"etn-nrhh",
		"etc-cs",
		"esu-rl",
		"eru-rsa",
		"era-rha",
		"eou-rl",
		"eok-bk",
		"emu-rha",
		"emi-rha",
		"eme-rl",
		"emc-rha",
		"elm-rl",
		"elc-ic",
		"eku-rha",
		"eiu-rha",
		"ehu-rha",
		"edb-rha",
		"ecu-rha",
		"ecs-bn",
		"ecn-bk",
		"eca-ehc",
		"dvu-rl",
		"duq-bn",
		"dtu-rl",
		"dtc-rl",
		"dst-hrl",
		"dru-rl",
		"drk-rha",
		"dny-rl",
		"dic-dcb",
		"del-hre",
		"dco-rl",
		"dcm-rsa",
		"cyi-rl",
		"cwu-hnsp",
		"cwr-nrhh",
		"cwc-rl",
		"cur-rl",
		"cup-ic",
		"cty-rl",
		"ctw-fheg",
		"ctl-rla",
		"ctc-rel",
		"css-rha",
		"csn-rha",
		"csl-uh",
		"csh-rha",
		"csf-fssh",
		"csc-rha",
		"csa-rha",
		"cru-irhg",
		"crt-rl",
		"crl-rl",
		"cpp-hs",
		"coe-drl",
		"cod-srl",
		"cob-rha",
		"cnw-rha",
		"cnu-cm",
		"cnp-rha",
		"cnm-rlo",
		"cnl-rl",
		"cnd-sa",
		"cms-hlt",
		"cmi-rha",
		"cmd-rl",
		"cmb-rha",
		"cly-ls",
		"clu-rha",
		"clt-rlo",
		"cls-rl",
		"cld-rl",
		"clb-rha",
		"cia-rl",
		"chw-rl",
		"cht-rl",
		"chs-bn",
		"chm-rl",
		"chl-rl",
		"chc-rl",
		"cha-rla",
		"cen-slo",
		"cec-rl",
		"cdh-uh",
		"cct-bn",
		"cco-rha",
		"ccn-saa",
		"ccm-cl",
		"cch-rha",
		"caz-rl",
		"cat-rl",
		"car-rlp",
		"can-sl",
		"cam-rha",
		"caa-ihc",
		"bwu-sl",
		"btc-sab",
		"bsc-rha",
		"bry-fheg",
		"bru-ssb",
		"brn-bsa",
		"brd-rha",
		"bpt-rl",
		"bou-bn",
		"bng-bn",
		"bmc-bmcb",
		"blm-rha",
		"blf-rl",
		"bku-rl",
		"bgs-rsa",
		"bfc-hou",
		"ber-rl",
		"ben-bn",
		"bem-drl",
		"bec-rl",
		"bcm-hrl",
		"bcl-rha",
		"bck-drl",
		"bar-hrl",
		"azw-rha",
		"azu-rha",
		"avo-rl",
		"aum-hrl",
		"aub-ab",
		"atx-urha",
		"atu-rha",
		"asy-rl",
		"asu-rha",
		"ast-harl",
		"asp-hlds",
		"ash-aub",
		"asb-rl",
		"arf-hrl",
		"app-rha",
		"aph-rha",
		"anw-enac",
		"ang-hrp",
		"amu-fheg",
		"alv-rl",
		"alf-aces",
		"alb-rha",
		"ala-hrl",
		"agc-cl",
		"adc-rl",
		"acv-cv",
		"abr-sa",
		"aam-rlh",
	}
	for _, j := range ns {
		req := httptest.NewRequest("POST", "/", strings.NewReader(strings.Replace(json, "NAMESPACE", j, -1)))
		req.Header.Add("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		ProcessRequest(rr, req)

		got := rr.Body.String()

		fmt.Println(got)
	}
}

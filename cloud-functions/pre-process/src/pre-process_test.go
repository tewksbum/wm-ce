package preprocess

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"regexp"
)

// DONT FORGET TO COMMENT OUT THE FATALS ON INIT() ON THE PRE-PROCESS FILE

func TestSignatures(t *testing.T) {
	perr := GetPeopleERR("MAILING STREET")
	j, _ := json.Marshal(perr)
	fmt.Printf("%v", string(j))
}

func TestPreProcess(t *testing.T) {
	type args struct {
		ctx context.Context
		m   PubSubMessage
	}
	v := args{
		m: PubSubMessage{
			Data: []byte(`json`),
		},
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "Test process",
			args:    v,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := PreProcess(tt.args.ctx, tt.args.m); (err != nil) != tt.wantErr {
				t.Errorf("PreProcess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}	
}

func TestPreProcessWithJson(t *testing.T) {
	json := `{"signature":{"ownerId":"rch-rl","source":"Admission","eventId":"a8fa48b0-e729-422e-87f6-544bd3733051","eventType":"UPLOAD","recordId":"15807fcd-59f9-46e4-aa1c-f11d02af98da","rowNum":27},"passthrough":{"ADCODE":"RCH1J1LQQQ0","distCode":"RCH1J1LQQQ01","emailFirst":"false","inputType":"P","masterProgramCode":"RHL","orderByDate":"2021-08-05","portalListID":"2786","salutation":"To The Parent(s) of:","schoolCode":"RCH","schoolColor":"#890709","schoolName":"Rochester University","schoolYear":"2122","seasonUpload":"Regular","sponsorCode":"rch-rl"},"fields":{"Address 1":"7380 East Banes Hollow road s","City":"Scottsburg","First Name":"victoria","Last Name":"lykins","State":"MI","Student Personal Email":"victorialykins1151@gmail.com","ZIP":"47170"},"attributes":{"PermE":"true","PermM":"true","PermS":"true","campaignId":"42367","campaignName":"rch-rl-RHL-2122","classStanding":"NewRes","emailFirst":"false","organization":"RCH","role":"Student","schoolYear":"2122","status":"","uploadType":"P"}}
	`
	re := regexp.MustCompile(`\r?\n`)
	var message PubSubMessage
	message.Data = []byte(re.ReplaceAllString(json, ""))
	PreProcess(context.Background(), message)
}

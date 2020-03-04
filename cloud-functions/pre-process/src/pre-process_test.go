package preprocess

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
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

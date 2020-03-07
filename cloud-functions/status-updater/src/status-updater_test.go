package statusupdater

import (
	"context"
	"testing"
)

func TestStatusUpdater(t *testing.T) {
	type args struct {
		ctx context.Context
		m   PubSubMessage
	}
	v := args{
		m: PubSubMessage{
			Data: []byte(`{
				"eventId": "123123-12312"
			}`),
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
			if err := StatusUpdater(tt.args.ctx, tt.args.m); (err != nil) != tt.wantErr {
				t.Errorf("StatusUpdater() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

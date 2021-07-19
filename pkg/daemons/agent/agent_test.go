package agent

import (
	"testing"
)

func TestAddFeatureGate(t *testing.T) {
	type args struct {
		currentArg string
		featureGate string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Feature gate added to empty arg",
			args: args{
				currentArg: "",
				featureGate: "SupportPodPidsLimit=false",
			},
			want: "SupportPodPidsLimit=false",
		},
		{
			name: "Feature gate added to existing arg",
			args: args{
				currentArg: "SupportPodPidsLimit=false",
				featureGate: "DevicePlugins=false",
			},
			want: "SupportPodPidsLimit=false,DevicePlugins=false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AddFeatureGate(tt.args.currentArg, tt.args.featureGate)
			if  got != tt.want {
				t.Errorf("error, should be " + tt.want + ", but got " + got)
			}
		})
	}
}

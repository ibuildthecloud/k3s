package loadbalancer

import (
	"encoding/json"
	"io/ioutil"

	"github.com/rancher/k3s/pkg/agent/util"
)

func (lb *LoadBalancer) writeConfig() error {
	configOut, err := json.MarshalIndent(lb, "", "  ")
	if err != nil {
		return err
	}
	return util.WriteFile(lb.configFile, string(configOut))
}

func (lb *LoadBalancer) updateConfig() error {
	writeConfig := true
	if configBytes, err := ioutil.ReadFile(lb.configFile); err == nil {
		config := &LoadBalancer{}
		if err := json.Unmarshal(configBytes, config); err == nil {
			if config.ServerURL == lb.ServerURL {
				writeConfig = false
				lb.setServers(config.ServerAddresses)
			}
		}
	}
	if writeConfig {
		if err := lb.writeConfig(); err != nil {
			return err
		}
	}
	return nil
}

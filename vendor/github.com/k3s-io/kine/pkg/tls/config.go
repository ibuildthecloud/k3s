package tls

import (
	"crypto/tls"

	"go.etcd.io/etcd/client/pkg/v3/transport"
)

type Config struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

func (c Config) ClientConfig() (*tls.Config, error) {
	if c.CertFile == "" && c.KeyFile == "" && c.CAFile == "" {
		return nil, nil
	}

	info := &transport.TLSInfo{
		CertFile:      c.CertFile,
		KeyFile:       c.KeyFile,
		TrustedCAFile: c.CAFile,
	}
	tlsConfig, err := info.ClientConfig()
	if err != nil {
		return nil, err
	}

	return tlsConfig, nil
}

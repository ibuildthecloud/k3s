package cluster

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/k3s-io/kine/pkg/client"
	"github.com/rancher/k3s/pkg/bootstrap"
	"github.com/rancher/k3s/pkg/clientaccess"
	"github.com/sirupsen/logrus"
)

// save writes the current ControlRuntimeBootstrap data to the datastore. This contains a complete
// snapshot of the cluster's CA certs and keys, encryption passphrases, etc - encrypted with the join token.
// This is used when bootstrapping a cluster from a managed database or external etcd cluster.
// This is NOT used with embedded etcd, which bootstraps over HTTP.
func (c *Cluster) save(ctx context.Context) error {
	buf := &bytes.Buffer{}
	if err := bootstrap.ReadFromDisk(buf, &c.runtime.ControlRuntimeBootstrap); err != nil {
		return err
	}
	token := c.config.Token
	if token == "" {
		tokenFromFile, err := readTokenFromFile(c.runtime.ServerToken, c.runtime.ServerCA, c.config.DataDir)
		if err != nil {
			return err
		}
		token = tokenFromFile
	}
	normalizedToken, err := normalizeToken(token)
	if err != nil {
		return err
	}

	data, err := encrypt(normalizedToken, buf.Bytes())
	if err != nil {
		return err
	}

	storageClient, err := client.New(c.etcdConfig)
	if err != nil {
		return err
	}

	_, _, err = c.getBootstrapKeyFromStorage(ctx, storageClient, normalizedToken)
	if err != nil {
		return err
	}

	if err := storageClient.Create(ctx, storageKey(normalizedToken), data); err != nil {
		if err.Error() == "key exists" {
			logrus.Warnln("bootstrap key exists; please follow documentation on updating a node after snapshot restore")
			return nil
		} else if strings.Contains(err.Error(), "not supported for learner") {
			logrus.Debug("skipping bootstrap data save on learner")
			return nil
		}
		return err
	}

	return nil
}

// storageBootstrap loads data from the datastore into the ControlRuntimeBootstrap struct.
// The storage key and encryption passphrase are both derived from the join token.
// token is either passed
func (c *Cluster) storageBootstrap(ctx context.Context) error {
	if err := c.startStorage(ctx); err != nil {
		return err
	}

	storageClient, err := client.New(c.etcdConfig)
	if err != nil {
		return err
	}

	token := c.config.Token
	if token == "" {
		tokenFromFile, err := readTokenFromFile(c.runtime.ServerToken, c.runtime.ServerCA, c.config.DataDir)
		if err != nil {
			return err
		}
		if tokenFromFile == "" {
			// at this point this is a fresh start in a non managed environment
			c.saveBootstrap = true
			return nil
		}
		token = tokenFromFile
	}
	normalizedToken, err := normalizeToken(token)
	if err != nil {
		return err
	}

	value, emptyKey, err := c.getBootstrapKeyFromStorage(ctx, storageClient, normalizedToken)
	if err != nil {
		return err
	}
	if value == nil {
		return nil
	}
	if emptyKey {
		normalizedToken = ""
	}
	data, err := decrypt(normalizedToken, value.Data)
	if err != nil {
		return err
	}

	return bootstrap.WriteToDisk(bytes.NewBuffer(data), &c.runtime.ControlRuntimeBootstrap)
}

// getBootstrapKeyFromStorage will list all keys that has prefix /bootstrap and will check for key that is
// hashed with empty string and will check for any key that is hashed by different token than the one
// passed to it, it will return error if it finds a key that is hashed with different token and will return
// value if it finds the key hashed by passed token or empty string
func (c *Cluster) getBootstrapKeyFromStorage(ctx context.Context, storageClient client.Client, token string) (*client.Value, bool, error) {
	emptyStringKey := storageKey("")
	tokenKey := storageKey(token)

	bootstrapList, err := storageClient.List(ctx, "/bootstrap", 0)
	if err != nil {
		return nil, false, err
	}
	if len(bootstrapList) == 0 {
		c.saveBootstrap = true
		return nil, false, nil
	}
	if len(bootstrapList) > 1 {
		return nil, false, errors.New("found multiple bootstrap keys in storage")
	}
	bootstrapKV := bootstrapList[0]
	// checking for empty string bootstrap key
	switch string(bootstrapKV.Key) {
	case emptyStringKey:
		logrus.Warn("bootstrap data encrypted with empty string, deleting and resaving with token")
		c.saveBootstrap = true
		if err := storageClient.Delete(ctx, emptyStringKey, bootstrapKV.Modified); err != nil {
			return nil, false, err
		}
		return &bootstrapKV, true, nil
	case tokenKey:
		return &bootstrapKV, false, nil
	}

	return nil, false, errors.New("bootstrap data already found and encrypted with different token")
}

// readTokenFromFile will attempt to get the token from <data-dir>/token if it the file not found
// in case of fresh installation it will try to use the runtime serverToken saved in memory
// after stripping it from any additional information like the username or cahash, if the file
// found then it will still strip the token from any additional info
func readTokenFromFile(serverToken, certs, dataDir string) (string, error) {
	tokenFile := filepath.Join(dataDir, "token")
	b, err := ioutil.ReadFile(tokenFile)
	if err != nil {
		if os.IsNotExist(err) {
			token, err := clientaccess.FormatToken(serverToken, certs)
			if err != nil {
				return token, err
			}
			return token, nil
		}
		return "", err
	}
	// strip the token from any new line if its read from file
	return string(bytes.TrimRight(b, "\n")), nil
}

// normalizeToken will normalize the token read from file or passed as a cli flag
func normalizeToken(token string) (string, error) {
	_, password, ok := clientaccess.ParseUsernamePassword(token)
	if !ok {
		return password, errors.New("failed to normalize token")
	}
	return password, nil
}

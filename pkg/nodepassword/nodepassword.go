package nodepassword

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rancher/k3s/pkg/authenticator/hash"
	"github.com/rancher/k3s/pkg/passwd"
	"github.com/rancher/k3s/pkg/version"
	coreclient "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	// hasher provides the algorithm for generating and verifying hashes
	hasher = hash.NewSCrypt()
)

func getSecretName(nodeName string) string {
	return strings.ToLower(nodeName + ".node-password." + version.Program)
}

func verifyHash(secretClient coreclient.SecretClient, nodeName, pass string) error {
	name := getSecretName(nodeName)
	secret, err := secretClient.Get(metav1.NamespaceSystem, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if hash, ok := secret.Data["hash"]; ok {
		if err := hasher.VerifyHash(string(hash), pass); err != nil {
			return errors.Wrapf(err, "unable to verify hash for node '%s'", nodeName)
		}
		return nil
	}
	return fmt.Errorf("unable to locate hash data for node secret '%s'", name)
}

// Ensure will verify a node-password secret if it exists, otherwise it will create one
func Ensure(secretClient coreclient.SecretClient, nodeName, pass string) error {
	if err := verifyHash(secretClient, nodeName, pass); !apierrors.IsNotFound(err) {
		return err
	}

	hash, err := hasher.CreateHash(pass)
	if err != nil {
		return errors.Wrapf(err, "unable to create hash for node '%s'", nodeName)
	}

	immutable := true
	_, err = secretClient.Create(&v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getSecretName(nodeName),
			Namespace: metav1.NamespaceSystem,
		},
		Immutable: &immutable,
		Data:      map[string][]byte{"hash": []byte(hash)},
	})
	if apierrors.IsAlreadyExists(err) {
		return verifyHash(secretClient, nodeName, pass)
	}
	return err
}

// Delete will remove a node-password secret
func Delete(secretClient coreclient.SecretClient, nodeName string) error {
	return secretClient.Delete(metav1.NamespaceSystem, getSecretName(nodeName), &metav1.DeleteOptions{})
}

// MigrateFile moves password file entries to secrets
func MigrateFile(secretClient coreclient.SecretClient, nodeClient coreclient.NodeClient, passwordFile string) error {
	_, err := os.Stat(passwordFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	passwd, err := passwd.Read(passwordFile)
	if err != nil {
		return err
	}

	nodeNames := []string{}
	nodeList, _ := nodeClient.List(metav1.ListOptions{})
	if nodeList != nil {
		for _, node := range nodeList.Items {
			nodeNames = append(nodeNames, node.Name)
		}
	}
	if len(nodeNames) == 0 {
		nodeNames = append(nodeNames, passwd.Users()...)
	}

	logrus.Infof("Migrating node password entries from '%s'", passwordFile)
	ensured := int64(0)
	start := time.Now()
	for _, nodeName := range nodeNames {
		if pass, ok := passwd.Pass(nodeName); ok {
			if err := Ensure(secretClient, nodeName, pass); err != nil {
				logrus.Warn(errors.Wrapf(err, "error migrating node password entry for node '%s'", nodeName))
			} else {
				ensured++
			}
		}
	}
	ms := time.Since(start).Milliseconds()
	logrus.Infof("Migrated %d node password entries in %d milliseconds, average %d ms", ensured, ms, ms/ensured)
	return os.Remove(passwordFile)
}

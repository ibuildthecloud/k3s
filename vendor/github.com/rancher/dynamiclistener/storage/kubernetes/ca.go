package kubernetes

import (
	"crypto"
	"crypto/x509"

	"github.com/rancher/dynamiclistener/factory"
	v1controller "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func LoadOrGenCA(secrets v1controller.SecretClient, namespace, name string) (*x509.Certificate, crypto.Signer, error) {
	secret, err := getSecret(secrets, namespace, name)
	if err != nil {
		return nil, nil, err
	}
	return factory.LoadCA(secret.Data[v1.TLSCertKey], secret.Data[v1.TLSPrivateKeyKey])
}

func LoadOrGenClient(secrets v1controller.SecretClient, namespace, name, cn string, ca *x509.Certificate, key crypto.Signer) (*x509.Certificate, crypto.Signer, error) {
	secret, err := getClientSecret(secrets, namespace, name, cn, ca, key)
	if err != nil {
		return nil, nil, err
	}
	return factory.LoadCA(secret.Data[v1.TLSCertKey], secret.Data[v1.TLSPrivateKeyKey])
}

func getClientSecret(secrets v1controller.SecretClient, namespace, name, cn string, caCert *x509.Certificate, caKey crypto.Signer) (*v1.Secret, error) {
	s, err := secrets.Get(namespace, name, metav1.GetOptions{})
	if !errors.IsNotFound(err) {
		return s, err
	}

	return createAndStoreClientCert(secrets, namespace, name, cn, caCert, caKey)
}

func getSecret(secrets v1controller.SecretClient, namespace, name string) (*v1.Secret, error) {
	s, err := secrets.Get(namespace, name, metav1.GetOptions{})
	if !errors.IsNotFound(err) {
		return s, err
	}

	return createAndStore(secrets, namespace, name)
}

func createAndStoreClientCert(secrets v1controller.SecretClient, namespace string, name, cn string, caCert *x509.Certificate, caKey crypto.Signer) (*v1.Secret, error) {
	key, err := factory.NewPrivateKey()
	if err != nil {
		return nil, err
	}

	cert, err := factory.NewSignedClientCert(key, caCert, caKey, cn)
	if err != nil {
		return nil, err
	}

	certPem, keyPem, err := factory.Marshal(cert, key)
	if err != nil {
		return nil, err
	}

	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string][]byte{
			v1.TLSCertKey:       certPem,
			v1.TLSPrivateKeyKey: keyPem,
		},
		Type: v1.SecretTypeTLS,
	}

	return secrets.Create(secret)
}

func createAndStore(secrets v1controller.SecretClient, namespace string, name string) (*v1.Secret, error) {
	ca, cert, err := factory.GenCA()
	if err != nil {
		return nil, err
	}

	certPem, keyPem, err := factory.Marshal(ca, cert)
	if err != nil {
		return nil, err
	}

	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string][]byte{
			v1.TLSCertKey:       certPem,
			v1.TLSPrivateKeyKey: keyPem,
		},
		Type: v1.SecretTypeTLS,
	}

	return secrets.Create(secret)
}

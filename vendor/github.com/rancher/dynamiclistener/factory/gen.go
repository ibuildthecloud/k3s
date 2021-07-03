package factory

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strings"

	"github.com/rancher/dynamiclistener/cert"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
)

const (
	cnPrefix    = "listener.cattle.io/cn-"
	Static      = "listener.cattle.io/static"
	fingerprint = "listener.cattle.io/fingerprint"
)

var (
	cnRegexp = regexp.MustCompile("^([A-Za-z0-9:][-A-Za-z0-9_.:]*)?[A-Za-z0-9:]$")
)

type TLS struct {
	CACert       *x509.Certificate
	CAKey        crypto.Signer
	CN           string
	Organization []string
	FilterCN     func(...string) []string
}

func cns(secret *v1.Secret) (cns []string) {
	if secret == nil {
		return nil
	}
	for k, v := range secret.Annotations {
		if strings.HasPrefix(k, cnPrefix) {
			cns = append(cns, v)
		}
	}
	return
}

func collectCNs(secret *v1.Secret) (domains []string, ips []net.IP, err error) {
	var (
		cns = cns(secret)
	)

	sort.Strings(cns)

	for _, v := range cns {
		ip := net.ParseIP(v)
		if ip == nil {
			domains = append(domains, v)
		} else {
			ips = append(ips, ip)
		}
	}

	return
}

// Merge combines the SAN lists from the target and additional Secrets, and returns a potentially modified Secret,
// along with a bool indicating if the returned Secret has been updated or not. If the two SAN lists alread matched
// and no merging was necessary, but the Secrets' certificate fingerprints differed, the second secret is returned
// and the updated bool is set to true despite neither certificate having actually been modified. This is required
// to support handling certificate renewal within the kubernetes storage provider.
func (t *TLS) Merge(target, additional *v1.Secret) (*v1.Secret, bool, error) {
	secret, updated, err := t.AddCN(target, cns(additional)...)
	if !updated {
		if target.Annotations[fingerprint] != additional.Annotations[fingerprint] {
			secret = additional
			updated = true
		}
	}
	return secret, updated, err
}

// Renew returns a copy of the given certificate that has been re-signed
// to extend the NotAfter date. It is an error to attempt to renew
// a static (user-provided) certificate.
func (t *TLS) Renew(secret *v1.Secret) (*v1.Secret, error) {
	if IsStatic(secret) {
		return secret, cert.ErrStaticCert
	}
	cns := cns(secret)
	secret = secret.DeepCopy()
	secret.Annotations = map[string]string{}
	secret, _, err := t.generateCert(secret, cns...)
	return secret, err
}

// Filter ensures that the CNs are all valid accorting to both internal logic, and any filter callbacks.
// The returned list will contain only approved CN entries.
func (t *TLS) Filter(cn ...string) []string {
	if len(cn) == 0 || t.FilterCN == nil {
		return cn
	}
	return t.FilterCN(cn...)
}

// AddCN attempts to add a list of CN strings to a given Secret, returning the potentially-modified
// Secret along with a bool indicating whether or not it has been updated. The Secret will not be changed
// if it has an attribute indicating that it is static (aka user-provided), or if no new CNs were added.
func (t *TLS) AddCN(secret *v1.Secret, cn ...string) (*v1.Secret, bool, error) {
	cn = t.Filter(cn...)

	if IsStatic(secret) || !NeedsUpdate(0, secret, cn...) {
		return secret, false, nil
	}
	return t.generateCert(secret, cn...)
}

func (t *TLS) generateCert(secret *v1.Secret, cn ...string) (*v1.Secret, bool, error) {
	secret = secret.DeepCopy()
	if secret == nil {
		secret = &v1.Secret{}
	}

	secret = populateCN(secret, cn...)

	privateKey, err := getPrivateKey(secret)
	if err != nil {
		return nil, false, err
	}

	domains, ips, err := collectCNs(secret)
	if err != nil {
		return nil, false, err
	}

	newCert, err := t.newCert(domains, ips, privateKey)
	if err != nil {
		return nil, false, err
	}

	certBytes, keyBytes, err := Marshal(newCert, privateKey)
	if err != nil {
		return nil, false, err
	}

	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	secret.Data[v1.TLSCertKey] = certBytes
	secret.Data[v1.TLSPrivateKeyKey] = keyBytes
	secret.Annotations[fingerprint] = fmt.Sprintf("SHA1=%X", sha1.Sum(newCert.Raw))

	return secret, true, nil
}

func (t *TLS) newCert(domains []string, ips []net.IP, privateKey crypto.Signer) (*x509.Certificate, error) {
	return NewSignedCert(privateKey, t.CACert, t.CAKey, t.CN, t.Organization, domains, ips)
}

func populateCN(secret *v1.Secret, cn ...string) *v1.Secret {
	secret = secret.DeepCopy()
	if secret.Annotations == nil {
		secret.Annotations = map[string]string{}
	}
	for _, cn := range cn {
		if cnRegexp.MatchString(cn) {
			secret.Annotations[cnPrefix+cn] = cn
		} else {
			logrus.Errorf("dropping invalid CN: %s", cn)
		}
	}
	return secret
}

// IsStatic returns true if the Secret has an attribute indicating that it contains
// a static (aka user-provided) certificate, which should not be modified.
func IsStatic(secret *v1.Secret) bool {
	return secret.Annotations[Static] == "true"
}

// NeedsUpdate returns true if any of the CNs are not currently present on the
// secret's Certificate, as recorded in the cnPrefix annotations. It will return
// false if all requested CNs are already present, or if maxSANs is non-zero and has
// been exceeded.
func NeedsUpdate(maxSANs int, secret *v1.Secret, cn ...string) bool {
	if secret == nil {
		return true
	}

	for _, cn := range cn {
		if secret.Annotations[cnPrefix+cn] == "" {
			if maxSANs > 0 && len(cns(secret)) >= maxSANs {
				return false
			}
			return true
		}
	}

	return false
}

func getPrivateKey(secret *v1.Secret) (crypto.Signer, error) {
	keyBytes := secret.Data[v1.TLSPrivateKeyKey]
	if len(keyBytes) == 0 {
		return NewPrivateKey()
	}

	privateKey, err := cert.ParsePrivateKeyPEM(keyBytes)
	if signer, ok := privateKey.(crypto.Signer); ok && err == nil {
		return signer, nil
	}

	return NewPrivateKey()
}

// Marshal returns the given cert and key as byte slices.
func Marshal(x509Cert *x509.Certificate, privateKey crypto.Signer) ([]byte, []byte, error) {
	certBlock := pem.Block{
		Type:  CertificateBlockType,
		Bytes: x509Cert.Raw,
	}

	keyBytes, err := cert.MarshalPrivateKeyToPEM(privateKey)
	if err != nil {
		return nil, nil, err
	}

	return pem.EncodeToMemory(&certBlock), keyBytes, nil
}

// NewPrivateKey returnes a new ECDSA key
func NewPrivateKey() (crypto.Signer, error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

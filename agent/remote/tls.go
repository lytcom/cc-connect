// tls.go — TLS configuration for WSS (WebSocket Secure).
// Supports self-signed certificates for internal network deployment.
package remote

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

// TLSConfig holds paths to TLS certificate and key files.
type TLSConfig struct {
	CertFile string `json:"cert_file"` // Path to server certificate
	KeyFile  string `json:"key_file"`  // Path to server private key
	CAFile   string `json:"ca_file"`   // Path to CA cert (for client verification / pinning)
}

// LoadServerTLS loads TLS config for the WebSocket server.
func LoadServerTLS(cfg TLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// LoadClientTLS loads TLS config for the agent (client side).
// If CAFile is provided, only that CA is trusted (certificate pinning).
func LoadClientTLS(caFile string) (*tls.Config, error) {
	if caFile == "" {
		// Use system CA pool
		return &tls.Config{MinVersion: tls.VersionTLS12}, nil
	}

	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	return &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}, nil
}

// GenerateSelfSignedCert creates a self-signed certificate for development/internal use.
// Returns cert and key PEM bytes.
func GenerateSelfSignedCert(hosts []string, validDays int) (certPEM, keyPEM []byte, err error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	serialNumber, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{Organization: []string{"ad-bot-agent"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Duration(validDays) * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	privDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})

	return certPEM, keyPEM, nil
}

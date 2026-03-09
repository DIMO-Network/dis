//go:build integration

package integration

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func generateCerts() error {
	// Generate CA key and cert
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return err
	}

	// Generate server key and cert
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return err
	}

	// Generate client key and cert — CN = source address
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: testSourceAddress},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		return err
	}

	// Write PEM files
	tlsCACertPath = filepath.Join(tmpDir, "ca.crt")
	tlsServerCertPath = filepath.Join(tmpDir, "server.crt")
	tlsServerKeyPath = filepath.Join(tmpDir, "server.key")
	tlsClientCertPath = filepath.Join(tmpDir, "client.crt")
	tlsClientKeyPath = filepath.Join(tmpDir, "client.key")

	if err := writePEM(tlsCACertPath, "CERTIFICATE", caCertDER); err != nil {
		return err
	}
	if err := writePEM(tlsServerCertPath, "CERTIFICATE", serverCertDER); err != nil {
		return err
	}
	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		return err
	}
	if err := writePEM(tlsServerKeyPath, "EC PRIVATE KEY", serverKeyDER); err != nil {
		return err
	}
	if err := writePEM(tlsClientCertPath, "CERTIFICATE", clientCertDER); err != nil {
		return err
	}
	clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		return err
	}
	if err := writePEM(tlsClientKeyPath, "EC PRIVATE KEY", clientKeyDER); err != nil {
		return err
	}

	// Generate Ruptela client cert — CN = Ruptela source address
	ruptelaClientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	ruptelaClientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(4),
		Subject:      pkix.Name{CommonName: ruptelaSourceAddress},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	ruptelaClientCertDER, err := x509.CreateCertificate(rand.Reader, ruptelaClientTemplate, caCert, &ruptelaClientKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	ruptelaClientCertPath := filepath.Join(tmpDir, "ruptela_client.crt")
	ruptelaClientKeyPath := filepath.Join(tmpDir, "ruptela_client.key")
	if err := writePEM(ruptelaClientCertPath, "CERTIFICATE", ruptelaClientCertDER); err != nil {
		return err
	}
	ruptelaClientKeyDER, err := x509.MarshalECPrivateKey(ruptelaClientKey)
	if err != nil {
		return err
	}
	if err := writePEM(ruptelaClientKeyPath, "EC PRIVATE KEY", ruptelaClientKeyDER); err != nil {
		return err
	}

	// Build TLS configs
	caCertPEM, err := os.ReadFile(tlsCACertPath)
	if err != nil {
		return err
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caCertPEM)

	clientCertTLS, err := tls.LoadX509KeyPair(tlsClientCertPath, tlsClientKeyPath)
	if err != nil {
		return err
	}
	clientTLSConfig = &tls.Config{
		Certificates: []tls.Certificate{clientCertTLS},
		RootCAs:      caPool,
	}

	ruptelaClientCertTLS, err := tls.LoadX509KeyPair(ruptelaClientCertPath, ruptelaClientKeyPath)
	if err != nil {
		return err
	}
	ruptelaTLSConfig = &tls.Config{
		Certificates: []tls.Certificate{ruptelaClientCertTLS},
		RootCAs:      caPool,
	}

	return nil
}

func writePEM(path, blockType string, der []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: blockType, Bytes: der})
}

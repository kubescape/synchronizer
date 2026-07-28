package backend

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/kubescape/synchronizer/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Kafka security protocols. The SASL_ prefix enables SASL and the _SSL suffix enables TLS.
const (
	securityProtocolPlaintext     = "PLAINTEXT"
	securityProtocolSSL           = "SSL"
	securityProtocolSASLPlaintext = "SASL_PLAINTEXT"
	securityProtocolSASLSSL       = "SASL_SSL"

	saslMechanismPlain       = "PLAIN"
	saslMechanismScramSHA256 = "SCRAM-SHA-256"
	saslMechanismScramSHA512 = "SCRAM-SHA-512"
)

func normalizeSecurityProtocol(protocol string) string {
	if protocol == "" {
		return securityProtocolPlaintext
	}
	return strings.ToUpper(protocol)
}

func protocolUsesSASL(protocol string) bool {
	return protocol == securityProtocolSASLPlaintext || protocol == securityProtocolSASLSSL
}

func protocolUsesTLS(protocol string) bool {
	return protocol == securityProtocolSSL || protocol == securityProtocolSASLSSL
}

// validateKafkaSecurity rejects inconsistent security settings before any client
// is constructed, so a misconfiguration fails fast instead of silently connecting
// in the wrong mode. securityProtocol is authoritative; contradicting fields error.
func validateKafkaSecurity(cfg *config.KafkaConfig) error {
	protocol := normalizeSecurityProtocol(cfg.SecurityProtocol)

	switch protocol {
	case securityProtocolPlaintext, securityProtocolSSL, securityProtocolSASLPlaintext, securityProtocolSASLSSL:
	default:
		return fmt.Errorf("kafkaConfig.securityProtocol %q is invalid (want PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL)", cfg.SecurityProtocol)
	}

	usesSASL := protocolUsesSASL(protocol)
	usesTLS := protocolUsesTLS(protocol)

	if usesSASL {
		mechanism := strings.ToUpper(cfg.SASLMechanism)
		switch mechanism {
		case saslMechanismPlain, saslMechanismScramSHA256, saslMechanismScramSHA512:
		default:
			return fmt.Errorf("kafkaConfig.saslMechanism %q is invalid for %s (want PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)", cfg.SASLMechanism, protocol)
		}
		// PLAIN sends the password verbatim, so refuse it without TLS to encrypt the
		// channel. SCRAM never exposes the password and is allowed over plaintext.
		if mechanism == saslMechanismPlain && !usesTLS {
			return fmt.Errorf("kafkaConfig.saslMechanism PLAIN sends the password in cleartext over %s; use SASL_SSL or a SCRAM mechanism", protocol)
		}
		if cfg.SASLUsername == "" {
			return fmt.Errorf("kafkaConfig.saslUsername is required for securityProtocol %s", protocol)
		}
		if cfg.SASLPassword == "" {
			return fmt.Errorf("kafkaConfig.saslPassword is required for securityProtocol %s", protocol)
		}
	} else if cfg.SASLMechanism != "" || cfg.SASLUsername != "" || cfg.SASLPassword != "" {
		return fmt.Errorf("kafkaConfig SASL settings are set but securityProtocol %s does not use SASL (use SASL_PLAINTEXT or SASL_SSL)", protocol)
	}

	// A CA path is meaningless without TLS; reject it rather than ignore it silently.
	if !usesTLS && cfg.TLSCaCertPath != "" {
		return fmt.Errorf("kafkaConfig.tlsCaCertPath is set but securityProtocol %s does not use TLS (use SSL or SASL_SSL)", protocol)
	}

	return nil
}

// kafkaSecurityOptions builds the franz-go options that enable SASL and/or TLS.
// It returns no options for PLAINTEXT, leaving the plaintext path unchanged.
// Validation is enforced here so no client is ever built from an inconsistent
// security config, whichever constructor is used.
func kafkaSecurityOptions(cfg *config.KafkaConfig) ([]kgo.Opt, error) {
	if err := validateKafkaSecurity(cfg); err != nil {
		return nil, err
	}

	protocol := normalizeSecurityProtocol(cfg.SecurityProtocol)
	var opts []kgo.Opt

	if protocolUsesSASL(protocol) {
		mechanism, err := kafkaSASLMechanism(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	if protocolUsesTLS(protocol) {
		tlsConfig, err := kafkaTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	return opts, nil
}

// kafkaSASLMechanism builds the franz-go SASL mechanism from the configured
// credentials. Credentials are captured at startup, so rotating the SASL secret
// requires a pod restart (the standard flow for a mounted Kubernetes Secret).
func kafkaSASLMechanism(cfg *config.KafkaConfig) (sasl.Mechanism, error) {
	switch strings.ToUpper(cfg.SASLMechanism) {
	case saslMechanismPlain:
		return plain.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsMechanism(), nil
	case saslMechanismScramSHA256:
		return scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha256Mechanism(), nil
	case saslMechanismScramSHA512:
		return scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafkaConfig.saslMechanism %q is invalid (want PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)", cfg.SASLMechanism)
	}
}

// kafkaTLSConfig builds a TLS config that verifies the broker against the system
// roots, plus an optional custom CA from tlsCaCertPath. Client auth is via SASL,
// so no client certificate is loaded.
func kafkaTLSConfig(cfg *config.KafkaConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	if cfg.TLSCaCertPath == "" {
		return tlsConfig, nil
	}

	// A custom CA is appended to the system pool so both public and private CAs work.
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	caPEM, err := os.ReadFile(cfg.TLSCaCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read kafkaConfig.tlsCaCertPath %q: %w", cfg.TLSCaCertPath, err)
	}
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("kafkaConfig.tlsCaCertPath %q contains no valid PEM certificates", cfg.TLSCaCertPath)
	}
	tlsConfig.RootCAs = pool

	return tlsConfig, nil
}

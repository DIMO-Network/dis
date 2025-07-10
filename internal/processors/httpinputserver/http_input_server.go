package httpinputserver

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redpanda-data/benthos/v4/public/components/io"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	DIMOCloudEventSource   = "dimo_cloudevent_source"
	ConnectionContent      = "dimo_content_connection"
	AttestationContent     = "dimo_content_attestation"
	tokenExchangeIssuer    = "token_exchange_issuer"
	tokenExchangeKeySetURL = "token_exchange_key_set_url"
)

var ErrInvalidEthAddr = errors.New("ethereum address not set in claim")

var field = service.NewObjectField("jwt",
	service.NewStringField(tokenExchangeIssuer).Description("Specifies issuer url for token exchange service."),
	service.NewStringField(tokenExchangeKeySetURL).Description("Specified the url that provides public keys for JWT signature validation."),
)

var zeroAddress common.Address

func init() {
	io.RegisterCustomHTTPServerInput("dimo_http_connection_server", CertRoutingMiddlewareConstructor, nil)
	io.RegisterCustomHTTPServerInput("dimo_http_attestation_server", AttestationMiddlewareConstructor, field)
}

func CertRoutingMiddlewareConstructor(*service.ParsedConfig) (io.HTTPInputMiddlewareMeta, error) {
	return CertRoutingMiddlewarefunc, nil
}

func CertRoutingMiddlewarefunc(r *http.Request) (map[string]any, error) {
	retMeta := map[string]any{}
	verifiedChains := r.TLS.VerifiedChains
	for _, certChain := range verifiedChains {
		firstCert := certChain[0]
		retMeta[DIMOCloudEventSource] = firstCert.Subject.CommonName
		retMeta[processors.MessageContentKey] = ConnectionContent
	}
	return retMeta, nil
}

func AttestationMiddlewareConstructor(conf *service.ParsedConfig) (io.HTTPInputMiddlewareMeta, error) {
	return attestationMiddleware(conf)
}

func attestationMiddleware(conf *service.ParsedConfig) (func(*http.Request) (map[string]any, error), error) {
	subConf := conf.Namespace("jwt")
	issuer, err := subConf.FieldString(tokenExchangeIssuer)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch token exchange issuer from config: %w", err)
	}

	jwksURI, err := subConf.FieldString(tokenExchangeKeySetURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch token exchange key set url from config: %w", err)
	}

	issuerURL, err := url.Parse(issuer)
	if err != nil {
		return nil, fmt.Errorf("failed to parse issuer URL: %w", err)
	}

	jwksResource, err := keyfunc.NewDefault([]string{jwksURI})
	if err != nil {
		return nil, fmt.Errorf("failed to create a keyfunc.Keyfunc from the server's URL: %w", err)
	}
	parser := jwt.NewParser(
		jwt.WithIssuer(issuerURL.String()),
		jwt.WithValidMethods([]string{"RS256"}),
	)

	return func(r *http.Request) (map[string]any, error) {
		retMeta := map[string]any{}
		authStr := r.Header.Get("Authorization")
		tokenStr := strings.TrimSpace(strings.Replace(authStr, "Bearer ", "", 1))

		var claims Claims
		if _, err := parser.ParseWithClaims(tokenStr, &claims, jwksResource.Keyfunc); err != nil {
			return retMeta, fmt.Errorf("invalid token string: %w", err)
		}

		if claims.EthereumAddress == (zeroAddress) {
			return retMeta, ErrInvalidEthAddr
		}

		retMeta[DIMOCloudEventSource] = claims.EthereumAddress.Hex()
		retMeta[processors.MessageContentKey] = AttestationContent

		return retMeta, nil
	}, nil
}

type Claims struct {
	EmailAddress    *string        `json:"email,omitempty"`
	ProviderID      *string        `json:"provider_id,omitempty"`
	EthereumAddress common.Address `json:"ethereum_address,omitempty"`
	jwt.RegisteredClaims
}

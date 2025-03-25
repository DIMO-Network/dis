package httpinputserver

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/processors/httpinputserver/dex"
	"github.com/auth0/go-jwt-middleware/v2/jwks"
	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redpanda-data/benthos/v4/public/components/io"
	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/protobuf/proto"
)

const (
	DIMOCloudEventSource      = "cloud_event_source"
	ConnectionContent         = "dimo_content_connection"
	AttestationContent        = "dimo_content_attestation"
	tokenExchangeIssuer       = "token_exchange_issuer"
	tokenExchangeJWTKeySetURL = "token_exchange_jwt_kery_set_url"
)

var configSpec = service.NewConfigSpec().
	Summary("handles cloud event inputs to DIS").
	Field(service.NewIntField(tokenExchangeIssuer).Description("dex token issuer")).
	Field(service.NewStringField(tokenExchangeJWTKeySetURL).Description("token exchange jwt key set url"))

func init() {
	io.RegisterCustomHTTPServerInput("dimo_http_server", CertRoutingMiddlewareConstructor, nil)
	io.RegisterCustomHTTPServerInput("dimo_attestation_server", AttestationMiddlewareConstructor, &configSpec)
}

func CertRoutingMiddlewareConstructor(conf *service.ParsedConfig) (func(*http.Request) (map[string]any, error), error) {
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

func AttestationMiddlewareConstructor(conf *service.ParsedConfig) (func(*http.Request) (map[string]any, error), error) {
	issuer, err := conf.FieldString(tokenExchangeIssuer)
	if err != nil {
		return nil, err
	}

	jwksURI, err := conf.FieldString(tokenExchangeJWTKeySetURL)
	if err != nil {
		return nil, err
	}

	issuerURL, err := url.Parse(issuer)
	if err != nil {
		return nil, fmt.Errorf("failed to parse issuer URL: %w", err)
	}

	opts := []any{}
	if jwksURI != "" {
		keysURI, err := url.Parse(jwksURI)
		if err != nil {
			return nil, fmt.Errorf("failed to parse jwksURI: %w", err)
		}
		opts = append(opts, jwks.WithCustomJWKSURI(keysURI))
	}
	provider := jwks.NewCachingProvider(issuerURL, 1*time.Minute, opts...)
	// Set up the validator.
	jwtValidator, err := validator.New(
		provider.KeyFunc,
		validator.RS256,
		issuerURL.String(),
		[]string{"dimo.zone"},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create validator: %w", err)
	}

	return func(r *http.Request) (map[string]any, error) {
		retMeta := map[string]any{}
		authStr := r.Header.Get("Authorization")
		tokenStr := strings.TrimSpace(strings.Replace(authStr, "Bearer ", "", 1))
		tkn, err := jwtValidator.ValidateToken(r.Context(), tokenStr)
		if err != nil {
			return retMeta, fmt.Errorf("invalid token string: %w", err)
		}

		token, ok := tkn.(jwt.Token)
		if !ok {
			return retMeta, fmt.Errorf("unexpted token type")
		}

		subj, err := token.Claims.GetSubject()
		if err != nil {
			return retMeta, fmt.Errorf("failed to get subject from token claims")
		}

		decoded, err := base64.RawURLEncoding.DecodeString(subj)
		if err != nil {
			return retMeta, fmt.Errorf("failed to decode subject")
		}

		var user dex.User
		if err = proto.Unmarshal(decoded, &user); err != nil {
			return retMeta, fmt.Errorf("failed to parse subject")
		}

		if !common.IsHexAddress(subj) {
			return retMeta, fmt.Errorf("subject is not valid hex address")
		}

		retMeta[DIMOCloudEventSource] = common.HexToAddress(subj)
		retMeta[processors.MessageContentKey] = AttestationContent

		// TODO(ae):
		// verify audience

		return retMeta, nil
	}, nil
}

package httpinputserver

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/auth0/go-jwt-middleware/v2/jwks"
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

var field = service.NewObjectField("jwt",
	service.NewStringField(tokenExchangeIssuer).Description("Specifies issuer url for token exchange service."),
	service.NewStringField(tokenExchangeKeySetURL).Description("Specified the url that provides public keys for JWT signature validation."),
)

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
	// issuer, err := subConf.FieldString(tokenExchangeIssuer)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to fetch token exchange issuer from config: %w", err)
	// }

	jwksURI, err := subConf.FieldString(tokenExchangeKeySetURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch token exchange key set url from config: %w", err)
	}

	// issuerURL, err := url.Parse(issuer)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to parse issuer URL: %w", err)
	// }

	opts := []any{}
	if jwksURI != "" {
		keysURI, err := url.Parse(jwksURI)
		if err != nil {
			return nil, fmt.Errorf("failed to parse jwksURI: %w", err)
		}
		opts = append(opts, jwks.WithCustomJWKSURI(keysURI))
	}
	// provider := jwks.NewCachingProvider(issuerURL, 1*time.Minute, opts...)

	// Set up the validator.
	// jwtValidator, err := validator.New(
	// 	provider.KeyFunc,
	// 	validator.RS256,
	// 	issuerURL.String(),
	// 	[]string{"dimo.zone"},
	// )
	if err != nil {
		return nil, fmt.Errorf("failed to create validator: %w", err)
	}

	return func(r *http.Request) (map[string]any, error) {
		retMeta := map[string]any{}
		authStr := r.Header.Get("Authorization")
		tokenStr := strings.TrimSpace(strings.Replace(authStr, "Bearer ", "", 1))

		jwkResource, err := keyfunc.NewDefaultCtx(r.Context(), []string{jwksURI}) // Context is used to end the refresh goroutine.
		if err != nil {
			log.Fatalf("Failed to create a keyfunc.Keyfunc from the server's URL.\nError: %s", err)
		}

		var claims CustomClaims
		if _, err := jwt.ParseWithClaims(tokenStr, &claims, jwkResource.Keyfunc); err != nil {
			return retMeta, fmt.Errorf("invalid token string: %w", err)
		}
		// tkn, err := jwtValidator.ValidateToken(r.Context(), tokenStr)
		// if err != nil {
		// 	return retMeta, fmt.Errorf("invalid token string: %w", err)
		// }

		// token, ok := tkn.(jwt.Token)
		// if !ok {
		// 	return retMeta, fmt.Errorf("unexpected token type %T", tkn)
		// }

		// claims, ok := token.Claims.(jwt.MapClaims)
		// if !ok {
		// 	return retMeta, fmt.Errorf("unexpected claims type %T", token.Claims)
		// }

		// ethAddr, exists := claims["ethereum_address"].(string)
		// if exists {
		// 	return retMeta, errors.New("no ethereum address in token")
		// }

		if !common.IsHexAddress(claims.EthereumAddress.Hex()) {
			return retMeta, errors.New(fmt.Sprintf("subject is not valid hex address: %s", claims.EthereumAddress.Hex()))
		}

		retMeta[DIMOCloudEventSource] = claims.EthereumAddress.Hex()
		retMeta[processors.MessageContentKey] = AttestationContent

		// TODO(ae):
		// verify audience

		return retMeta, nil
	}, nil
}

type CustomClaims struct {
	EmailAddress    *string         `json:"email,omitempty"`
	ProviderID      *string         `json:"provider_id,omitempty"`
	EthereumAddress *common.Address `json:"ethereum_address,omitempty"`
	jwt.RegisteredClaims
}

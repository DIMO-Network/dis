package httpinputserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/auth0/go-jwt-middleware/v2/jwks"
	"github.com/auth0/go-jwt-middleware/v2/validator"
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

	opts := []any{}
	if jwksURI != "" {
		keysURI, err := url.Parse(jwksURI)
		if err != nil {
			return nil, fmt.Errorf("failed to parse jwksURI: %w", err)
		}
		opts = append(opts, jwks.WithCustomJWKSURI(keysURI))
	}
	provider := jwks.NewCachingProvider(issuerURL, 1*time.Minute, opts...)

	jwkResource, err := keyfunc.NewDefaultCtx(context.Background(), []string{jwksURI}) // Context is used to end the refresh goroutine.
	if err != nil {
		log.Fatalf("Failed to create a keyfunc.Keyfunc from the server's URL.\nError: %s", err)
	}

	return func(r *http.Request) (map[string]any, error) {
		retMeta := map[string]any{}
		authStr := r.Header.Get("Authorization")
		tokenStr := strings.TrimSpace(strings.Replace(authStr, "Bearer ", "", 1))
		fmt.Println("TokenStr: ", tokenStr)
		var claims Claims
		if _, err := jwt.ParseWithClaims(tokenStr, &claims, jwkResource.Keyfunc); err != nil {
			return retMeta, fmt.Errorf("invalid token string: %w", err)
		}

		fmt.Println("claims audience: ", claims.Audience)

		// Set up the validator.
		jwtValidator, err := validator.New(
			provider.KeyFunc,
			validator.RS256,
			issuerURL.String(),
			claims.Audience,
			validator.WithCustomClaims(
				func() validator.CustomClaims {
					return &CustomClaims{}
				},
			),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create jwt validator: %w", err)
		}

		vClaims, err := jwtValidator.ValidateToken(r.Context(), tokenStr)
		if err != nil {
			return retMeta, fmt.Errorf("failed to validate token string with validator: %w", err)
		}

		validClaims, ok := vClaims.(*validator.ValidatedClaims)
		if !ok {
			return retMeta, fmt.Errorf("unexpected type for validated claims: %T", vClaims)
		}

		customClaims, ok := validClaims.CustomClaims.(*CustomClaims)
		if !ok {
			return retMeta, fmt.Errorf("unexpected type for custom claims: %T", validClaims.CustomClaims)
		}
		// fmt.Println("Token: ", token)
		// if err := token.CustomClaims.Validate(r.Context()); err != nil {
		// 	return retMeta, fmt.Errorf("failed to validate custom claims: %w", err)
		// }

		// ethAddr := tkn.CustomClaims.GetEthereumAddress

		// custom, ok := token.CustomClaims.(map[string]any{})
		// if !ok {
		// 	return retMeta, fmt.Errorf("unexpected claims type %T", token.Claims)
		// }

		// if !common.IsHexAddress(ethAddr) || zeroAddress == common.HexToAddress(ethAddr) {
		// 	return retMeta, fmt.Errorf("subject is not valid hex address: %s", ethAddr)
		// }

		retMeta[DIMOCloudEventSource] = strings.TrimSpace(customClaims.EthereumAddress.Hex())
		retMeta[processors.MessageContentKey] = AttestationContent

		return retMeta, nil
	}, nil
}

type Claims struct {
	EmailAddress    *string         `json:"email,omitempty"`
	ProviderID      *string         `json:"provider_id,omitempty"`
	EthereumAddress *common.Address `json:"ethereum_address,omitempty"`
	jwt.RegisteredClaims
}

type CustomClaims struct {
	EthereumAddress common.Address `json:"ethereum_address,omitempty"`
}

func (cc *CustomClaims) Validate(ctx context.Context) error {
	fmt.Println("validating")
	addr := common.HexToAddress(strings.TrimSpace(cc.EthereumAddress.Hex()))
	if addr == (zeroAddress) {
		return errors.New("zero address")
	}

	if common.IsHexAddress(addr.Hex()) {
		return errors.New("not valid hex address")
	}

	return nil
}

func (cc *CustomClaims) GetEthereumAddress() common.Address {
	return cc.EthereumAddress
}

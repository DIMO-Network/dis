package httpinputserver

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttestationMiddleware(t *testing.T) {
	authServer := setupMockAuthServer(t)
	defer authServer.Close()

	// Create test configuration
	config := service.NewConfigSpec()
	config.Field(service.NewObjectField("jwt",
		service.NewStringField("token_exchange_issuer").Description("Specifies issuer url for token exchange service."),
		service.NewStringField("token_exchange_key_set_url").Description("Specified the url that provides public keys for JWT signature validation."),
	))

	parsedConfig, err := config.ParseYAML(fmt.Sprintf(`
jwt:
  token_exchange_issuer: "%s"
  token_exchange_key_set_url: "%s/keys"
`, authServer.issuerURL, authServer.issuerURL), nil)
	require.NoError(t, err)

	// Create middleware
	middleware, err := attestationMiddleware(parsedConfig)
	require.NoError(t, err)

	tests := []struct {
		name          string
		setupRequest  func() *http.Request
		expectedMeta  map[string]any
		expectedError bool
		errorIs       error
	}{

		{
			name: "valid token with custom claims",
			setupRequest: func() *http.Request {
				ethereumAddr := common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd")
				token := authServer.createToken(t, ethereumAddr, map[string]any{
					"email":       "test@example.com",
					"provider_id": "test-provider",
				})

				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer "+token)
				return req
			},
			expectedMeta: map[string]any{
				DIMOCloudEventSource:         common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd").Hex(),
				processors.MessageContentKey: AttestationContent,
			},
			expectedError: false,
		},
		{
			name: "missing authorization header",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("POST", "/", nil)
				return req
			},
			expectedError: true,
			errorIs:       jwt.ErrTokenMalformed,
		},
		{
			name: "invalid bearer format",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "InvalidFormat token")
				return req
			},
			expectedError: true,
			errorIs:       jwt.ErrTokenMalformed,
		},
		{
			name: "empty bearer token",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer ")
				return req
			},
			expectedError: true,
			errorIs:       jwt.ErrTokenMalformed,
		},
		{
			name: "token without ethereum address",
			setupRequest: func() *http.Request {
				// Create token without ethereum_address claim
				now := time.Now()
				claims := jwt.MapClaims{
					"iss": authServer.issuerURL,
					"sub": "test-subject",
					"aud": []string{"dimo.zone"},
					"exp": now.Add(time.Hour).Unix(),
					"iat": now.Unix(),
				}

				token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
				token.Header["kid"] = authServer.keyID
				tokenString, err := token.SignedString(authServer.privateKey)
				require.NoError(t, err)

				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer "+tokenString)
				return req
			},
			expectedError: true,
			errorIs:       ErrInvalidEthAddr,
		},
		{
			name: "token with zero ethereum address",
			setupRequest: func() *http.Request {
				zeroAddr := common.Address{}
				token := authServer.createToken(t, zeroAddr, nil)

				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer "+token)
				return req
			},
			expectedError: true,
			errorIs:       ErrInvalidEthAddr,
		},
		{
			name: "expired token",
			setupRequest: func() *http.Request {
				ethereumAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
				now := time.Now()
				claims := jwt.MapClaims{
					"iss":              authServer.issuerURL,
					"sub":              ethereumAddr.Hex(),
					"aud":              []string{"dimo.zone"},
					"exp":              now.Add(-time.Hour).Unix(), // Expired
					"iat":              now.Add(-2 * time.Hour).Unix(),
					"ethereum_address": ethereumAddr.Hex(),
				}

				token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
				token.Header["kid"] = authServer.keyID
				tokenString, err := token.SignedString(authServer.privateKey)
				require.NoError(t, err)

				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer "+tokenString)
				return req
			},
			expectedError: true,
			errorIs:       jwt.ErrTokenExpired,
		},
		{
			name: "token with wrong issuer",
			setupRequest: func() *http.Request {
				ethereumAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
				now := time.Now()
				claims := jwt.MapClaims{
					"iss":              "https://wrong-issuer.com", // Wrong issuer
					"sub":              ethereumAddr.Hex(),
					"aud":              []string{"dimo.zone"},
					"exp":              now.Add(time.Hour).Unix(),
					"iat":              now.Unix(),
					"ethereum_address": ethereumAddr.Hex(),
				}

				token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
				token.Header["kid"] = authServer.keyID
				tokenString, err := token.SignedString(authServer.privateKey)
				require.NoError(t, err)

				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer "+tokenString)
				return req
			},
			expectedError: true,
			errorIs:       jwt.ErrTokenInvalidIssuer,
		},
		{
			name: "malformed token",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("POST", "/", nil)
				req.Header.Set("Authorization", "Bearer invalid.token.here")
				return req
			},
			expectedError: true,
			errorIs:       jwt.ErrTokenMalformed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.setupRequest()

			meta, err := middleware(req)

			if tt.expectedError {
				require.Error(t, err)
				if tt.errorIs != nil {
					assert.ErrorIs(t, err, tt.errorIs)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, meta)

				// Verify expected metadata
				for key, expectedValue := range tt.expectedMeta {
					actualValue, exists := meta[key]
					assert.True(t, exists, "metadata key %s not found", key)
					assert.Equal(t, expectedValue, actualValue, "unexpected value for metadata key %s", key)
				}
			}
		})
	}
}

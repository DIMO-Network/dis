//go:build integration

package integration

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-jwt/jwt/v5"
)

func startJWKSServer() {
	var err error
	jwtPrivateKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(fmt.Sprintf("failed to generate JWT key: %v", err))
	}
	jwtKeyID = fmt.Sprintf("test-key-%d", time.Now().Unix())

	jwksServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/keys" {
			http.NotFound(w, r)
			return
		}
		jwk := map[string]any{
			"kty": "RSA",
			"kid": jwtKeyID,
			"alg": "RS256",
			"use": "sig",
			"n":   base64.RawURLEncoding.EncodeToString(jwtPrivateKey.N.Bytes()),
			"e":   "AQAB",
		}
		jwks := map[string]any{"keys": []map[string]any{jwk}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jwks)
	}))
	jwtIssuerURL = jwksServer.URL
}

func createJWT(ethereumAddress common.Address) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"iss":              jwtIssuerURL,
		"sub":              ethereumAddress.Hex(),
		"aud":              []string{"dimo.zone"},
		"exp":              now.Add(time.Hour).Unix(),
		"iat":              now.Unix(),
		"ethereum_address": ethereumAddress.Hex(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = jwtKeyID
	return token.SignedString(jwtPrivateKey)
}

func startRPCMock() {
	rpcServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			ID     int    `json:"id"`
		}
		json.NewDecoder(r.Body).Decode(&req)

		var result any
		switch req.Method {
		case "net_version":
			result = "137"
		case "eth_chainId":
			result = "0x89" // 137 in hex
		default:
			result = "0x"
		}

		resp := map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  result,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
}

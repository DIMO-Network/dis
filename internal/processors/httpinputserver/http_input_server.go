package httpinputserver

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/DIMO-Network/dis/internal/processors/httpinputserver/dex"
	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redpanda-data/benthos/v4/public/components/io"
	"google.golang.org/protobuf/proto"
)

// TODO(ae):
const DIMOCloudEventSource = "cloud_event_source"
const DIMOConnectionIdKey = "dimo_connection_source"
const DIMOAttestationIdKey = "dimo_attestation_source"

func init() {
	io.RegisterCustomHTTPServerInput("dimo_http_server", CertRoutingMiddlewarefunc)
	io.RegisterCustomHTTPServerInput("dimo_attestation_server", AttestationMiddlewarefunc)
}

func CertRoutingMiddlewarefunc(r *http.Request) (map[string]any, error) {
	retMeta := map[string]any{}
	verifiedChains := r.TLS.VerifiedChains
	for _, certChain := range verifiedChains {
		firstCert := certChain[0]
		retMeta[DIMOConnectionIdKey] = firstCert.Subject.CommonName
	}
	return retMeta, nil
}

func AttestationMiddlewarefunc(r *http.Request) (map[string]any, error) {
	retMeta := map[string]any{}
	userStr := r.Header.Get("user")
	var token jwt.Token
	if err := json.Unmarshal([]byte(userStr), &token); err != nil {
		return retMeta, fmt.Errorf("failed to pull token from request header: %w", err)
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

	retMeta[DIMOAttestationIdKey] = common.HexToAddress(subj)

	return retMeta, nil
}

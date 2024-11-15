package httpinputserver

import (
	"net/http"

	"github.com/redpanda-data/benthos/v4/public/components/io"
)

const DIMOConnectionIdKey = "dimo_cloudevent_source"

func init() {
	io.RegisterCustomHTTPServerInput("dimo_http_server", CertRoutingMiddlewarefunc)
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

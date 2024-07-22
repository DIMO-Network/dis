package dimovss

import (
	"context"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss/convert"
	"github.com/benthosdev/benthos/v4/public/service"
	"golang.org/x/time/rate"
)

type LimitedTokenGetter struct {
	tokenGetter convert.TokenIDGetter
	limiters    map[string]*rate.Sometimes
	logger      *service.Logger
}

func NewLimitedTokenGetter(tokenGetter convert.TokenIDGetter, logger *service.Logger) *LimitedTokenGetter {
	return &LimitedTokenGetter{
		tokenGetter: tokenGetter,
		limiters:    map[string]*rate.Sometimes{},
		logger:      logger,
	}
}

// TokenIDFromSubject wraps a provided TokenIDGetter and logs successful queries once per day per userDevice.
func (l *LimitedTokenGetter) TokenIDFromSubject(ctx context.Context, userDeviceID string) (uint32, error) {
	tokenID, err := l.tokenGetter.TokenIDFromSubject(ctx, userDeviceID)
	if err != nil {
		return 0, err
	}
	limiter, ok := l.limiters[userDeviceID]
	if !ok {
		// limit to one log per day.
		limiter = &rate.Sometimes{
			Interval: time.Hour * 24,
			First:    1,
		}
		l.limiters[userDeviceID] = limiter
	}
	limiter.Do(func() {
		l.logger.Infof("Found token id '%d' for userDevice '%s'", tokenID, userDeviceID)
	})
	return tokenID, nil
}

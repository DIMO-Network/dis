package tokengetter

import (
	"context"
	"sync"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/nativestatus"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/time/rate"
)

type LimitedTokenGetter struct {
	tokenGetter nativestatus.TokenIDGetter
	limiters    map[string]*rate.Sometimes
	mapMutex    sync.RWMutex
	logger      *service.Logger
}

func NewLimitedTokenGetter(tokenGetter nativestatus.TokenIDGetter, logger *service.Logger) *LimitedTokenGetter {
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
	l.mapMutex.RLock()
	limiter, ok := l.limiters[userDeviceID]
	l.mapMutex.RUnlock()
	if !ok {
		// limit to one log per day.
		limiter = &rate.Sometimes{
			Interval: time.Hour * 24,
			First:    1,
		}
		l.mapMutex.Lock()
		l.limiters[userDeviceID] = limiter
		l.mapMutex.Unlock()
	}
	limiter.Do(func() {
		l.logger.Infof("Found token id '%d' for userDevice '%s'", tokenID, userDeviceID)
	})
	return tokenID, nil
}

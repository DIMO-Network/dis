package ratedlogger

import (
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/time/rate"
)

// Logger is a logger that limits the rate of log messages.
type Logger struct {
	limiter rate.Sometimes
	rate    time.Duration
	logger  *service.Logger
}

// New creates a new RatedLogger with a given logger and log rate.
func New(logger *service.Logger, logRate time.Duration) *Logger {
	return &Logger{
		limiter: rate.Sometimes{
			Interval: logRate,
			First:    1,
		},
		logger: logger,
		rate:   logRate,
	}
}

// Tracef logs a trace message using fmt.Sprintf when args are specified.
func (l *Logger) Tracef(template string, args ...any) {
	if l == nil {
		return
	}
	l.limiter.Do(func() {
		l.logger.Tracef(template, args...)
	})
}

// Trace logs a trace message.
func (l *Logger) Trace(message string) {
	if l == nil {
		return
	}
	l.limiter.Do(func() {
		l.logger.Trace(message)
	})
}

// Debugf logs a debug message using fmt.Sprintf when args are specified.
func (l *Logger) Debugf(template string, args ...any) {
	if l == nil {
		return
	}
	l.limiter.Do(func() {
		l.logger.Debugf(template, args...)
	})
}

// Debug logs a debug message.
func (l *Logger) Debug(message string) {
	if l == nil {
		return
	}
	l.limiter.Do(func() {
		l.logger.Debug(message)
	})
}

// Infof logs an info message using fmt.Sprintf when args are specified.
func (l *Logger) Infof(template string, args ...any) {
	if l == nil {
		return
	}
	l.limiter.Do(func() {
		l.logger.Infof(template, args...)
	})
}

// Info logs an info message.
func (l *Logger) Info(message string) {
	if l == nil {
		return
	}
	l.limiter.Do(func() {
		l.logger.Info(message)
	})
}

// Warnf logs a warning message using fmt.Sprintf when args are specified.
func (l *Logger) Warnf(template string, args ...any) {
	if l == nil {
		return
	}
	l.limiter.Do(func() {
		l.logger.Warnf(template, args...)
	})
}

// Warn logs a warning message.
func (l *Logger) Warn(message string) {
	if l == nil {
		return
	}
	l.limiter.Do(func() {
		l.logger.Warn(message)
	})
}

// Errorf logs an error message using fmt.Sprintf when args are specified.
func (l *Logger) Errorf(template string, args ...any) {
	if l == nil {
		return
	}
	l.limiter.Do(func() {
		l.logger.Errorf(template, args...)
	})
}

// Error logs an error message.
func (l *Logger) Error(message string) {
	if l == nil {
		return
	}
	l.limiter.Do(func() {
		l.logger.Error(message)
	})
}

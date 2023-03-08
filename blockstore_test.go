package main

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-libipfs/gateway"
	"github.com/stretchr/testify/assert"
)

type testErr struct {
	message    string
	retryAfter time.Duration
}

func (e *testErr) Error() string {
	return e.message
}

func (e *testErr) RetryAfter() time.Duration {
	return e.retryAfter
}

func TestGatewayErrorRetryAfter(t *testing.T) {
	originalErr := &testErr{message: "test", retryAfter: time.Minute}
	var (
		convertedErr error
		gatewayErr   *gateway.ErrorRetryAfter
	)

	// Test unwrapped
	convertedErr = gatewayError(originalErr)
	ok := errors.As(convertedErr, &gatewayErr)
	assert.True(t, ok)
	assert.EqualValues(t, originalErr.retryAfter, gatewayErr.RetryAfter)

	// Test wrapped.
	convertedErr = gatewayError(fmt.Errorf("wrapped error: %w", originalErr))
	ok = errors.As(convertedErr, &gatewayErr)
	assert.True(t, ok)
	assert.EqualValues(t, originalErr.retryAfter, gatewayErr.RetryAfter)
}

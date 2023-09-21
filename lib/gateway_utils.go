package lib

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/boxo/gateway"
)

// contentPathToCarUrl returns an URL that allows retrieval of specified resource
// from a trustless gateway that implements IPIP-402
func contentPathToCarUrl(path gateway.ImmutablePath, params gateway.CarParams) *url.URL {
	return &url.URL{
		Path:     path.String(),
		RawQuery: carParamsToString(params),
	}
}

// carParamsToString converts CarParams to URL parameters compatible with IPIP-402
func carParamsToString(params gateway.CarParams) string {
	paramsBuilder := strings.Builder{}
	paramsBuilder.WriteString("format=car") // always send explicit format in URL, this  makes debugging easier, even when Accept header was set
	if params.Scope != "" {
		paramsBuilder.WriteString("&dag-scope=")
		paramsBuilder.WriteString(string(params.Scope))
	}
	if params.Range != nil {
		paramsBuilder.WriteString("&entity-bytes=")
		paramsBuilder.WriteString(strconv.FormatInt(params.Range.From, 10))
		paramsBuilder.WriteString(":")
		if params.Range.To != nil {
			paramsBuilder.WriteString(strconv.FormatInt(*params.Range.To, 10))
		} else {
			paramsBuilder.WriteString("*")
		}
	}
	return paramsBuilder.String()
}

// GatewayError translates underlying blockstore error into one that gateway code will return as HTTP 502 or 504
// it also makes sure Retry-After hint from remote blockstore will be passed to HTTP client, if present.
func GatewayError(err error) error {
	if errors.Is(err, &gateway.ErrorStatusCode{}) ||
		errors.Is(err, &gateway.ErrorRetryAfter{}) {
		// already correct error
		return err
	}

	// All timeouts should produce 504 Gateway Timeout
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, caboose.ErrTimeout) ||
		// Unfortunately this is not an exported type so we have to check for the content.
		strings.Contains(err.Error(), "Client.Timeout exceeded") {
		return fmt.Errorf("%w: %s", gateway.ErrGatewayTimeout, err.Error())
	}

	// (Saturn) errors that support the RetryAfter interface need to be converted
	// to the correct gateway error, such that the HTTP header is set.
	for v := err; v != nil; v = errors.Unwrap(v) {
		if r, ok := v.(interface{ RetryAfter() time.Duration }); ok {
			return gateway.NewErrorRetryAfter(err, r.RetryAfter())
		}
	}

	// everything else returns 502 Bad Gateway
	return fmt.Errorf("%w: %s", gateway.ErrBadGateway, err.Error())
}

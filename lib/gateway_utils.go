package lib

import (
	"net/url"
	"strconv"
	"strings"

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

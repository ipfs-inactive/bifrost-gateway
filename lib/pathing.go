package lib

import (
	"fmt"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/go-cid"
	"strings"
)

func ImmutablePathToRootCidWithRemainder(p gateway.ImmutablePath) (cid.Cid, []string, error) {
	pathSegs := strings.Split(p.String(), "/")
	if len(pathSegs) < 3 || !(pathSegs[0] == "" && pathSegs[1] == "ipfs") {
		return cid.Undef, nil, fmt.Errorf("invalid path")
	}
	pathSegs = pathSegs[2:]
	rootCidStr := pathSegs[0]
	rootCid, err := cid.Decode(rootCidStr)
	if err != nil {
		return cid.Undef, nil, err
	}

	return rootCid, pathSegs[1:], nil
}

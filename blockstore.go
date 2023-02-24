package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	blocks "github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/gateway"
	"go.uber.org/zap/zapcore"
)

var errNotImplemented = errors.New("not implemented")

const GetBlockTimeout = time.Second * 60

func newExchange(bs blockstore.Blockstore) (exchange.Interface, error) {
	return &exchangeBsWrapper{bstore: bs}, nil
}

type exchangeBsWrapper struct {
	bstore blockstore.Blockstore
}

func (e *exchangeBsWrapper) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, cancel := context.WithTimeout(ctx, GetBlockTimeout)
	defer cancel()

	if goLog.Level().Enabled(zapcore.DebugLevel) {
		goLog.Debugw("block requested from remote blockstore", "cid", c.String())
	}

	blk, err := e.bstore.Get(ctx, c)
	if err != nil {
		return nil, gatewayError(err)
	}
	return blk, nil
}

func (e *exchangeBsWrapper) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	out := make(chan blocks.Block)

	go func() {
		defer close(out)
		for _, c := range cids {
			blk, err := e.GetBlock(ctx, c)
			if err != nil {
				return
			}
			out <- blk
		}
	}()
	return out, nil
}

func (e *exchangeBsWrapper) NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error {
	return nil
}

func (e *exchangeBsWrapper) Close() error {
	return nil
}

// gatewayError translates underlying blockstore error into one that gateway code will return as HTTP 502 or 504
func gatewayError(err error) error {
	if errors.Is(err, gateway.ErrGatewayTimeout) || errors.Is(err, gateway.ErrBadGateway) {
		// already correct error
		return err
	}

	// All timeouts should produce 504 Gateway Timeout
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, caboose.ErrSaturnTimeout) ||
		// Unfortunately this is not an exported type so we have to check for the content.
		strings.Contains(err.Error(), "Client.Timeout exceeded") {
		return fmt.Errorf("%w: %s", gateway.ErrGatewayTimeout, err.Error())
	}

	// everything else returns 502 Bad Gateway
	return fmt.Errorf("%w: %s", gateway.ErrBadGateway, err.Error())
}

var _ exchange.Interface = (*exchangeBsWrapper)(nil)

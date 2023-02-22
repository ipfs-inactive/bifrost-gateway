package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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
		return nil, wrapRemoteError(err)
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

func wrapRemoteError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) ||
		// Unfortunately this is not an exported type so we have to check for the content.
		strings.Contains(err.Error(), "Client.Timeout exceeded while awaiting headers") {
		return fmt.Errorf("%w: %s", gateway.ErrGatewayTimeout, err.Error())
	} else {
		return fmt.Errorf("%w: %s", gateway.ErrBadGateway, err.Error())
	}
}

var _ exchange.Interface = (*exchangeBsWrapper)(nil)

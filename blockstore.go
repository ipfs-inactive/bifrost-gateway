package main

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	blocks "github.com/ipfs/go-libipfs/blocks"
	"go.uber.org/zap/zapcore"
)

const GetBlockTimeout = time.Second * 60

func newExchange(orchestrator, loggingEndpoint string, cdns *cachedDNS) (exchange.Interface, error) {
	b, err := newCabooseBlockStore(orchestrator, loggingEndpoint, cdns)
	if err != nil {
		return nil, err
	}
	return &exchangeBsWrapper{bstore: b}, nil
}

type exchangeBsWrapper struct {
	bstore blockstore.Blockstore
}

func (e *exchangeBsWrapper) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, cancel := context.WithTimeout(ctx, GetBlockTimeout)
	defer cancel()

	if goLog.Level().Enabled(zapcore.DebugLevel) {
		goLog.Debugw("block requested from strn", "cid", c.String())
	}

	return e.bstore.Get(ctx, c)
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

var _ exchange.Interface = (*exchangeBsWrapper)(nil)

package lib

import (
	"context"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
	"go.uber.org/multierr"
)

type inboundBlockExchange struct {
	ps BlockPubSub
}

func newInboundBlockExchange() *inboundBlockExchange {
	return &inboundBlockExchange{
		ps: NewBlockPubSub(),
	}
}

func (i *inboundBlockExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blk, more := <-i.ps.Subscribe(ctx, c.Hash())
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !more {
		return nil, format.ErrNotFound{Cid: c}
	}
	return blk, nil
}

func (i *inboundBlockExchange) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	mhMap := make(map[string]struct{})
	for _, c := range cids {
		mhMap[string(c.Hash())] = struct{}{}
	}
	mhs := make([]multihash.Multihash, 0, len(mhMap))
	for k := range mhMap {
		mhs = append(mhs, multihash.Multihash(k))
	}
	return i.ps.Subscribe(ctx, mhs...), nil
}

func (i *inboundBlockExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	// TODO: handle context cancellation and/or blockage here
	i.ps.Publish(blocks...)
	return nil
}

func (i *inboundBlockExchange) Close() error {
	i.ps.Shutdown()
	return nil
}

var _ exchange.Interface = (*inboundBlockExchange)(nil)

type handoffExchange struct {
	startingExchange, followupExchange exchange.Interface
	bstore                             blockstore.Blockstore
	handoffCh                          <-chan struct{}
	metrics                            *GraphGatewayMetrics
}

func (f *handoffExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blkCh, err := f.startingExchange.GetBlocks(ctx, []cid.Cid{c})
	if err != nil {
		return nil, err
	}
	blk, ok := <-blkCh
	if ok {
		return blk, nil
	}

	select {
	case <-f.handoffCh:
		graphLog.Debugw("switching to backup block fetcher", "cid", c)
		f.metrics.blockRecoveryAttemptMetric.Inc()
		return f.followupExchange.GetBlock(ctx, c)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *handoffExchange) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	blkCh, err := f.startingExchange.GetBlocks(ctx, cids)
	if err != nil {
		return nil, err
	}

	retCh := make(chan blocks.Block)

	go func() {
		cs := cid.NewSet()
		for cs.Len() < len(cids) {
			blk, ok := <-blkCh
			if !ok {
				break
			}
			select {
			case retCh <- blk:
				cs.Add(blk.Cid())
			case <-ctx.Done():
			}
		}

		for cs.Len() < len(cids) {
			select {
			case <-ctx.Done():
				return
			case <-f.handoffCh:
				var newCidArr []cid.Cid
				for _, c := range cids {
					if !cs.Has(c) {
						blk, _ := f.bstore.Get(ctx, c)
						if blk != nil {
							select {
							case retCh <- blk:
								cs.Add(blk.Cid())
							case <-ctx.Done():
								return
							}
						} else {
							newCidArr = append(newCidArr, c)
						}
					}
				}

				if len(newCidArr) == 0 {
					return
				}

				graphLog.Debugw("needed to use use a backup fetcher for cids", "cids", newCidArr)
				f.metrics.blockRecoveryAttemptMetric.Add(float64(len(newCidArr)))
				fch, err := f.followupExchange.GetBlocks(ctx, newCidArr)
				if err != nil {
					graphLog.Errorw("error getting blocks from followupExchange", "error", err)
					return
				}
				for cs.Len() < len(cids) {
					blk, ok := <-fch
					if !ok {
						return
					}
					select {
					case retCh <- blk:
						cs.Add(blk.Cid())
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return retCh, nil
}

func (f *handoffExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	err1 := f.startingExchange.NotifyNewBlocks(ctx, blocks...)
	err2 := f.followupExchange.NotifyNewBlocks(ctx, blocks...)
	return multierr.Combine(err1, err2)
}

func (f *handoffExchange) Close() error {
	err1 := f.startingExchange.Close()
	err2 := f.followupExchange.Close()
	return multierr.Combine(err1, err2)
}

var _ exchange.Interface = (*handoffExchange)(nil)

type blockFetcherExchWrapper struct {
	f exchange.Fetcher
}

func (b *blockFetcherExchWrapper) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return b.f.GetBlock(ctx, c)
}

func (b *blockFetcherExchWrapper) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	return b.f.GetBlocks(ctx, cids)
}

func (b *blockFetcherExchWrapper) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	return nil
}

func (b *blockFetcherExchWrapper) Close() error {
	return nil
}

var _ exchange.Interface = (*blockFetcherExchWrapper)(nil)

package main

import (
	"context"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/prometheus/client_golang/prometheus"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-libipfs/blocks"

	lru "github.com/hashicorp/golang-lru/v2"
	uatomic "go.uber.org/atomic"
	"go.uber.org/zap/zapcore"
)

const DefaultCacheBlockStoreSize = 1024

func newCacheBlockStore(size int) (blockstore.Blockstore, error) {
	c, err := lru.New2Q[string, []byte](size)
	if err != nil {
		return nil, err
	}

	cacheHitsMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "http",
		Name:      "blockstore_cache_hit",
		Help:      "The number of global block cache hits.",
	})

	cacheRequestsMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "http",
		Name:      "blockstore_cache_requests",
		Help:      "The number of global block cache requests.",
	})

	err = prometheus.Register(cacheHitsMetric)
	if err != nil {
		return nil, err
	}

	err = prometheus.Register(cacheRequestsMetric)
	if err != nil {
		return nil, err
	}

	return &cacheBlockStore{
		cache:               c,
		rehash:              uatomic.NewBool(false),
		cacheHitsMetric:     cacheHitsMetric,
		cacheRequestsMetric: cacheRequestsMetric,
	}, nil
}

type cacheBlockStore struct {
	cache               *lru.TwoQueueCache[string, []byte]
	rehash              *uatomic.Bool
	cacheHitsMetric     prometheus.Counter
	cacheRequestsMetric prometheus.Counter
}

func (l *cacheBlockStore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	l.cache.Remove(string(c.Hash()))
	return nil
}

func (l *cacheBlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return l.cache.Contains(string(c.Hash())), nil
}

func (l *cacheBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	l.cacheRequestsMetric.Add(1)

	blkData, found := l.cache.Get(string(c.Hash()))
	if !found {
		if goLog.Level().Enabled(zapcore.DebugLevel) {
			goLog.Debugw("block not found in cache", "cid", c.String())
		}
		return nil, format.ErrNotFound{Cid: c}
	}

	// It's a HIT!
	l.cacheHitsMetric.Add(1)
	if goLog.Level().Enabled(zapcore.DebugLevel) {
		goLog.Debugw("block found in cache", "cid", c.String())
	}

	if l.rehash.Load() {
		rbcid, err := c.Prefix().Sum(blkData)
		if err != nil {
			return nil, err
		}

		if !rbcid.Equals(c) {
			return nil, blockstore.ErrHashMismatch
		}
	}

	return blocks.NewBlockWithCid(blkData, c)
}

func (l *cacheBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	blkData, found := l.cache.Get(string(c.Hash()))
	if !found {
		return -1, format.ErrNotFound{Cid: c}
	}

	return len(blkData), nil
}

func (l *cacheBlockStore) Put(ctx context.Context, blk blocks.Block) error {
	l.cache.Add(string(blk.Cid().Hash()), blk.RawData())
	return nil
}

func (l *cacheBlockStore) PutMany(ctx context.Context, blks []blocks.Block) error {
	for _, b := range blks {
		if err := l.Put(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

func (l *cacheBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errNotImplemented
}

func (l *cacheBlockStore) HashOnRead(enabled bool) {
	l.rehash.Store(enabled)
}

var _ blockstore.Blockstore = (*cacheBlockStore)(nil)

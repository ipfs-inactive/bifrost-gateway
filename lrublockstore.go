package main

import (
	"context"
	"errors"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-libipfs/blocks"

	lru "github.com/hashicorp/golang-lru/v2"
	uatomic "go.uber.org/atomic"
)

func newLruBlockstore(size int) (blockstore.Blockstore, error) {
	c, err := lru.New2Q[string, []byte](size)
	if err != nil {
		return nil, err
	}
	return &lruBlockstore{
		cache:  c,
		rehash: uatomic.NewBool(false),
	}, nil
}

type lruBlockstore struct {
	cache  *lru.TwoQueueCache[string, []byte]
	rehash *uatomic.Bool
}

func (l *lruBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	l.cache.Remove(string(c.Hash()))
	return nil
}

func (l *lruBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return l.cache.Contains(string(c.Hash())), nil
}

func (l *lruBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blkData, found := l.cache.Get(string(c.Hash()))
	if !found {
		return nil, format.ErrNotFound{Cid: c}
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

func (l *lruBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	blkData, found := l.cache.Get(string(c.Hash()))
	if !found {
		return -1, format.ErrNotFound{Cid: c}
	}

	return len(blkData), nil
}

func (l *lruBlockstore) Put(ctx context.Context, blk blocks.Block) error {
	l.cache.Add(string(blk.Cid().Hash()), blk.RawData())
	return nil
}

func (l *lruBlockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	for _, b := range blks {
		if err := l.Put(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

func (l *lruBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errors.New("not implemented")
}

func (l *lruBlockstore) HashOnRead(enabled bool) {
	l.rehash.Store(enabled)
}

var _ blockstore.Blockstore = (*lruBlockstore)(nil)

package lib

import (
	"context"
	"github.com/multiformats/go-multihash"
	"sync"

	pubsub "github.com/cskr/pubsub"
	blocks "github.com/ipfs/go-libipfs/blocks"
)

// Note: blantantly copied from boxo/bitswap internals https://github.com/ipfs/boxo/blob/664b3e5ee4a997c67909da9b017a28efa40cc8ae/bitswap/client/internal/notifications/notifications.go
// some minor changes were made

const bufferSize = 16

// PubSub is a simple interface for publishing blocks and being able to subscribe
// for multihashes. It's used internally by an exchange to decouple receiving blocks
// and actually providing them back to the GetBlocks caller.
// Note: because multihashes are being requested and blocks returned the codecs could be anything
type PubSub interface {
	Publish(blocks ...blocks.Block)
	Subscribe(ctx context.Context, keys ...multihash.Multihash) <-chan blocks.Block
	Shutdown()
}

// NewPubSub generates a new PubSub interface.
func NewPubSub() PubSub {
	return &impl{
		wrapped: *pubsub.New(bufferSize),
		closed:  make(chan struct{}),
	}
}

type impl struct {
	lk      sync.RWMutex
	wrapped pubsub.PubSub

	closed chan struct{}
}

func (ps *impl) Publish(blocks ...blocks.Block) {
	ps.lk.RLock()
	defer ps.lk.RUnlock()
	select {
	case <-ps.closed:
		return
	default:
	}

	for _, block := range blocks {
		ps.wrapped.Pub(block, string(block.Cid().Hash()))
	}
}

func (ps *impl) Shutdown() {
	ps.lk.Lock()
	defer ps.lk.Unlock()
	select {
	case <-ps.closed:
		return
	default:
	}
	close(ps.closed)
	ps.wrapped.Shutdown()
}

// Subscribe returns a channel of blocks for the given |keys|. |blockChannel|
// is closed if the |ctx| times out or is cancelled, or after receiving the blocks
// corresponding to |keys|.
func (ps *impl) Subscribe(ctx context.Context, keys ...multihash.Multihash) <-chan blocks.Block {

	blocksCh := make(chan blocks.Block, len(keys))
	valuesCh := make(chan interface{}, len(keys)) // provide our own channel to control buffer, prevent blocking
	if len(keys) == 0 {
		close(blocksCh)
		return blocksCh
	}

	// prevent shutdown
	ps.lk.RLock()
	defer ps.lk.RUnlock()

	select {
	case <-ps.closed:
		close(blocksCh)
		return blocksCh
	default:
	}

	// AddSubOnceEach listens for each key in the list, and closes the channel
	// once all keys have been received
	ps.wrapped.AddSubOnceEach(valuesCh, toStrings(keys)...)
	go func() {
		defer func() {
			close(blocksCh)

			ps.lk.RLock()
			defer ps.lk.RUnlock()
			// Don't touch the pubsub instance if we're
			// already closed.
			select {
			case <-ps.closed:
				return
			default:
			}

			ps.wrapped.Unsub(valuesCh)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ps.closed:
			case val, ok := <-valuesCh:
				if !ok {
					return
				}
				block, ok := val.(blocks.Block)
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case blocksCh <- block: // continue
				case <-ps.closed:
				}
			}
		}
	}()

	return blocksCh
}

func toStrings(keys []multihash.Multihash) []string {
	strs := make([]string, 0, len(keys))
	for _, key := range keys {
		strs = append(strs, string(key))
	}
	return strs
}

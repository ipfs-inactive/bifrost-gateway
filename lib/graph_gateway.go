package lib

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	leveldb "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/go-libipfs/gateway"
	"github.com/ipfs/go-namesys"
	"github.com/ipfs/go-namesys/resolve"
	ipfspath "github.com/ipfs/go-path"
	nsopts "github.com/ipfs/interface-go-ipfs-core/options/namesys"
	ifacepath "github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/ipld/go-car"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"io"
	"net/http"
)

// type DataCallback = func(resource string, reader io.Reader) error
// TODO: Don't use a caboose type, perhaps ask them to use a type alias instead of a type
type DataCallback = caboose.DataCallback

type CarFetcher interface {
	Fetch(ctx context.Context, path string, cb DataCallback) error
}

type gwOptions struct {
	ns namesys.NameSystem
	vs routing.ValueStore
	bs blockstore.Blockstore
}

// WithNameSystem sets the name system to use for the gateway. If not set it will use a default DNSLink resolver
// along with any configured ValueStore
func WithNameSystem(ns namesys.NameSystem) GraphGatewayOption {
	return func(opts *gwOptions) error {
		opts.ns = ns
		return nil
	}
}

// WithValueStore sets the ValueStore to use for the gateway
func WithValueStore(vs routing.ValueStore) GraphGatewayOption {
	return func(opts *gwOptions) error {
		opts.vs = vs
		return nil
	}
}

// WithBlockstore sets the Blockstore to use for the gateway
func WithBlockstore(bs blockstore.Blockstore) GraphGatewayOption {
	return func(opts *gwOptions) error {
		opts.bs = bs
		return nil
	}
}

type GraphGatewayOption func(gwOptions *gwOptions) error

type GraphGateway struct {
	fetcher CarFetcher
	routing routing.ValueStore
	namesys namesys.NameSystem
	bstore  blockstore.Blockstore
	bsrv    blockservice.BlockService
}

func NewGraphGatewayBackend(f CarFetcher, opts ...GraphGatewayOption) (*GraphGateway, error) {
	var compiledOptions gwOptions
	for _, o := range opts {
		if err := o(&compiledOptions); err != nil {
			return nil, err
		}
	}

	// Setup a name system so that we are able to resolve /ipns links.
	vs := compiledOptions.vs
	if vs == nil {
		vs = routinghelpers.Null{}
	}

	ns := compiledOptions.ns
	if ns == nil {
		dns, err := gateway.NewDNSResolver(nil, nil)
		if err != nil {
			return nil, err
		}

		ns, err = namesys.NewNameSystem(vs, namesys.WithDNSResolver(dns))
		if err != nil {
			return nil, err
		}
	}

	bs := compiledOptions.bs
	if compiledOptions.bs == nil {
		// Sets up a cache to store blocks in
		cbs, err := NewCacheBlockStore(1000)
		if err != nil {
			return nil, err
		}

		// Set up support for identity hashes (https://github.com/ipfs/bifrost-gateway/issues/38)
		cbs = blockstore.NewIdStore(cbs)
		bs = cbs
	}

	return &GraphGateway{
		fetcher: f,
		routing: vs,
		namesys: ns,
		bstore:  bs,
	}, nil
}

/*
Implementation iteration plan:

1. Fetch CAR into per-request memory blockstore and serve response
2. Fetch CAR into shared memory blockstore and serve response along with a blockservice that does block requests for missing data
3. Start doing the walk locally and then if a path segment is incomplete send a request for blocks and upon every received block try to continue
4. Start doing the walk locally and keep a list of "plausible" blocks, if after issuing a request we get a non-plausible block then report them and attempt to recover by redoing the last segment
5. Don't redo the last segment fully if it's part of a UnixFS file and we can do range requests
*/

func (api *GraphGateway) loadRequestIntoMemoryBlockstoreAndBlocksGateway(ctx context.Context, path string) (gateway.IPFSBackend, error) {
	ds, err := leveldb.NewDatastore("", nil)
	if err != nil {
		return nil, err
	}
	bstore := blockstore.NewBlockstore(ds)
	exch := newInboundBlockExchange()

	doneWithFetcher := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered fetcher error", r)
			}
		}()
		doneWithFetcher <- api.fetcher.Fetch(ctx, path, func(resource string, reader io.Reader) error {
			cr, err := car.NewCarReader(reader)
			if err != nil {
				return err
			}
			for {
				blk, err := cr.Next()
				if err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}
				if err := bstore.Put(ctx, blk); err != nil {
					return err
				}
				if err := exch.NotifyNewBlocks(ctx, blk); err != nil {
					return err
				}
			}
		})
	}()

	bserv := blockservice.New(bstore, exch)
	blkgw, err := gateway.NewBlocksGateway(bserv)
	if err != nil {
		return nil, err
	}

	return blkgw, nil
}

func (api *GraphGateway) Get(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, *gateway.GetResponse, error) {
	blkgw, err := api.loadRequestIntoMemoryBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&depth=1")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	return blkgw.Get(ctx, path)
}

func (api *GraphGateway) GetRange(ctx context.Context, path gateway.ImmutablePath, getRange ...gateway.GetRange) (gateway.ContentPathMetadata, files.File, error) {
	// TODO: actually implement ranges
	blkgw, err := api.loadRequestIntoMemoryBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&depth=1")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	return blkgw.GetRange(ctx, path)
}

func (api *GraphGateway) GetAll(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, files.Node, error) {
	blkgw, err := api.loadRequestIntoMemoryBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&depth=all")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	return blkgw.GetAll(ctx, path)
}

func (api *GraphGateway) GetBlock(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, files.File, error) {
	blkgw, err := api.loadRequestIntoMemoryBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&depth=0")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	return blkgw.GetBlock(ctx, path)
}

func (api *GraphGateway) Head(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, files.Node, error) {
	blkgw, err := api.loadRequestIntoMemoryBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&bytes=0:1023")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	return blkgw.Head(ctx, path)
}

func (api *GraphGateway) ResolvePath(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, error) {
	blkgw, err := api.loadRequestIntoMemoryBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&depth=0")
	if err != nil {
		return gateway.ContentPathMetadata{}, err
	}
	return blkgw.ResolvePath(ctx, path)
}

func (api *GraphGateway) GetCAR(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, io.ReadCloser, <-chan error, error) {
	blkgw, err := api.loadRequestIntoMemoryBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, nil, err
	}
	return blkgw.GetCAR(ctx, path)
}

func (api *GraphGateway) IsCached(ctx context.Context, path ifacepath.Path) bool {
	return false
}

// TODO: This is copy-paste from blocks gateway, maybe share code
func (api *GraphGateway) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	if api.routing == nil {
		return nil, gateway.NewErrorResponse(errors.New("IPNS Record responses are not supported by this gateway"), http.StatusNotImplemented)
	}

	// Fails fast if the CID is not an encoded Libp2p Key, avoids wasteful
	// round trips to the remote routing provider.
	if multicodec.Code(c.Type()) != multicodec.Libp2pKey {
		return nil, gateway.NewErrorResponse(errors.New("cid codec must be libp2p-key"), http.StatusBadRequest)
	}

	// The value store expects the key itself to be encoded as a multihash.
	id, err := peer.FromCid(c)
	if err != nil {
		return nil, err
	}

	return api.routing.GetValue(ctx, "/ipns/"+string(id))
}

// TODO: This is copy-paste from blocks gateway, maybe share code
func (api *GraphGateway) ResolveMutable(ctx context.Context, p ifacepath.Path) (gateway.ImmutablePath, error) {
	err := p.IsValid()
	if err != nil {
		return gateway.ImmutablePath{}, err
	}

	ipath := ipfspath.Path(p.String())
	switch ipath.Segments()[0] {
	case "ipns":
		ipath, err = resolve.ResolveIPNS(ctx, api.namesys, ipath)
		if err != nil {
			return gateway.ImmutablePath{}, err
		}
		imPath, err := gateway.NewImmutablePath(ifacepath.New(ipath.String()))
		if err != nil {
			return gateway.ImmutablePath{}, err
		}
		return imPath, nil
	case "ipfs":
		imPath, err := gateway.NewImmutablePath(ifacepath.New(ipath.String()))
		if err != nil {
			return gateway.ImmutablePath{}, err
		}
		return imPath, nil
	default:
		return gateway.ImmutablePath{}, gateway.NewErrorResponse(fmt.Errorf("unsupported path namespace: %s", p.Namespace()), http.StatusNotImplemented)
	}
}

// TODO: This is copy-paste from blocks gateway, maybe share code
func (api *GraphGateway) GetDNSLinkRecord(ctx context.Context, hostname string) (ifacepath.Path, error) {
	if api.namesys != nil {
		p, err := api.namesys.Resolve(ctx, "/ipns/"+hostname, nsopts.Depth(1))
		if err == namesys.ErrResolveRecursion {
			err = nil
		}
		return ifacepath.New(p.String()), err
	}

	return nil, gateway.NewErrorResponse(errors.New("not implemented"), http.StatusNotImplemented)
}

var _ gateway.IPFSBackend = (*GraphGateway)(nil)

type inboundBlockExchange struct {
	ps PubSub
}

func newInboundBlockExchange() *inboundBlockExchange {
	return &inboundBlockExchange{
		ps: NewPubSub(),
	}
}

func (i *inboundBlockExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blk := <-i.ps.Subscribe(ctx, c.Hash())
	if err := ctx.Err(); err != nil {
		return nil, err
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

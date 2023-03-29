package lib

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
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
	"go.uber.org/multierr"
	"io"
	"net/http"
	"runtime"
	"sync"
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

type Notifier interface {
	NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error
}

type GraphGateway struct {
	fetcher      CarFetcher
	blockFetcher exchange.Fetcher
	routing      routing.ValueStore
	namesys      namesys.NameSystem
	bstore       blockstore.Blockstore
	bsrv         blockservice.BlockService

	lk        sync.RWMutex
	notifiers map[Notifier]struct{}
}

func NewGraphGatewayBackend(f CarFetcher, blockFetcher exchange.Fetcher, opts ...GraphGatewayOption) (*GraphGateway, error) {
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
		cbs, err := NewCacheBlockStore(DefaultCacheBlockStoreSize)
		if err != nil {
			return nil, err
		}

		// Set up support for identity hashes (https://github.com/ipfs/bifrost-gateway/issues/38)
		cbs = blockstore.NewIdStore(cbs)
		bs = cbs
	}

	return &GraphGateway{
		fetcher:      f,
		blockFetcher: blockFetcher,
		routing:      vs,
		namesys:      ns,
		bstore:       bs,
		notifiers:    make(map[Notifier]struct{}),
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

func (api *GraphGateway) loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx context.Context, path string) (gateway.IPFSBackend, func(), error) {
	bstore := api.bstore
	carFetchingExch := newInboundBlockExchange()
	doneWithFetcher := make(chan struct{}, 1)
	exch := &handoffExchange{
		startingExchange: carFetchingExch,
		followupExchange: &blockFetcherExchWrapper{api.blockFetcher},
		handoffCh:        doneWithFetcher,
	}

	api.lk.Lock()
	api.notifiers[exch] = struct{}{}
	api.lk.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered fetcher error", r)
			}
		}()
		err := api.fetcher.Fetch(ctx, path, func(resource string, reader io.Reader) error {
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
				api.notifyAllOngoingRequests(ctx, blk)
			}
		})
		if err != nil {
			goLog.Error(err)
		}
		if err := carFetchingExch.Close(); err != nil {
			goLog.Error(err)
		}
		doneWithFetcher <- struct{}{}
		close(doneWithFetcher)
	}()

	bserv := blockservice.New(bstore, exch)
	blkgw, err := gateway.NewBlocksGateway(bserv)
	if err != nil {
		return nil, nil, err
	}

	return blkgw, func() {
		api.lk.Lock()
		delete(api.notifiers, exch)
		api.lk.Unlock()
	}, nil
}

func (api *GraphGateway) notifyAllOngoingRequests(ctx context.Context, blks ...blocks.Block) {
	api.lk.RLock()
	for n := range api.notifiers {
		err := n.NotifyNewBlocks(ctx, blks...)
		if err != nil {
			goLog.Error(fmt.Errorf("notifyAllOngoingRequests failed: %w", err))
		}
	}
	api.lk.RUnlock()
}

type fileCloseWrapper struct {
	files.File
	closeFn func()
}

func (w *fileCloseWrapper) Close() error {
	w.closeFn()
	return w.File.Close()
}

type dirCloseWrapper struct {
	files.Directory
	closeFn func()
}

func (w *dirCloseWrapper) Close() error {
	w.closeFn()
	return w.Directory.Close()
}

func wrapNodeWithClose[T files.Node](node T, closeFn func()) (T, error) {
	var genericNode files.Node = node
	switch n := genericNode.(type) {
	case files.File:
		var f files.File = &fileCloseWrapper{n, closeFn}
		return f.(T), nil
	case files.Directory:
		var d files.Directory = &dirCloseWrapper{n, closeFn}
		return d.(T), nil
	case *files.Symlink:
		closeFn()
		return node, nil
	default:
		closeFn()
		var zeroType T
		return zeroType, fmt.Errorf("unsupported node type")
	}
}

func (api *GraphGateway) Get(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, *gateway.GetResponse, error) {
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&depth=1")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	md, gr, err := blkgw.Get(ctx, path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	//TODO: interfaces here aren't good enough so we're getting around the problem this way
	runtime.SetFinalizer(gr, func(_ *gateway.GetResponse) { closeFn() })
	return md, gr, err
}

func (api *GraphGateway) GetRange(ctx context.Context, path gateway.ImmutablePath, getRange ...gateway.GetRange) (gateway.ContentPathMetadata, files.File, error) {
	// TODO: actually implement ranges
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&depth=1")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	md, f, err := blkgw.GetRange(ctx, path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	f, err = wrapNodeWithClose(f, closeFn)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	return md, f, nil
}

func (api *GraphGateway) GetAll(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, files.Node, error) {
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&depth=all")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	md, f, err := blkgw.GetAll(ctx, path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	f, err = wrapNodeWithClose(f, closeFn)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	return md, f, nil
}

func (api *GraphGateway) GetBlock(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, files.File, error) {
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&depth=0")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	md, f, err := blkgw.GetBlock(ctx, path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	f, err = wrapNodeWithClose(f, closeFn)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	return md, f, nil
}

func (api *GraphGateway) Head(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, files.Node, error) {
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&bytes=0:1023")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	md, f, err := blkgw.Head(ctx, path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	f, err = wrapNodeWithClose(f, closeFn)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	return md, f, nil
}

func (api *GraphGateway) ResolvePath(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, error) {
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&depth=0")
	if err != nil {
		return gateway.ContentPathMetadata{}, err
	}
	defer closeFn()
	return blkgw.ResolvePath(ctx, path)
}

func (api *GraphGateway) GetCAR(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, io.ReadCloser, <-chan error, error) {
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car")
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, nil, err
	}
	defer closeFn()
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
	ps BlockPubSub
}

func newInboundBlockExchange() *inboundBlockExchange {
	return &inboundBlockExchange{
		ps: NewBlockPubSub(),
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

type handoffExchange struct {
	startingExchange, followupExchange exchange.Interface
	handoffCh                          <-chan struct{}
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
		goLog.Infof("needed to use use a backup fetcher for cid %s", c)
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
						newCidArr = append(newCidArr, c)
					}
				}
				goLog.Infof("needed to use use a backup fetcher for cids %v", newCidArr)
				fch, err := f.followupExchange.GetBlocks(ctx, newCidArr)
				if err != nil {
					goLog.Error(fmt.Errorf("error getting blocks from followup exchange %w", err))
					return
				}
				for cs.Len() < len(cids) {
					select {
					case blk := <-fch:
						select {
						case retCh <- blk:
							cs.Add(blk.Cid())
						case <-ctx.Done():
							return
						}
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

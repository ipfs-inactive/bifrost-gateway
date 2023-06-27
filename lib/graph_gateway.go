package lib

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	nsopts "github.com/ipfs/boxo/coreiface/options/namesys"
	ifacepath "github.com/ipfs/boxo/coreiface/path"
	exchange "github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/ipld/car"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/namesys/resolve"
	ipfspath "github.com/ipfs/boxo/path"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log/v2"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multicodec"
	"github.com/prometheus/client_golang/prometheus"
)

var graphLog = golog.Logger("backend/graph")

const GetBlockTimeout = time.Second * 60

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

	metrics *GraphGatewayMetrics
}

type GraphGatewayMetrics struct {
	contextAlreadyCancelledMetric prometheus.Counter
	carFetchAttemptMetric         prometheus.Counter
	carBlocksFetchedMetric        prometheus.Counter
	blockRecoveryAttemptMetric    prometheus.Counter
	carParamsMetric               *prometheus.CounterVec

	bytesRangeStartMetric prometheus.Histogram
	bytesRangeSizeMetric  prometheus.Histogram
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
		metrics:      registerGraphGatewayMetrics(),
	}, nil
}

func registerGraphGatewayMetrics() *GraphGatewayMetrics {

	// How many CAR Fetch attempts we had? Need this to calculate % of various graph request types.
	// We only count attempts here, because success/failure with/without retries are provided by caboose:
	// - ipfs_caboose_fetch_duration_car_success_count
	// - ipfs_caboose_fetch_duration_car_failure_count
	// - ipfs_caboose_fetch_duration_car_peer_success_count
	// - ipfs_caboose_fetch_duration_car_peer_failure_count
	carFetchAttemptMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "car_fetch_attempts",
		Help:      "The number of times a CAR fetch was attempted by IPFSBackend.",
	})
	prometheus.MustRegister(carFetchAttemptMetric)

	contextAlreadyCancelledMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "car_fetch_context_already_cancelled",
		Help:      "The number of times context is already cancelled when a CAR fetch was attempted by IPFSBackend.",
	})
	prometheus.MustRegister(contextAlreadyCancelledMetric)

	// How many blocks were read via CARs?
	// Need this as a baseline to reason about error ratio vs raw_block_recovery_attempts.
	carBlocksFetchedMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "car_blocks_fetched",
		Help:      "The number of blocks successfully read via CAR fetch.",
	})
	prometheus.MustRegister(carBlocksFetchedMetric)

	// How many times CAR response was not enough or just failed, and we had to read a block via ?format=raw
	// We only count attempts here, because success/failure with/without retries are provided by caboose:
	// - ipfs_caboose_fetch_duration_block_success_count
	// - ipfs_caboose_fetch_duration_block_failure_count
	// - ipfs_caboose_fetch_duration_block_peer_success_count
	// - ipfs_caboose_fetch_duration_block_peer_failure_count
	blockRecoveryAttemptMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "raw_block_recovery_attempts",
		Help:      "The number of ?format=raw  block fetch attempts due to GraphGateway failure (CAR fetch error, missing block in CAR response, or a block evicted from cache too soon).",
	})
	prometheus.MustRegister(blockRecoveryAttemptMetric)

	carParamsMetric := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "car_fetch_params",
		Help:      "How many times specific CAR parameter was used during CAR data fetch.",
	}, []string{"dagScope", "entityRanges"}) // we use 'ranges' instead of 'bytes' here because we only count the number of ranges present
	prometheus.MustRegister(carParamsMetric)

	bytesRangeStartMetric := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "range_request_start",
		Help:      "Tracks where did the range request start.",
		Buckets:   prometheus.ExponentialBuckets(1024, 2, 24), // 1024 bytes to 8 GiB
	})
	prometheus.MustRegister(bytesRangeStartMetric)

	bytesRangeSizeMetric := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "range_request_size",
		Help:      "Tracks the size of range requests.",
		Buckets:   prometheus.ExponentialBuckets(256*1024, 2, 10), // From 256KiB to 100MiB
	})
	prometheus.MustRegister(bytesRangeSizeMetric)

	return &GraphGatewayMetrics{
		contextAlreadyCancelledMetric,
		carFetchAttemptMetric,
		carBlocksFetchedMetric,
		blockRecoveryAttemptMetric,
		carParamsMetric,
		bytesRangeStartMetric,
		bytesRangeSizeMetric,
	}
}

var cacheLimiter = semaphore.NewWeighted(2048)
var cachePool = sync.Pool{
	New: func() any {
		bs, _ := NewCacheBlockStore(512)
		return bs
	},
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
	bstore := cachePool.Get().(blockstore.Blockstore)
	exch := newBlockExchange(bstore, api.blockFetcher)

	go func(metrics *GraphGatewayMetrics) {
		defer func() {
			if r := recover(); r != nil {
				// TODO: move to Debugw?
				graphLog.Errorw("Recovered fetcher error", "path", path, "error", r, "stacktrace", string(debug.Stack()))
			}
		}()
		metrics.carFetchAttemptMetric.Inc()

		if ce := ctx.Err(); ce != nil && errors.Is(ce, context.Canceled) {
			metrics.contextAlreadyCancelledMetric.Inc()
		}

		cctx, cncl := context.WithCancel(ctx)
		defer cncl()
		err := api.fetcher.Fetch(cctx, path, func(resource string, reader io.Reader) error {
			cr, err := car.NewCarReader(reader)
			if err != nil {
				return err
			}

			cbCtx, cncl := context.WithCancel(cctx)
			defer cncl()

			type blockRead struct {
				block blocks.Block
				err   error
			}

			blkCh := make(chan blockRead, 1)
			go func() {
				defer close(blkCh)
				for {
					blk, rdErr := cr.Next()
					select {
					case blkCh <- blockRead{blk, rdErr}:
						if rdErr != nil {
							return
						}
					case <-cbCtx.Done():
						return
					}
				}
			}()

			// initially set a higher timeout here so that if there's an initial timeout error we get it from the car reader.
			t := time.NewTimer(GetBlockTimeout * 2)
			for {
				var blkRead blockRead
				var ok bool
				select {
				case blkRead, ok = <-blkCh:
					if !t.Stop() {
						<-t.C
					}
					t.Reset(GetBlockTimeout)
				case <-t.C:
					return gateway.ErrGatewayTimeout
				}
				if !ok || blkRead.err != nil {
					if errors.Is(blkRead.err, io.EOF) {
						return nil
					}
					return blkRead.err
				}
				if blkRead.block != nil {
					if err := bstore.PutMany(ctx, []blocks.Block{blkRead.block}); err != nil {
						return err
					}
					metrics.carBlocksFetchedMetric.Inc()
					exch.NotifyNewBlocks(ctx, blkRead.block)
				}
			}
		})
		if err != nil {
			graphLog.Infow("car Fetch failed", "path", path, "error", err)
		}
		if err := exch.Close(); err != nil {
			graphLog.Errorw("carFetchingExch.Close()", "error", err)
		}
	}(api.metrics)

	bserv := blockservice.New(bstore, exch)
	blkgw, err := gateway.NewBlocksGateway(bserv)
	if err != nil {
		return nil, nil, err
	}

	return blkgw, func() {}, nil
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
	case *files.Symlink:
		closeFn()
		return node, nil
	case files.File:
		var f files.File = &fileCloseWrapper{n, closeFn}
		return f.(T), nil
	case files.Directory:
		var d files.Directory = &dirCloseWrapper{n, closeFn}
		return d.(T), nil
	default:
		closeFn()
		var zeroType T
		return zeroType, fmt.Errorf("unsupported node type")
	}
}

func (api *GraphGateway) Get(ctx context.Context, path gateway.ImmutablePath, byteRanges ...gateway.ByteRange) (gateway.ContentPathMetadata, *gateway.GetResponse, error) {
	if err := cacheLimiter.Acquire(ctx, 1); err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	defer cacheLimiter.Release(1)

	rangeCount := len(byteRanges)
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "entity", "entityRanges": strconv.Itoa(rangeCount)}).Inc()

	// IPIP-402
	carParams := "?format=car&dag-scope=entity"

	// If request was HTTP Range Request fetch CAR with entity-bytes=from:to to
	// get minimal set of blocks
	// Note: majority of requests have 0 or max 1 ranges. if there are more ranges than one,
	// that is a niche edge cache we don't prefetch as CAR and use fallback blockstore instead.
	if rangeCount > 0 {
		bytesBuilder := strings.Builder{}
		bytesBuilder.WriteString("&entity-bytes=")
		r := byteRanges[0]

		bytesBuilder.WriteString(strconv.FormatUint(r.From, 10))
		bytesBuilder.WriteString(":")

		// TODO: move to boxo or to loadRequestIntoSharedBlockstoreAndBlocksGateway after we pass params in a humane way
		api.metrics.bytesRangeStartMetric.Observe(float64(r.From))

		if r.To != nil {
			bytesBuilder.WriteString(strconv.FormatInt(*r.To, 10))

			// TODO: move to boxo or to loadRequestIntoSharedBlockstoreAndBlocksGateway after we pass params in a humane way
			api.metrics.bytesRangeSizeMetric.Observe(float64(*r.To) - float64(r.From) + 1)
		} else {
			bytesBuilder.WriteString("*")
		}
		carParams += bytesBuilder.String()
	}

	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+carParams)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	md, gr, err := blkgw.Get(ctx, path, byteRanges...)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}

	//TODO: interfaces here aren't good enough so we're getting around the problem this way
	runtime.SetFinalizer(gr, func(_ *gateway.GetResponse) { closeFn() })
	return md, gr, nil
}

func (api *GraphGateway) GetAll(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, files.Node, error) {
	if err := cacheLimiter.Acquire(ctx, 1); err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	defer cacheLimiter.Release(1)

	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "all", "entityRanges": "0"}).Inc()
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&dag-scope=all")
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
	if err := cacheLimiter.Acquire(ctx, 1); err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	defer cacheLimiter.Release(1)

	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "block", "entityRanges": "0"}).Inc()
	// TODO: if path is `/ipfs/cid`, we should use ?format=raw
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&dag-scope=block")
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
	if err := cacheLimiter.Acquire(ctx, 1); err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	defer cacheLimiter.Release(1)

	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "entity", "entityRanges": "1"}).Inc()

	// TODO:  we probably want to move this either to boxo, or at least to loadRequestIntoSharedBlockstoreAndBlocksGateway
	api.metrics.bytesRangeStartMetric.Observe(0)
	api.metrics.bytesRangeSizeMetric.Observe(1024)

	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&dag-scope=entity&entity-bytes=0:1023")

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
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "block", "entityRanges": "0"}).Inc()
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&dag-scope=block")
	if err != nil {
		return gateway.ContentPathMetadata{}, err
	}
	defer closeFn()
	return blkgw.ResolvePath(ctx, path)
}

func (api *GraphGateway) GetCAR(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, io.ReadCloser, <-chan error, error) {
	if err := cacheLimiter.Acquire(ctx, 1); err != nil {
		return gateway.ContentPathMetadata{}, nil, nil, err
	}
	defer cacheLimiter.Release(1)

	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "all", "entityRanges": "0"}).Inc()
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&dag-scope=all")
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

type blockingExchange struct {
	notify chan struct{}
	nl     sync.Mutex

	bstore blockstore.Blockstore
	f      exchange.Fetcher
}

func newBlockExchange(bstore blockstore.Blockstore, fetcher exchange.Fetcher) *blockingExchange {
	return &blockingExchange{
		notify: make(chan struct{}),
		bstore: bstore,
		f:      fetcher,
	}
}

func (b *blockingExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	<-b.notify

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if blk, err := b.bstore.Get(ctx, c); err == nil {
		return blk, nil
	}
	blk, err := b.f.GetBlock(ctx, c)
	if err == nil && blk != nil {
		b.bstore.Put(ctx, blk)
	}
	return blk, err
}

func (b *blockingExchange) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	ch := make(chan blocks.Block)

	go func(ctx context.Context, cids []cid.Cid) {
		for _, c := range cids {
			blk, err := b.GetBlock(ctx, c)
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				ch <- blk
			}
		}
	}(ctx, cids)

	return ch, nil
}

func (b *blockingExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	b.nl.Lock()
	defer b.nl.Unlock()
	on := b.notify
	if on == nil {
		return nil
	}
	b.notify = make(chan struct{})
	close(on)
	return nil
}

func (b *blockingExchange) Close() error {
	b.nl.Lock()
	on := b.notify
	b.notify = nil
	close(on)
	b.nl.Unlock()
	return nil
}

var _ exchange.Interface = (*blockingExchange)(nil)

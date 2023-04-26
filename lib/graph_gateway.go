package lib

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"

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
	format "github.com/ipfs/go-ipld-format"
	golog "github.com/ipfs/go-log/v2"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
)

var graphLog = golog.Logger("backend/graph")

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

// notifiersForRootCid is used for reducing lock contention by only notifying
// exchanges related to the same content root CID
type notifiersForRootCid struct {
	lk        sync.RWMutex
	deleted   int8
	notifiers []Notifier
}

type GraphGateway struct {
	fetcher      CarFetcher
	blockFetcher exchange.Fetcher
	routing      routing.ValueStore
	namesys      namesys.NameSystem
	bstore       blockstore.Blockstore

	notifiers sync.Map // cid -> notifiersForRootCid
	metrics   *GraphGatewayMetrics
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
		notifiers:    sync.Map{},
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
	}, []string{"carScope", "ranges"}) // we use 'ranges' instead of 'bytes' here because we only caount the number of ranges present
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

func (api *GraphGateway) getRootOfPath(path string) string {
	pth, err := ipfspath.ParsePath(path)
	if err != nil {
		return path
	}
	if pth.IsJustAKey() {
		return pth.Segments()[0]
	} else {
		return pth.Segments()[1]
	}
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
		metrics:          api.metrics,
	}

	notifierKey := api.getRootOfPath(path)
	var notifier *notifiersForRootCid
	for {
		notifiers, _ := api.notifiers.LoadOrStore(notifierKey, &notifiersForRootCid{notifiers: []Notifier{}})
		if n, ok := notifiers.(*notifiersForRootCid); ok {
			n.lk.Lock()
			// could have been deleted after our load. try again.
			if n.deleted != 0 {
				n.lk.Unlock()
				continue
			}
			notifier = n
			n.notifiers = append(n.notifiers, exch)
			n.lk.Unlock()
			break
		} else {
			return nil, nil, errors.New("failed to get notifier")
		}
	}

	go func(metrics *GraphGatewayMetrics) {
		defer func() {
			if r := recover(); r != nil {
				// TODO: move to Debugw?
				graphLog.Errorw("Recovered fetcher error", "path", path, "error", r)
			}
		}()
		metrics.carFetchAttemptMetric.Inc()

		if ce := ctx.Err(); ce != nil && errors.Is(ce, context.Canceled) {
			metrics.contextAlreadyCancelledMetric.Inc()
		}

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
				metrics.carBlocksFetchedMetric.Inc()
				api.notifyOngoingRequests(ctx, notifierKey, blk)
			}
		})
		if err != nil {
			graphLog.Debugw("car Fetch failed", "path", path, "error", err)
		}
		if err := carFetchingExch.Close(); err != nil {
			graphLog.Errorw("carFetchingExch.Close()", "error", err)
		}
		doneWithFetcher <- struct{}{}
		close(doneWithFetcher)
	}(api.metrics)

	bserv := blockservice.New(bstore, exch)
	blkgw, err := gateway.NewBlocksGateway(bserv)
	if err != nil {
		return nil, nil, err
	}

	return blkgw, func() {
		notifier.lk.Lock()
		for i, e := range notifier.notifiers {
			if e == exch {
				notifier.notifiers = append(notifier.notifiers[0:i], notifier.notifiers[i+1:]...)
				break
			}
		}
		if len(notifier.notifiers) == 0 {
			notifier.deleted = 1
			api.notifiers.Delete(notifierKey)
		}
		notifier.lk.Unlock()
	}, nil
}

func (api *GraphGateway) notifyOngoingRequests(ctx context.Context, key string, blks ...blocks.Block) {
	if notifiers, ok := api.notifiers.Load(key); ok {
		notifier, ok := notifiers.(*notifiersForRootCid)
		if !ok {
			graphLog.Errorw("notifyOngoingRequests failed", "key", key, "error", "could not get notifiersForRootCid")
			return
		}
		notifier.lk.RLock()
		for _, n := range notifier.notifiers {
			err := n.NotifyNewBlocks(ctx, blks...)
			if err != nil {
				graphLog.Errorw("notifyOngoingRequests failed", "key", key, "error", err)
			}
		}
		notifier.lk.RUnlock()
	}
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
	rangeCount := len(byteRanges)
	api.metrics.carParamsMetric.With(prometheus.Labels{"carScope": "file", "ranges": strconv.Itoa(rangeCount)}).Inc()

	// TODO: remove &depth=  from all CAR request after transition is done:
	// https://github.com/ipfs/bifrost-gateway/issues/80
	carParams := "?format=car&car-scope=file&depth=1"

	// fetch CAR with &bytes= to get minimal set of blocks for the request
	// Note: majority of requests have 0 or max 1 ranges. if there are more ranges than one,
	// that is a niche edge cache we don't prefetch as CAR and use fallback blockstore instead.
	if rangeCount > 0 {
		bytesBuilder := strings.Builder{}
		bytesBuilder.WriteString("&bytes=")
		r := byteRanges[0]

		bytesBuilder.WriteString(strconv.FormatUint(r.From, 10))
		bytesBuilder.WriteString(":")

		// TODO: move to boxo or to loadRequestIntoSharedBlockstoreAndBlocksGateway after we pass params in a humane way
		api.metrics.bytesRangeStartMetric.Observe(float64(r.From))

		if r.To != nil {
			bytesBuilder.WriteString(strconv.FormatInt(*r.To, 10))

			// TODO: move to boxo or to loadRequestIntoSharedBlockstoreAndBlocksGateway after we pass params in a humane way
			api.metrics.bytesRangeSizeMetric.Observe(float64(*r.To) - float64(r.From) + 1)
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
	api.metrics.carParamsMetric.With(prometheus.Labels{"carScope": "all", "ranges": "0"}).Inc()
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&car-scope=all&depth=all")
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
	api.metrics.carParamsMetric.With(prometheus.Labels{"carScope": "block", "ranges": "0"}).Inc()
	// TODO: if path is `/ipfs/cid`, we should use ?format=raw
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&car-scope=block&depth=0")
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
	api.metrics.carParamsMetric.With(prometheus.Labels{"carScope": "file", "ranges": "1"}).Inc()

	// TODO:  we probably want to move this either to boxo, or at least to loadRequestIntoSharedBlockstoreAndBlocksGateway
	api.metrics.bytesRangeStartMetric.Observe(0)
	api.metrics.bytesRangeSizeMetric.Observe(1024)

	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&car-scope=file&depth=1&bytes=0:1023")

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
	api.metrics.carParamsMetric.With(prometheus.Labels{"carScope": "block", "ranges": "0"}).Inc()
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&car-scope=block&depth=0")
	if err != nil {
		return gateway.ContentPathMetadata{}, err
	}
	defer closeFn()
	return blkgw.ResolvePath(ctx, path)
}

func (api *GraphGateway) GetCAR(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, io.ReadCloser, <-chan error, error) {
	api.metrics.carParamsMetric.With(prometheus.Labels{"carScope": "all", "ranges": "0"}).Inc()
	blkgw, closeFn, err := api.loadRequestIntoSharedBlockstoreAndBlocksGateway(ctx, path.String()+"?format=car&car-scope=all&depth=all")
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
						newCidArr = append(newCidArr, c)
					}
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

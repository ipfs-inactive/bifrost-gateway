package lib

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/boxo/path/resolver"
	"io"
	"net/http"
	gopath "path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	nsopts "github.com/ipfs/boxo/coreiface/options/namesys"
	ifacepath "github.com/ipfs/boxo/coreiface/path"
	exchange "github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/ipld/car"
	carv2 "github.com/ipfs/boxo/ipld/car/v2"
	"github.com/ipfs/boxo/ipld/car/v2/storage"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/namesys/resolve"
	ipfspath "github.com/ipfs/boxo/path"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	golog "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-unixfsnode"
	ufsData "github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
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
		bstore:           bstore,
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
					if err := bstore.Put(ctx, blkRead.block); err != nil {
						return err
					}
					metrics.carBlocksFetchedMetric.Inc()
					api.notifyOngoingRequests(ctx, notifierKey, blkRead.block)
				}
			}
		})
		if err != nil {
			graphLog.Infow("car Fetch failed", "path", path, "error", err)
		}
		if err := carFetchingExch.Close(); err != nil {
			graphLog.Errorw("carFetchingExch.Close()", "error", err)
		}
		doneWithFetcher <- struct{}{}
		close(doneWithFetcher)
	}(api.metrics)

	bserv := blockservice.New(bstore, exch)
	blkgw, err := gateway.NewBlocksBackend(bserv)
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
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "entity", "entityRanges": strconv.Itoa(rangeCount)}).Inc()

	// TODO: remove &bytes= &depth= and &car-scope from all CAR request after transition is done:
	// https://github.com/ipfs/bifrost-gateway/issues/80
	carParams := "?format=car&dag-scope=entity"

	// fetch CAR with &bytes= to get minimal set of blocks for the request
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

	rootCid, err := getRootCid(path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	p := ipfspath.FromString(path.String())

	type terminalPathType struct {
		resp    *gateway.GetResponse
		err     error
		lastCid cid.Cid
	}

	terminalPathElementCh := make(chan terminalPathType, 1)
	errNotUnixFS := fmt.Errorf("data was not unixfs")

	var terminalFile *multiReadCloser
	var terminalDir chan unixfs.LinkResult
	lastDirLinkNum := 0
	go func() {
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		err = api.fetcher.Fetch(cctx, path.String()+"?format=car&dag-scope=all", func(resource string, reader io.Reader) error {
			gb, err := carToLinearBlockGetter(cctx, reader, api.metrics)
			if err != nil {
				return err
			}
			r, lsys := getIPFSPathResolverAndLsysFromBlockReader(cctx, gb)
			// First resolve the path since we always need to.
			lastCid, remainder, err := r.ResolveToLastNode(cctx, p)
			if err != nil {
				return err
			}

			lctx := ipld.LinkContext{Ctx: cctx}
			pathTerminalCidLink := cidlink.Link{Cid: lastCid}

			if len(remainder) > 0 {
				terminalPathElementCh <- terminalPathType{err: errNotUnixFS}
				return nil
			}

			// From now on, dag-scope=entity!
			// Since we need more of the graph load it to figure out what we have
			// This includes determining if the terminal node is UnixFS or not
			pc := dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
				if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
					return tlnkNd.LinkTargetNodePrototype(), nil
				}
				return basicnode.Prototype.Any, nil
			})

			np, err := pc(pathTerminalCidLink, lctx)
			if err != nil {
				return err
			}

			lastCidNode, nodeBytes, err := lsys.LoadPlusRaw(lctx, pathTerminalCidLink, np)
			if err != nil {
				return err
			}

			if pbn, ok := lastCidNode.(dagpb.PBNode); !ok {
				// If it's not valid dag-pb then we're done
				return nil
			} else if len(remainder) > 0 {
				// If we're trying to path into dag-pb node that's invalid and we're done
				return nil
			} else if !pbn.FieldData().Exists() {
				// If it's not valid UnixFS then we're done
				return nil
			} else if unixfsFieldData, decodeErr := ufsData.DecodeUnixFSData(pbn.Data.Must().Bytes()); decodeErr != nil {
				// If it's not valid dag-pb and UnixFS then we're done
				return nil
			} else {
				switch fieldNum := unixfsFieldData.FieldDataType().Int(); fieldNum {
				case ufsData.Data_Symlink:
					fd := unixfsFieldData.FieldData()
					if fd.Exists() {
						lnkTarget := string(fd.Must().Bytes())
						f := files.NewLinkFile(lnkTarget, nil)
						s, ok := f.(*files.Symlink)
						if !ok {
							terminalPathElementCh <- terminalPathType{err: fmt.Errorf("should be unreachable: symlink does not have a symlink type")}
							return nil
						}
						terminalPathElementCh <- terminalPathType{resp: gateway.NewGetResponseFromSymlink(s, int64(len(lnkTarget))), lastCid: lastCid}
						return nil
					}
					terminalPathElementCh <- terminalPathType{err: fmt.Errorf("UnixFS Symlink does not contain target")}
					return nil
				case ufsData.Data_Metadata:
					terminalPathElementCh <- terminalPathType{err: fmt.Errorf("UnixFS Metadata unsupported")}
					return nil
				case ufsData.Data_HAMTShard, ufsData.Data_Directory:
					blk, err := blocks.NewBlockWithCid(nodeBytes, lastCid)
					if err != nil {
						terminalPathElementCh <- terminalPathType{err: fmt.Errorf("could not create block: %w", err)}
						return nil
					}
					dirRootNd, err := merkledag.ProtoNodeConverter(blk, lastCidNode)
					if err != nil {
						terminalPathElementCh <- terminalPathType{err: fmt.Errorf("could not create dag-pb universal block from UnixFS directory root: %w", err)}
						return nil
					}
					pn, ok := dirRootNd.(*merkledag.ProtoNode)
					if !ok {
						terminalPathElementCh <- terminalPathType{err: fmt.Errorf("could not create dag-pb node from UnixFS directory root: %w", err)}
						return nil
					}

					sz, err := pn.Size()
					if err != nil {
						terminalPathElementCh <- terminalPathType{err: fmt.Errorf("could not get cumulative size from dag-pb node: %w", err)}
						return nil
					}

					lnks := make(chan unixfs.LinkResult)
					defer close(lnks)
					if terminalDir == nil {
						terminalDir = lnks
					}
					terminalPathElementCh <- terminalPathType{resp: gateway.NewGetResponseFromDirectoryListing(sz, lnks, nil)}

					dirLinkNum := 0
					switch fieldNum {
					case ufsData.Data_Directory:
						iter := pbn.Links.Iterator()
						for !iter.Done() {
							_, v := iter.Next()
							c := v.Hash.Link().(cidlink.Link).Cid
							var name string
							var size int64
							if v.Name.Exists() {
								name = v.Name.Must().String()
							}
							if v.Tsize.Exists() {
								size = v.Tsize.Must().Int()
							}
							lnk := unixfs.LinkResult{Link: &format.Link{
								Name: name,
								Size: uint64(size),
								Cid:  c,
							}}

							dirLinkNum++
							if dirLinkNum-1 <= lastDirLinkNum {
								continue
							}

							select {
							case lnks <- lnk:
								lastDirLinkNum++
							case <-cctx.Done():
								// TODO: what here, send an error with another select?
								return nil
							}
						}
					case ufsData.Data_HAMTShard:
						// Note: we are making up the entries
						dirNd, err := unixfsnode.Reify(lctx, pbn, lsys)
						if err != nil {
							select {
							case lnks <- unixfs.LinkResult{Err: fmt.Errorf("could not reify sharded directory: %w", err)}:
							case <-cctx.Done():
								// TODO: what here?
							}
							return nil
						}

						mi := dirNd.MapIterator()
						for !mi.Done() {
							k, v, err := mi.Next()
							if err != nil {
								return err
							}
							keyStr, err := k.AsString()
							if err != nil {
								select {
								case lnks <- unixfs.LinkResult{Err: fmt.Errorf("could not interpret directory key as string: %w", err)}:
								case <-cctx.Done():
									// TODO: what here?
								}
								return nil
							}
							valLink, err := v.AsLink()
							if err != nil {
								select {
								case lnks <- unixfs.LinkResult{Err: fmt.Errorf("could not interpret directory value as link: %w", err)}:
								case <-cctx.Done():
									// TODO: what here?
								}
								return nil
							}
							valCid := valLink.(cidlink.Link).Cid
							lnk := unixfs.LinkResult{Link: &format.Link{
								Name: keyStr,
								Size: uint64(0),
								Cid:  valCid,
							}}

							dirLinkNum++
							if dirLinkNum-1 <= lastDirLinkNum {
								continue
							}

							select {
							case lnks <- lnk:
								lastDirLinkNum++
							case <-cctx.Done():
								// TODO: what here?
							}
						}
					}
					return nil
				case ufsData.Data_Raw, ufsData.Data_File:
					nd, err := unixfsnode.Reify(lctx, lastCidNode, lsys)
					if err != nil {
						return err
					}

					fnd, ok := nd.(datamodel.LargeBytesNode)
					if !ok {
						return fmt.Errorf("could not process file since it did not present as large bytes")
					}
					f, err := fnd.AsLargeBytes()
					if err != nil {
						return err
					}

					fileSize, err := f.Seek(0, io.SeekEnd)
					if err != nil {
						terminalPathElementCh <- terminalPathType{err: fmt.Errorf("unable to get UnixFS file size: %w", err)}
						return nil
					}
					_, err = f.Seek(0, io.SeekStart)
					if err != nil {
						terminalPathElementCh <- terminalPathType{err: fmt.Errorf("unable to get reset UnixFS file reader: %w", err)}
						return nil
					}

					if terminalFile == nil {
						mrc := &multiReadCloser{
							closeFn:   nil,
							mr:        f,
							closed:    make(chan struct{}),
							newReader: make(chan io.Reader),
							retErr:    nil,
							isClosed:  false,
						}
						terminalFile = mrc
						terminalPathElementCh <- terminalPathType{resp: gateway.NewGetResponseFromReader(files.NewReaderFile(mrc), fileSize), lastCid: lastCid}
					} else {
						select {
						case terminalFile.newReader <- f:
						case <-cctx.Done():
							// TODO: what here?
							return nil
						}
					}

					_, err = io.Copy(io.Discard, terminalFile)
					if err != nil {
						return err
					}
					return nil
				default:
					terminalPathElementCh <- terminalPathType{err: fmt.Errorf("unknown UnixFS field type")}
					return nil
				}
			}
		})
	}()

	select {
	case t := <-terminalPathElementCh:
		if t.err != nil {
			return gateway.ContentPathMetadata{}, nil, t.err
		}

		md := gateway.ContentPathMetadata{
			PathSegmentRoots: []cid.Cid{rootCid},
			LastSegment:      ifacepath.NewResolvedPath(p, t.lastCid, rootCid, ""),
		}
		return md, t.resp, nil
	case <-ctx.Done():
		return gateway.ContentPathMetadata{}, nil, ctx.Err()
	}
}

type multiReadCloser struct {
	closeFn   func() error
	mr        io.Reader
	closed    chan struct{}
	newReader chan io.Reader
	retErr    error
	isClosed  bool
}

func (r *multiReadCloser) Read(p []byte) (n int, err error) {
	if r.retErr == nil {
		n, err = r.mr.Read(p)
		if err == nil || err == io.EOF {
			return n, err
		}

		if n > 0 {
			r.retErr = err
			return n, nil
		}
	} else {
		err = r.retErr
	}

	select {
	case <-r.closed:
		return n, err
	case newReader, ok := <-r.newReader:
		if ok {
			r.mr = io.MultiReader(r.mr, newReader)
			return r.Read(p)
		}
		return n, err
	}
}

func (r *multiReadCloser) Close() error {
	if r.isClosed {
		return nil
	}

	close(r.newReader)
	close(r.closed)

	if r.closeFn == nil {
		return nil
	}
	return r.closeFn()
}

var _ io.ReadCloser = (*multiReadCloser)(nil)

func (api *GraphGateway) GetAll(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, files.Node, error) {
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
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "block", "entityRanges": "0"}).Inc()
	// TODO: if path is `/ipfs/cid`, we should use ?format=raw
	rootCid, err := getRootCid(path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	p := ipfspath.FromString(path.String())

	var md gateway.ContentPathMetadata
	var f files.File
	var ipldError error
	// TODO: fallback to dynamic fetches in case we haven't requested enough data
	err = api.fetcher.Fetch(ctx, path.String()+"?format=car&dag-scope=block", func(resource string, reader io.Reader) error {
		return checkRetryableError(&ipldError, func() error {
			gb, err := carToLinearBlockGetter(ctx, reader, api.metrics)
			if err != nil {
				return err
			}
			r, lsys := getIPFSPathResolverAndLsysFromBlockReader(ctx, gb)

			// First resolve the path since we always need to.
			lastCid, remainder, err := r.ResolveToLastNode(ctx, p)
			if err != nil {
				return err
			}

			md = gateway.ContentPathMetadata{
				PathSegmentRoots: []cid.Cid{rootCid},
				LastSegment:      ifacepath.NewResolvedPath(p, lastCid, rootCid, gopath.Join(remainder...)),
			}

			lctx := ipld.LinkContext{Ctx: ctx}
			pathTerminalCidLink := cidlink.Link{Cid: lastCid}

			data, err := lsys.LoadRaw(lctx, pathTerminalCidLink)
			if err != nil {
				return err
			}

			f = files.NewBytesFile(data)
			return nil
		})
	})

	if ipldError != nil {
		err = ipldError
	}
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}

	return md, f, nil
}

func (api *GraphGateway) Head(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, *gateway.HeadResponse, error) {
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "entity", "entityRanges": "1"}).Inc()

	// TODO:  we probably want to move this either to boxo, or at least to loadRequestIntoSharedBlockstoreAndBlocksGateway
	api.metrics.bytesRangeStartMetric.Observe(0)
	api.metrics.bytesRangeSizeMetric.Observe(3071)

	rootCid, err := getRootCid(path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	p := ipfspath.FromString(path.String())

	var md gateway.ContentPathMetadata
	var n *gateway.HeadResponse
	var ipldError error
	// TODO: fallback to dynamic fetches in case we haven't requested enough data
	err = api.fetcher.Fetch(ctx, path.String()+"?format=car&dag-scope=entity&entity-bytes=0:3071", func(resource string, reader io.Reader) error {
		return checkRetryableError(&ipldError, func() error {
			gb, err := carToLinearBlockGetter(ctx, reader, api.metrics)
			if err != nil {
				return err
			}
			r, lsys := getIPFSPathResolverAndLsysFromBlockReader(ctx, gb)

			// First resolve the path since we always need to.
			lastCid, remainder, err := r.ResolveToLastNode(ctx, p)
			if err != nil {
				return err
			}

			md = gateway.ContentPathMetadata{
				PathSegmentRoots: []cid.Cid{rootCid},
				LastSegment:      ifacepath.NewResolvedPath(p, lastCid, rootCid, gopath.Join(remainder...)),
			}

			lctx := ipld.LinkContext{Ctx: ctx}
			pathTerminalCidLink := cidlink.Link{Cid: lastCid}

			// Load the block at the root of the terminal path element
			dataBytes, err := lsys.LoadRaw(lctx, pathTerminalCidLink)
			if err != nil {
				return err
			}

			// It's not UnixFS if there is a remainder or it's not dag-pb
			if len(remainder) > 0 || lastCid.Type() != uint64(multicodec.DagPb) {
				n = gateway.NewHeadResponseForFile(files.NewBytesFile(dataBytes), int64(len(dataBytes)))
				return nil
			}

			// Let's figure out if the terminal element is valid UnixFS and if so what kind
			pc := dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
				if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
					return tlnkNd.LinkTargetNodePrototype(), nil
				}
				return basicnode.Prototype.Any, nil
			})

			np, err := pc(pathTerminalCidLink, lctx)
			if err != nil {
				return err
			}

			nodeDecoder, err := lsys.DecoderChooser(pathTerminalCidLink)
			if err != nil {
				return err
			}

			nb := np.NewBuilder()
			err = nodeDecoder(nb, bytes.NewReader(dataBytes))
			if err != nil {
				return err
			}
			lastCidNode := nb.Build()

			if pbn, ok := lastCidNode.(dagpb.PBNode); !ok {
				// This shouldn't be possible since we already checked for dag-pb usage
				return fmt.Errorf("node was not go-codec-dagpb node")
			} else if !pbn.FieldData().Exists() {
				// If it's not valid UnixFS then just return the block bytes
				n = gateway.NewHeadResponseForFile(files.NewBytesFile(dataBytes), int64(len(dataBytes)))
				return nil
			} else if unixfsFieldData, decodeErr := ufsData.DecodeUnixFSData(pbn.Data.Must().Bytes()); decodeErr != nil {
				// If it's not valid UnixFS then just return the block bytes
				n = gateway.NewHeadResponseForFile(files.NewBytesFile(dataBytes), int64(len(dataBytes)))
				return nil
			} else {
				switch fieldNum := unixfsFieldData.FieldDataType().Int(); fieldNum {
				case ufsData.Data_Directory, ufsData.Data_HAMTShard:
					blk, err := blocks.NewBlockWithCid(dataBytes, lastCid)
					if err != nil {
						return fmt.Errorf("could not create block: %w", err)
					}
					dirRootNd, err := merkledag.ProtoNodeConverter(blk, lastCidNode)
					if err != nil {
						return fmt.Errorf("could not create dag-pb universal block from UnixFS directory root: %w", err)
					}
					pn, ok := dirRootNd.(*merkledag.ProtoNode)
					if !ok {
						return fmt.Errorf("could not create dag-pb node from UnixFS directory root: %w", err)
					}

					sz, err := pn.Size()
					if err != nil {
						return fmt.Errorf("could not get cumulative size from dag-pb node: %w", err)
					}

					n = gateway.NewHeadResponseForDirectory(int64(sz))
					return nil
				case ufsData.Data_Symlink:
					fd := unixfsFieldData.FieldData()
					if fd.Exists() {
						n = gateway.NewHeadResponseForSymlink(int64(len(fd.Must().Bytes())))
						return nil
					}
					// If there is no target then it's invalid so just return the block
					gateway.NewHeadResponseForFile(files.NewBytesFile(dataBytes), int64(len(dataBytes)))
					return nil
				case ufsData.Data_Metadata:
					n = gateway.NewHeadResponseForFile(files.NewBytesFile(dataBytes), int64(len(dataBytes)))
					return nil
				case ufsData.Data_Raw, ufsData.Data_File:
					ufsNode, err := unixfsnode.Reify(lctx, pbn, lsys)
					if err != nil {
						return err
					}
					fileNode, ok := ufsNode.(datamodel.LargeBytesNode)
					if !ok {
						return fmt.Errorf("data not a large bytes node despite being UnixFS bytes")
					}
					f, err := fileNode.AsLargeBytes()
					if err != nil {
						return err
					}

					fileSize, err := f.Seek(0, io.SeekEnd)
					if err != nil {
						return fmt.Errorf("unable to get UnixFS file size: %w", err)
					}
					_, err = f.Seek(0, io.SeekStart)
					if err != nil {
						return fmt.Errorf("unable to get reset UnixFS file reader: %w", err)
					}

					out, err := io.ReadAll(io.LimitReader(f, 3072))
					if errors.Is(err, io.EOF) {
						n = gateway.NewHeadResponseForFile(files.NewBytesFile(out), fileSize)
						return nil
					}
					return err
				}
			}
			return nil
		})
	})

	if ipldError != nil {
		err = ipldError
	}
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}

	return md, n, nil
}

func (api *GraphGateway) ResolvePath(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, error) {
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "block", "entityRanges": "0"}).Inc()
	rootCid, err := getRootCid(path)
	if err != nil {
		return gateway.ContentPathMetadata{}, err
	}

	var md gateway.ContentPathMetadata
	var ipldError error
	err = api.fetcher.Fetch(ctx, path.String()+"?format=car&dag-scope=block", func(resource string, reader io.Reader) error {
		return checkRetryableError(&ipldError, func() error {
			gb, err := carToLinearBlockGetter(ctx, reader, api.metrics)
			if err != nil {
				return err
			}
			r, _ := getIPFSPathResolverAndLsysFromBlockReader(ctx, gb)

			// First resolve the path since we always need to.
			lastCid, remainder, err := r.ResolveToLastNode(ctx, ipfspath.FromString(path.String()))
			if err != nil {
				return err
			}

			md = gateway.ContentPathMetadata{
				PathSegmentRoots: []cid.Cid{rootCid},
				LastSegment:      ifacepath.NewResolvedPath(ipfspath.FromString(path.String()), lastCid, rootCid, gopath.Join(remainder...)),
			}

			return nil
		})
	})

	if ipldError != nil {
		err = ipldError
	}
	if err != nil {
		return gateway.ContentPathMetadata{}, err
	}

	return md, nil
}

func (api *GraphGateway) GetCAR(ctx context.Context, path gateway.ImmutablePath, params gateway.CarParams) (gateway.ContentPathMetadata, io.ReadCloser, error) {
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": string(params.Scope), "entityRanges": "0"}).Inc()
	rootCid, err := getRootCid(path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}
	p := ipfspath.FromString(path.String())

	r, w := io.Pipe()
	go func() {
		numBlocksSent := 0
		var cw storage.WritableCar
		var blockBuffer []blocks.Block
		var ipldError error
		err = api.fetcher.Fetch(ctx, path.String()+"?format=car&dag-scope=all", func(resource string, reader io.Reader) error {
			return checkRetryableError(&ipldError, func() error {
				numBlocksThisCall := 0
				gb, err := carToLinearBlockGetter(ctx, reader, api.metrics)
				if err != nil {
					return err
				}
				teeBlock := func(ctx context.Context, c cid.Cid) (blocks.Block, error) {
					blk, err := gb(ctx, c)
					if err != nil {
						return nil, err
					}
					if numBlocksThisCall >= numBlocksSent {
						if cw == nil {
							blockBuffer = append(blockBuffer, blk)
						} else {
							err = cw.Put(ctx, blk.Cid().KeyString(), blk.RawData())
							if err != nil {
								return nil, fmt.Errorf("error writing car block: %w", err)
							}
						}
						numBlocksSent++
					}
					numBlocksThisCall++
					return blk, nil
				}
				r, l := getIPFSPathResolverAndLsysFromBlockReader(ctx, teeBlock)

				// First resolve the path since we always need to.
				lastCid, remainder, err := r.ResolveToLastNode(ctx, p)
				if err != nil {
					return err
				}
				if len(remainder) > 0 {
					return nil
				}

				cw, err = storage.NewWritable(w, []cid.Cid{lastCid}, carv2.WriteAsCarV1(true))
				if err != nil {
					// io.PipeWriter.CloseWithError always returns nil.
					_ = w.CloseWithError(err)
					return nil
				}
				for _, blk := range blockBuffer {
					err = cw.Put(ctx, blk.Cid().KeyString(), blk.RawData())
					if err != nil {
						_ = w.CloseWithError(fmt.Errorf("error writing car block: %w", err))
						return nil
					}
				}
				blockBuffer = nil

				err = walkGatewaySimpleSelector(ctx, lastCid, params, l, r)
				if err != nil {
					return err
				}
				return nil
			})
		})

		if ipldError != nil {
			err = ipldError
		}
		if err != nil {
			// io.PipeWriter.CloseWithError always returns nil.
			_ = w.CloseWithError(err)
		}
	}()

	return gateway.ContentPathMetadata{
		PathSegmentRoots: []cid.Cid{rootCid},
		LastSegment:      ifacepath.NewResolvedPath(p, rootCid, rootCid, ""),
		ContentType:      "",
	}, r, nil
}

func getRootCid(imPath gateway.ImmutablePath) (cid.Cid, error) {
	imPathStr := imPath.String()
	if !strings.HasPrefix(imPathStr, "/ipfs/") {
		return cid.Undef, fmt.Errorf("path does not have /ipfs/ prefix")
	}

	firstSegment, _, _ := strings.Cut(imPathStr[6:], "/")
	rootCid, err := cid.Decode(firstSegment)
	if err != nil {
		return cid.Undef, err
	}

	return rootCid, nil
}

func (api *GraphGateway) IsCached(ctx context.Context, path ifacepath.Path) bool {
	return false
}

// TODO: This is copy-paste from blocks gateway, maybe share code
func (api *GraphGateway) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	if api.routing == nil {
		return nil, gateway.NewErrorStatusCode(errors.New("IPNS Record responses are not supported by this gateway"), http.StatusNotImplemented)
	}

	// Fails fast if the CID is not an encoded Libp2p Key, avoids wasteful
	// round trips to the remote routing provider.
	if multicodec.Code(c.Type()) != multicodec.Libp2pKey {
		return nil, gateway.NewErrorStatusCode(errors.New("cid codec must be libp2p-key"), http.StatusBadRequest)
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
		return gateway.ImmutablePath{}, gateway.NewErrorStatusCode(fmt.Errorf("unsupported path namespace: %s", p.Namespace()), http.StatusNotImplemented)
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

	return nil, gateway.NewErrorStatusCode(errors.New("not implemented"), http.StatusNotImplemented)
}

var _ gateway.IPFSBackend = (*GraphGateway)(nil)

func checkRetryableError(e *error, fn func() error) error {
	err := fn()
	if format.IsNotFound(err) {
		return err
	}
	initialErr := err

	// Checks if err is of a type that does not implement the .Is interface and
	// cannot be directly compared to. Therefore, errors.Is cannot be used.
	for {
		_, ok := err.(resolver.ErrNoLink)
		if ok {
			*e = err
			return nil
		}

		_, ok = err.(datamodel.ErrWrongKind)
		if ok {
			*e = err
			return nil
		}

		_, ok = err.(datamodel.ErrNotExists)
		if ok {
			*e = err
			return nil
		}

		err = errors.Unwrap(err)
		if err == nil {
			return initialErr
		}
	}
}

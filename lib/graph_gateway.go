package lib

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	gopath "path"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/hashicorp/go-multierror"
	nsopts "github.com/ipfs/boxo/coreiface/options/namesys"
	ifacepath "github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/namesys/resolve"
	ipfspath "github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/path/resolver"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	ufsData "github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/hamt"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multicodec"
	"github.com/prometheus/client_golang/prometheus"
)

const GetBlockTimeout = time.Second * 60

// type DataCallback = func(resource string, reader io.Reader) error
// TODO: Don't use a caboose type, perhaps ask them to use a type alias instead of a type
type DataCallback = caboose.DataCallback

// TODO: Don't use a caboose type
type ErrPartialResponse = caboose.ErrPartialResponse

var ErrFetcherUnexpectedEOF = fmt.Errorf("failed to fetch IPLD data")

type CarFetcher interface {
	Fetch(ctx context.Context, path string, cb DataCallback) error
}

type gwOptions struct {
	ns           namesys.NameSystem
	vs           routing.ValueStore
	promRegistry prometheus.Registerer
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

// WithPrometheusRegistry sets the registry to use for metrics collection
func WithPrometheusRegistry(reg prometheus.Registerer) GraphGatewayOption {
	return func(opts *gwOptions) error {
		opts.promRegistry = reg
		return nil
	}
}

type GraphGatewayOption func(gwOptions *gwOptions) error

type GraphGateway struct {
	fetcher CarFetcher
	routing routing.ValueStore
	namesys namesys.NameSystem

	pc traversal.LinkTargetNodePrototypeChooser

	metrics *GraphGatewayMetrics
}

type GraphGatewayMetrics struct {
	contextAlreadyCancelledMetric prometheus.Counter
	carFetchAttemptMetric         prometheus.Counter
	carBlocksFetchedMetric        prometheus.Counter
	carParamsMetric               *prometheus.CounterVec

	bytesRangeStartMetric prometheus.Histogram
	bytesRangeSizeMetric  prometheus.Histogram
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

	var promReg prometheus.Registerer = prometheus.NewRegistry()
	if compiledOptions.promRegistry != nil {
		promReg = compiledOptions.promRegistry
	}

	return &GraphGateway{
		fetcher: f,
		routing: vs,
		namesys: ns,
		metrics: registerGraphGatewayMetrics(promReg),
		pc: dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
			if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
				return tlnkNd.LinkTargetNodePrototype(), nil
			}
			return basicnode.Prototype.Any, nil
		}),
	}, nil
}

func registerGraphGatewayMetrics(registerer prometheus.Registerer) *GraphGatewayMetrics {
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
	registerer.MustRegister(carFetchAttemptMetric)

	contextAlreadyCancelledMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "car_fetch_context_already_cancelled",
		Help:      "The number of times context is already cancelled when a CAR fetch was attempted by IPFSBackend.",
	})
	registerer.MustRegister(contextAlreadyCancelledMetric)

	// How many blocks were read via CARs?
	// Need this as a baseline to reason about error ratio vs raw_block_recovery_attempts.
	carBlocksFetchedMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "car_blocks_fetched",
		Help:      "The number of blocks successfully read via CAR fetch.",
	})
	registerer.MustRegister(carBlocksFetchedMetric)

	carParamsMetric := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "car_fetch_params",
		Help:      "How many times specific CAR parameter was used during CAR data fetch.",
	}, []string{"dagScope", "entityRanges"}) // we use 'ranges' instead of 'bytes' here because we only count the number of ranges present
	registerer.MustRegister(carParamsMetric)

	bytesRangeStartMetric := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "range_request_start",
		Help:      "Tracks where did the range request start.",
		Buckets:   prometheus.ExponentialBuckets(1024, 2, 24), // 1024 bytes to 8 GiB
	})
	registerer.MustRegister(bytesRangeStartMetric)

	bytesRangeSizeMetric := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ipfs",
		Subsystem: "gw_graph_backend",
		Name:      "range_request_size",
		Help:      "Tracks the size of range requests.",
		Buckets:   prometheus.ExponentialBuckets(256*1024, 2, 10), // From 256KiB to 100MiB
	})
	registerer.MustRegister(bytesRangeSizeMetric)

	return &GraphGatewayMetrics{
		contextAlreadyCancelledMetric,
		carFetchAttemptMetric,
		carBlocksFetchedMetric,
		carParamsMetric,
		bytesRangeStartMetric,
		bytesRangeSizeMetric,
	}
}

func paramsToString(params gateway.CarParams) string {
	paramsBuilder := strings.Builder{}
	paramsBuilder.WriteString("dag-scope=")
	paramsBuilder.WriteString(string(params.Scope))
	if params.Range != nil {
		paramsBuilder.WriteString("&entity-bytes=")
		paramsBuilder.WriteString(strconv.FormatInt(params.Range.From, 10))
		paramsBuilder.WriteString(":")
		if params.Range.To != nil {
			paramsBuilder.WriteString(strconv.FormatInt(*params.Range.To, 10))
		} else {
			paramsBuilder.WriteString("*")
		}
	}
	return paramsBuilder.String()
}

func (api *GraphGateway) fetchCAR(ctx context.Context, path gateway.ImmutablePath, params gateway.CarParams, cb DataCallback) error {
	escapedPath := url.PathEscape(path.String())
	escapedPath = strings.ReplaceAll(escapedPath, "%2F", "/")
	paramsStr := paramsToString(params)
	urlWithoutHost := fmt.Sprintf("%s?%s", escapedPath, paramsStr)

	api.metrics.carFetchAttemptMetric.Inc()
	var ipldError error
	fetchErr := api.fetcher.Fetch(ctx, urlWithoutHost, func(resource string, reader io.Reader) error {
		return checkRetryableError(&ipldError, func() error {
			return cb(resource, reader)
		})
	})

	if ipldError != nil {
		fetchErr = ipldError
	}

	return fetchErr
}

// resolvePathWithRootsAndBlock takes a path and linksystem and returns the set of non-terminal cids, the terminal cid, the remainder, and the block corresponding to the terminal cid
func resolvePathWithRootsAndBlock(ctx context.Context, fpath ipfspath.Path, unixFSLsys *ipld.LinkSystem) ([]cid.Cid, cid.Cid, []string, blocks.Block, error) {
	pathRootCids, terminalCid, remainder, terminalBlk, err := resolvePathToLastWithRoots(ctx, fpath, unixFSLsys)
	if err != nil {
		return nil, cid.Undef, nil, nil, err
	}

	if terminalBlk == nil {
		lctx := ipld.LinkContext{Ctx: ctx}
		lnk := cidlink.Link{Cid: terminalCid}
		blockData, err := unixFSLsys.LoadRaw(lctx, lnk)
		if err != nil {
			return nil, cid.Undef, nil, nil, err
		}
		terminalBlk, err = blocks.NewBlockWithCid(blockData, terminalCid)
		if err != nil {
			return nil, cid.Undef, nil, nil, err
		}
	}

	return pathRootCids, terminalCid, remainder, terminalBlk, err
}

// resolvePathToLastWithRoots takes a path and linksystem and returns the set of non-terminal cids, the terminal cid,
// the remainder pathing, the last block loaded, and the last node loaded.
//
// Note: the block returned will be nil if the terminal element is a link or the path is just a CID
func resolvePathToLastWithRoots(ctx context.Context, fpath ipfspath.Path, unixFSLsys *ipld.LinkSystem) ([]cid.Cid, cid.Cid, []string, blocks.Block, error) {
	c, p, err := ipfspath.SplitAbsPath(fpath)
	if err != nil {
		return nil, cid.Undef, nil, nil, err
	}

	if len(p) == 0 {
		return nil, c, nil, nil, nil
	}

	unixFSLsys.NodeReifier = unixfsnode.Reify
	defer func() { unixFSLsys.NodeReifier = nil }()

	var cids []cid.Cid
	cids = append(cids, c)

	pc := dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})

	loadNode := func(ctx context.Context, c cid.Cid) (blocks.Block, ipld.Node, error) {
		lctx := ipld.LinkContext{Ctx: ctx}
		rootLnk := cidlink.Link{Cid: c}
		np, err := pc(rootLnk, lctx)
		if err != nil {
			return nil, nil, err
		}
		nd, blockData, err := unixFSLsys.LoadPlusRaw(lctx, rootLnk, np)
		if err != nil {
			return nil, nil, err
		}
		blk, err := blocks.NewBlockWithCid(blockData, c)
		if err != nil {
			return nil, nil, err
		}
		return blk, nd, nil
	}

	nextBlk, nextNd, err := loadNode(ctx, c)
	if err != nil {
		return nil, cid.Undef, nil, nil, err
	}

	depth := 0
	for i, elem := range p {
		nextNd, err = nextNd.LookupBySegment(ipld.ParsePathSegment(elem))
		if err != nil {
			return nil, cid.Undef, nil, nil, err
		}
		if nextNd.Kind() == ipld.Kind_Link {
			depth = 0
			lnk, err := nextNd.AsLink()
			if err != nil {
				return nil, cid.Undef, nil, nil, err
			}
			cidLnk, ok := lnk.(cidlink.Link)
			if !ok {
				return nil, cid.Undef, nil, nil, fmt.Errorf("link is not a cidlink: %v", cidLnk)
			}
			cids = append(cids, cidLnk.Cid)

			if i < len(p)-1 {
				nextBlk, nextNd, err = loadNode(ctx, cidLnk.Cid)
				if err != nil {
					return nil, cid.Undef, nil, nil, err
				}
			}
		} else {
			depth++
		}
	}

	// if last node is not a link, just return it's cid, add path to remainder and return
	if nextNd.Kind() != ipld.Kind_Link {
		// return the cid and the remainder of the path
		return cids[:len(cids)-1], cids[len(cids)-1], p[len(p)-depth:], nextBlk, nil
	}

	return cids[:len(cids)-1], cids[len(cids)-1], nil, nil, nil
}

func contentMetadataFromRootsAndRemainder(p ipfspath.Path, pathRoots []cid.Cid, terminalCid cid.Cid, remainder []string) gateway.ContentPathMetadata {
	var rootCid cid.Cid
	if len(pathRoots) > 0 {
		rootCid = pathRoots[0]
	} else {
		rootCid = terminalCid
	}
	md := gateway.ContentPathMetadata{
		PathSegmentRoots: pathRoots,
		LastSegment:      ifacepath.NewResolvedPath(p, terminalCid, rootCid, gopath.Join(remainder...)),
	}
	return md
}

var errNotUnixFS = fmt.Errorf("data was not unixfs")

func (api *GraphGateway) Get(ctx context.Context, path gateway.ImmutablePath, byteRanges ...gateway.ByteRange) (gateway.ContentPathMetadata, *gateway.GetResponse, error) {
	rangeCount := len(byteRanges)
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "entity", "entityRanges": strconv.Itoa(rangeCount)}).Inc()

	carParams := gateway.CarParams{Scope: gateway.DagScopeEntity}

	// fetch CAR with &bytes= to get minimal set of blocks for the request
	// Note: majority of requests have 0 or max 1 ranges. if there are more ranges than one,
	// that is a niche edge cache we don't prefetch as CAR and use fallback blockstore instead.
	if rangeCount > 0 {
		r := byteRanges[0]
		carParams.Range = &gateway.DagByteRange{
			From: int64(r.From),
		}

		// TODO: move to boxo or to loadRequestIntoSharedBlockstoreAndBlocksGateway after we pass params in a humane way
		api.metrics.bytesRangeStartMetric.Observe(float64(r.From))

		if r.To != nil {
			carParams.Range.To = r.To

			// TODO: move to boxo or to loadRequestIntoSharedBlockstoreAndBlocksGateway after we pass params in a humane way
			api.metrics.bytesRangeSizeMetric.Observe(float64(*r.To) - float64(r.From) + 1)
		}
	}

	md, terminalElem, err := fetchWithPartialRetries(ctx, path, carParams, loadTerminalEntity, api.metrics, api.fetchCAR)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}

	var resp *gateway.GetResponse

	switch typedTerminalElem := terminalElem.(type) {
	case *gateway.GetResponse:
		resp = typedTerminalElem
	case *backpressuredFile:
		resp = gateway.NewGetResponseFromReader(typedTerminalElem, typedTerminalElem.size)
	case *backpressuredHAMTDirIterNoRecursion:
		ch := make(chan unixfs.LinkResult)
		go func() {
			defer close(ch)
			for typedTerminalElem.Next() {
				l := typedTerminalElem.Link()
				select {
				case ch <- l:
				case <-ctx.Done():
					return
				}
			}
			if err := typedTerminalElem.Err(); err != nil {
				select {
				case ch <- unixfs.LinkResult{Err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
		resp = gateway.NewGetResponseFromDirectoryListing(typedTerminalElem.dagSize, ch, nil)
	default:
		return gateway.ContentPathMetadata{}, nil, fmt.Errorf("invalid data type")
	}

	return md, resp, nil

}

// loadTerminalEntity returns either a [*gateway.GetResponse], [*backpressuredFile], or [*backpressuredHAMTDirIterNoRecursion]
func loadTerminalEntity(ctx context.Context, c cid.Cid, blk blocks.Block, lsys *ipld.LinkSystem, params gateway.CarParams, getLsys lsysGetter) (interface{}, error) {
	var err error
	if lsys == nil {
		lsys, err = getLsys(ctx, c, params)
		if err != nil {
			return nil, err
		}
	}

	lctx := ipld.LinkContext{Ctx: ctx}

	if c.Type() != uint64(multicodec.DagPb) {
		var blockData []byte

		if blk != nil {
			blockData = blk.RawData()
		} else {
			blockData, err = lsys.LoadRaw(lctx, cidlink.Link{Cid: c})
			if err != nil {
				return nil, err
			}
		}

		return gateway.NewGetResponseFromReader(files.NewBytesFile(blockData), int64(len(blockData))), nil
	}

	blockData, pbn, _, fieldNum, fieldDataBytes, err := loadUnixFSBase(ctx, c, blk, lsys)
	if err != nil {
		return nil, err
	}

	switch fieldNum {
	case ufsData.Data_Symlink:
		lnkTarget := string(fieldDataBytes)
		f := gateway.NewGetResponseFromSymlink(files.NewLinkFile(lnkTarget, nil).(*files.Symlink), int64(len(fieldDataBytes)))
		return f, nil
	case ufsData.Data_Metadata:
		return nil, fmt.Errorf("UnixFS Metadata unsupported")
	case ufsData.Data_HAMTShard, ufsData.Data_Directory:
		blk, err := blocks.NewBlockWithCid(blockData, c)
		if err != nil {
			return nil, fmt.Errorf("could not create block: %w", err)
		}
		dirRootNd, err := merkledag.ProtoNodeConverter(blk, pbn)
		if err != nil {
			return nil, fmt.Errorf("could not create dag-pb universal block from UnixFS directory root: %w", err)
		}
		pn, ok := dirRootNd.(*merkledag.ProtoNode)
		if !ok {
			return nil, fmt.Errorf("could not create dag-pb node from UnixFS directory root: %w", err)
		}

		dirDagSize, err := pn.Size()
		if err != nil {
			return nil, fmt.Errorf("could not get cumulative size from dag-pb node: %w", err)
		}

		switch fieldNum {
		case ufsData.Data_Directory:
			ch := make(chan unixfs.LinkResult, pbn.Links.Length())
			defer close(ch)
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
				ch <- lnk
			}
			return gateway.NewGetResponseFromDirectoryListing(dirDagSize, ch, nil), nil
		case ufsData.Data_HAMTShard:
			dirNd, err := unixfsnode.Reify(lctx, pbn, lsys)
			if err != nil {
				return nil, fmt.Errorf("could not reify sharded directory: %w", err)
			}

			d := &backpressuredHAMTDirIterNoRecursion{
				dagSize:   dirDagSize,
				linksItr:  dirNd.MapIterator(),
				dirCid:    c,
				lsys:      lsys,
				getLsys:   getLsys,
				ctx:       ctx,
				closed:    make(chan error),
				hasClosed: false,
			}
			return d, nil
		default:
			return nil, fmt.Errorf("not a basic or HAMT directory: should be unreachable")
		}
	case ufsData.Data_Raw, ufsData.Data_File:
		nd, err := unixfsnode.Reify(lctx, pbn, lsys)
		if err != nil {
			return nil, err
		}

		fnd, ok := nd.(datamodel.LargeBytesNode)
		if !ok {
			return nil, fmt.Errorf("could not process file since it did not present as large bytes")
		}
		f, err := fnd.AsLargeBytes()
		if err != nil {
			return nil, err
		}

		fileSize, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, fmt.Errorf("unable to get UnixFS file size: %w", err)
		}

		from := int64(0)
		if params.Range != nil {
			from = params.Range.From
		}
		_, err = f.Seek(from, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("unable to get reset UnixFS file reader: %w", err)
		}

		return &backpressuredFile{ctx: ctx, fileCid: c, size: fileSize, f: f, getLsys: getLsys, closed: make(chan error)}, nil
	default:
		return nil, fmt.Errorf("unknown UnixFS field type")
	}
}

type backpressuredHAMTDirIterNoRecursion struct {
	dagSize  uint64
	linksItr ipld.MapIterator
	dirCid   cid.Cid

	lsys    *ipld.LinkSystem
	getLsys lsysGetter
	ctx     context.Context

	curLnk       unixfs.LinkResult
	curProcessed int

	closed    chan error
	hasClosed bool
	err       error
}

func (it *backpressuredHAMTDirIterNoRecursion) AwaitClose() <-chan error {
	return it.closed
}

func (it *backpressuredHAMTDirIterNoRecursion) Link() unixfs.LinkResult {
	return it.curLnk
}

func (it *backpressuredHAMTDirIterNoRecursion) Next() bool {
	defer func() {
		if it.linksItr.Done() || it.err != nil {
			if !it.hasClosed {
				it.hasClosed = true
				close(it.closed)
			}
		}
	}()

	if it.err != nil {
		return false
	}

	iter := it.linksItr
	if iter.Done() {
		return false
	}

	/*
		Since there is no way to make a graph request for part of a HAMT during errors we can either fill in the HAMT with
		block requests, or we can re-request the HAMT and skip over the parts we already have.

		Here we choose the latter, however in the event of a re-request we request the entity rather than the entire DAG as
		a compromise between more requests and over-fetching data.
	*/

	var err error
	for {
		if it.ctx.Err() != nil {
			it.err = it.ctx.Err()
			return false
		}

		retry, processedErr := isRetryableError(err)
		if !retry {
			it.err = processedErr
			return false
		}

		var nd ipld.Node
		if err != nil {
			var lsys *ipld.LinkSystem
			lsys, err = it.getLsys(it.ctx, it.dirCid, gateway.CarParams{Scope: gateway.DagScopeEntity})
			if err != nil {
				continue
			}

			_, pbn, fieldData, _, _, ufsBaseErr := loadUnixFSBase(it.ctx, it.dirCid, nil, lsys)
			if ufsBaseErr != nil {
				err = ufsBaseErr
				continue
			}

			nd, err = hamt.NewUnixFSHAMTShard(it.ctx, pbn, fieldData, lsys)
			if err != nil {
				err = fmt.Errorf("could not reify sharded directory: %w", err)
				continue
			}

			iter = nd.MapIterator()
			for i := 0; i < it.curProcessed; i++ {
				_, _, err = iter.Next()
				if err != nil {
					continue
				}
			}

			it.linksItr = iter
		}

		var k, v ipld.Node
		k, v, err = iter.Next()
		if err != nil {
			retry, processedErr = isRetryableError(err)
			if retry {
				err = processedErr
				continue
			}
			it.err = processedErr
			return false
		}

		var name string
		name, err = k.AsString()
		if err != nil {
			it.err = err
			return false
		}
		var lnk ipld.Link
		lnk, err = v.AsLink()
		if err != nil {
			it.err = err
			return false
		}

		cl, ok := lnk.(cidlink.Link)
		if !ok {
			it.err = fmt.Errorf("link not a cidlink")
			return false
		}

		c := cl.Cid

		it.curLnk = unixfs.LinkResult{
			Link: &format.Link{
				Name: name,
				Size: 0,
				Cid:  c,
			},
		}
		it.curProcessed++
		break
	}

	return true
}

func (it *backpressuredHAMTDirIterNoRecursion) Err() error {
	return it.err
}

var _ AwaitCloser = (*backpressuredHAMTDirIterNoRecursion)(nil)

func (api *GraphGateway) GetAll(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, files.Node, error) {
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "all", "entityRanges": "0"}).Inc()
	return fetchWithPartialRetries(ctx, path, gateway.CarParams{Scope: gateway.DagScopeAll}, loadTerminalUnixFSElementWithRecursiveDirectories, api.metrics, api.fetchCAR)
}

type loadTerminalElement[T any] func(ctx context.Context, c cid.Cid, blk blocks.Block, lsys *ipld.LinkSystem, params gateway.CarParams, getLsys lsysGetter) (T, error)
type fetchCarFn = func(ctx context.Context, path gateway.ImmutablePath, params gateway.CarParams, cb DataCallback) error

type terminalPathType[T any] struct {
	resp T
	err  error
	md   gateway.ContentPathMetadata
}

type nextReq struct {
	c      cid.Cid
	params gateway.CarParams
}

func fetchWithPartialRetries[T any](ctx context.Context, path gateway.ImmutablePath, initialParams gateway.CarParams, resolveTerminalElementFn loadTerminalElement[T], metrics *GraphGatewayMetrics, fetchCAR fetchCarFn) (gateway.ContentPathMetadata, T, error) {
	var zeroReturnType T

	terminalPathElementCh := make(chan terminalPathType[T], 1)

	go func() {
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()

		hasSentAsyncData := false
		var closeCh <-chan error

		sendRequest := make(chan nextReq, 1)
		sendResponse := make(chan *ipld.LinkSystem, 1)
		getLsys := func(ctx context.Context, c cid.Cid, params gateway.CarParams) (*ipld.LinkSystem, error) {
			select {
			case sendRequest <- nextReq{c: c, params: params}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			select {
			case lsys := <-sendResponse:
				return lsys, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		p := ipfspath.FromString(path.String())
		params := initialParams

		err := fetchCAR(cctx, path, params, func(resource string, reader io.Reader) error {
			gb, err := carToLinearBlockGetter(cctx, reader, metrics)
			if err != nil {
				return err
			}

			lsys := getLinksystem(gb)

			if hasSentAsyncData {
				_, _, _, _, err = resolvePathToLastWithRoots(cctx, p, lsys)
				if err != nil {
					return err
				}

				select {
				case sendResponse <- lsys:
				case <-cctx.Done():
					return cctx.Err()
				}
			} else {
				// First resolve the path since we always need to.
				pathRootCids, terminalCid, remainder, terminalBlk, err := resolvePathWithRootsAndBlock(cctx, p, lsys)
				if err != nil {
					return err
				}
				md := contentMetadataFromRootsAndRemainder(p, pathRootCids, terminalCid, remainder)

				if len(remainder) > 0 {
					terminalPathElementCh <- terminalPathType[T]{err: errNotUnixFS}
					return nil
				}

				if hasSentAsyncData {
					select {
					case sendResponse <- lsys:
					case <-ctx.Done():
						return ctx.Err()
					}
				}

				nd, err := resolveTerminalElementFn(cctx, terminalCid, terminalBlk, lsys, params, getLsys)
				if err != nil {
					return err
				}

				ndAc, ok := any(nd).(AwaitCloser)
				if !ok {
					terminalPathElementCh <- terminalPathType[T]{
						resp: nd,
						md:   md,
					}
					return nil
				}

				hasSentAsyncData = true
				terminalPathElementCh <- terminalPathType[T]{
					resp: nd,
					md:   md,
				}

				closeCh = ndAc.AwaitClose()
			}

			select {
			case closeErr := <-closeCh:
				return closeErr
			case req := <-sendRequest:
				requestStr := fmt.Sprintf("/ipfs/%s?%s", req.c.String(), paramsToString(req.params))
				// set path and params for next iteration
				p = ipfspath.FromCid(req.c)
				params = req.params
				return &ErrPartialResponse{StillNeed: []string{requestStr}}
			case <-cctx.Done():
				return cctx.Err()
			}
		})

		if !hasSentAsyncData && err != nil {
			terminalPathElementCh <- terminalPathType[T]{err: err}
			return
		}

		if err != nil {
			lsys := getLinksystem(func(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
				return nil, multierror.Append(ErrFetcherUnexpectedEOF, format.ErrNotFound{Cid: cid})
			})
			for {
				select {
				case <-closeCh:
					return
				case <-sendRequest:
				case sendResponse <- lsys:
				case <-cctx.Done():
					return
				}
			}
		}
	}()

	select {
	case t := <-terminalPathElementCh:
		if t.err != nil {
			return gateway.ContentPathMetadata{}, zeroReturnType, t.err
		}
		return t.md, t.resp, nil
	case <-ctx.Done():
		return gateway.ContentPathMetadata{}, zeroReturnType, ctx.Err()
	}
}

func (api *GraphGateway) GetBlock(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, files.File, error) {
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "block", "entityRanges": "0"}).Inc()
	p := ipfspath.FromString(path.String())

	var md gateway.ContentPathMetadata
	var f files.File
	// TODO: if path is `/ipfs/cid`, we should use ?format=raw
	err := api.fetchCAR(ctx, path, gateway.CarParams{Scope: gateway.DagScopeBlock}, func(resource string, reader io.Reader) error {
		gb, err := carToLinearBlockGetter(ctx, reader, api.metrics)
		if err != nil {
			return err
		}
		lsys := getLinksystem(gb)

		// First resolve the path since we always need to.
		pathRoots, terminalCid, remainder, terminalBlk, err := resolvePathToLastWithRoots(ctx, p, lsys)
		if err != nil {
			return err
		}

		var blockData []byte
		if terminalBlk != nil {
			blockData = terminalBlk.RawData()
		} else {
			lctx := ipld.LinkContext{Ctx: ctx}
			lnk := cidlink.Link{Cid: terminalCid}
			blockData, err = lsys.LoadRaw(lctx, lnk)
			if err != nil {
				return err
			}
		}

		md = contentMetadataFromRootsAndRemainder(p, pathRoots, terminalCid, remainder)

		f = files.NewBytesFile(blockData)
		return nil
	})

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

	p := ipfspath.FromString(path.String())

	var md gateway.ContentPathMetadata
	var n *gateway.HeadResponse
	// TODO: fallback to dynamic fetches in case we haven't requested enough data
	rangeTo := int64(3071)
	err := api.fetchCAR(ctx, path, gateway.CarParams{Scope: gateway.DagScopeEntity, Range: &gateway.DagByteRange{From: 0, To: &rangeTo}}, func(resource string, reader io.Reader) error {
		gb, err := carToLinearBlockGetter(ctx, reader, api.metrics)
		if err != nil {
			return err
		}
		lsys := getLinksystem(gb)

		// First resolve the path since we always need to.
		pathRoots, terminalCid, remainder, terminalBlk, err := resolvePathWithRootsAndBlock(ctx, p, lsys)
		if err != nil {
			return err
		}

		md = contentMetadataFromRootsAndRemainder(p, pathRoots, terminalCid, remainder)

		lctx := ipld.LinkContext{Ctx: ctx}
		pathTerminalCidLink := cidlink.Link{Cid: terminalCid}

		// Load the block at the root of the terminal path element
		dataBytes := terminalBlk.RawData()

		// It's not UnixFS if there is a remainder or it's not dag-pb
		if len(remainder) > 0 || terminalCid.Type() != uint64(multicodec.DagPb) {
			n = gateway.NewHeadResponseForFile(files.NewBytesFile(dataBytes), int64(len(dataBytes)))
			return nil
		}

		// Let's figure out if the terminal element is valid UnixFS and if so what kind
		np, err := api.pc(pathTerminalCidLink, lctx)
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
				dirRootNd, err := merkledag.ProtoNodeConverter(terminalBlk, lastCidNode)
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

	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}

	return md, n, nil
}

func (api *GraphGateway) ResolvePath(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, error) {
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "block", "entityRanges": "0"}).Inc()

	var md gateway.ContentPathMetadata
	err := api.fetchCAR(ctx, path, gateway.CarParams{Scope: gateway.DagScopeBlock}, func(resource string, reader io.Reader) error {
		gb, err := carToLinearBlockGetter(ctx, reader, api.metrics)
		if err != nil {
			return err
		}
		lsys := getLinksystem(gb)

		// First resolve the path since we always need to.
		p := ipfspath.FromString(path.String())
		pathRoots, terminalCid, remainder, _, err := resolvePathToLastWithRoots(ctx, p, lsys)
		if err != nil {
			return err
		}

		md = contentMetadataFromRootsAndRemainder(p, pathRoots, terminalCid, remainder)

		return nil
	})

	if err != nil {
		return gateway.ContentPathMetadata{}, err
	}

	return md, nil
}

func (api *GraphGateway) GetCAR(ctx context.Context, path gateway.ImmutablePath, params gateway.CarParams) (gateway.ContentPathMetadata, io.ReadCloser, error) {
	numRanges := "0"
	if params.Range != nil {
		numRanges = "1"
	}
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": string(params.Scope), "entityRanges": numRanges}).Inc()
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
		err = api.fetchCAR(ctx, path, params, func(resource string, reader io.Reader) error {
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
			l := getLinksystem(teeBlock)

			// First resolve the path since we always need to.
			_, terminalCid, remainder, terminalBlk, err := resolvePathWithRootsAndBlock(ctx, p, l)
			if err != nil {
				return err
			}
			if len(remainder) > 0 {
				return nil
			}

			if cw == nil {
				cw, err = storage.NewWritable(w, []cid.Cid{terminalCid}, carv2.WriteAsCarV1(true))
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
			}

			err = walkGatewaySimpleSelector(ctx, terminalBlk, params, l)
			if err != nil {
				return err
			}
			return nil
		})

		_ = w.CloseWithError(err)
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
	retry, processedErr := isRetryableError(err)
	if retry {
		return processedErr
	}
	*e = processedErr
	return nil
}

func isRetryableError(err error) (bool, error) {
	if errors.Is(err, ErrFetcherUnexpectedEOF) {
		return false, err
	}

	if format.IsNotFound(err) {
		return true, err
	}
	initialErr := err

	// Checks if err is of a type that does not implement the .Is interface and
	// cannot be directly compared to. Therefore, errors.Is cannot be used.
	for {
		_, ok := err.(resolver.ErrNoLink)
		if ok {
			return false, err
		}

		_, ok = err.(datamodel.ErrWrongKind)
		if ok {
			return false, err
		}

		_, ok = err.(datamodel.ErrNotExists)
		if ok {
			return false, err
		}

		errNoSuchField, ok := err.(schema.ErrNoSuchField)
		if ok {
			// Convert into a more general error type so the gateway code can know what this means
			// TODO: Have either a more generally usable error type system for IPLD errors (e.g. a base type indicating that data cannot exist)
			// or at least have one that is specific to the gateway consumer and part of the Backend contract instead of this being implicit
			err = datamodel.ErrNotExists{Segment: errNoSuchField.Field}
			return false, err
		}

		err = errors.Unwrap(err)
		if err == nil {
			return true, initialErr
		}
	}
}

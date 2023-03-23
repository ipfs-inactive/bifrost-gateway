package main

import (
	"bytes"
	"context"
	"fmt"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/go-libipfs/gateway"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-path"
	"github.com/ipfs/go-path/resolver"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"io"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
)

func makeMetricsHandler(port int) (*http.Server, error) {
	mux := http.NewServeMux()

	gatherers := prometheus.Gatherers{
		prometheus.DefaultGatherer,
	}
	options := promhttp.HandlerOpts{}
	mux.Handle("/debug/metrics/prometheus", promhttp.HandlerFor(gatherers, options))

	return &http.Server{
		Handler: mux,
		Addr:    ":" + strconv.Itoa(port),
	}, nil
}

func withRequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goLog.Infow(r.Method, "url", r.URL, "host", r.Host)
		// TODO: if debug is enabled, show more? goLog.Infow("request received", "url", r.URL, "host", r.Host, "method", r.Method, "ua", r.UserAgent(), "referer", r.Referer())
		next.ServeHTTP(w, r)
	})
}

var noModtime = time.Unix(0, 0)

func makeGatewayCARHandler(bsrv blockservice.BlockService, port int) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ipfs/", func(w http.ResponseWriter, r *http.Request) {
		// the hour is a hard fallback, we don't expect it to happen, but just in case
		ctx, cancel := context.WithTimeout(r.Context(), time.Hour)
		defer cancel()
		r = r.WithContext(ctx)

		defer func() {
			if r := recover(); r != nil {
				goLog.Error("A panic occurred in the gateway handler!")
				goLog.Error(r)
				debug.PrintStack()
			}
		}()

		if r.Method != http.MethodGet {
			w.Header().Add("Allow", http.MethodGet)

			errmsg := "Method " + r.Method + " not allowed"
			http.Error(w, errmsg, http.StatusMethodNotAllowed)
			return
		}

		isCar := false
		if formatParam := r.URL.Query().Get("format"); formatParam != "" {
			isCar = formatParam == "car"
			if !isCar {
				http.Error(w, "only car format supported", http.StatusBadRequest)
				return
			}
		} else {
			for _, header := range r.Header.Values("Accept") {
				for _, value := range strings.Split(header, ",") {
					accept := strings.TrimSpace(value)
					if accept == "application/vnd.ipld.car" {
						isCar = true
						break
					}
				}
			}
		}
		if !isCar {
			http.Error(w, "only car format supported", http.StatusBadRequest)
			return
		}

		contentPath := ipath.New(r.URL.Path)
		if contentPath.Mutable() {
			http.Error(w, "only immutable block requests supported", http.StatusBadRequest)
			return
		} else if contentPath.Namespace() != "ipfs" {
			http.Error(w, "only the ipfs names is supported", http.StatusBadRequest)
			return
		}

		carStream, err := simpleSelectorToCar(ctx, bsrv, contentPath.String(), r.URL.Query())
		if err != nil {
			http.Error(w, "only the ipfs names is supported", http.StatusBadRequest)
			return
		}

		const immutableCacheControl = "public, max-age=29030400, immutable"
		// immutable! CACHE ALL THE THINGS, FOREVER! wolololol
		w.Header().Set("Cache-Control", immutableCacheControl)

		// Set modtime to 'zero time' to disable Last-Modified header (superseded by Cache-Control)

		io.Copy(w, carStream)
		return
	})

	// Creates metrics handler for total response size. Matches the same metrics
	// from Kubo:
	// https://github.com/ipfs/kubo/blob/e550d9e4761ea394357c413c02ade142c0dea88c/core/corehttp/metrics.go#L79-L152
	sum := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "ipfs",
		Subsystem:  "http",
		Name:       "response_size_bytes",
		Help:       "The HTTP response sizes in bytes.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, nil)
	err := prometheus.Register(sum)
	if err != nil {
		return nil, err
	}

	// Construct the HTTP handler for the gateway.
	handler := promhttp.InstrumentHandlerResponseSize(sum, mux)

	// Add logging
	handler = withRequestLogger(handler)

	return &http.Server{
		Handler: handler,
		Addr:    ":" + strconv.Itoa(port),
	}, nil
}

func simpleSelectorToCar(ctx context.Context, bsrv blockservice.BlockService, p string, params url.Values) (io.ReadCloser, error) {
	pathSegs := strings.Split(p, "/")
	if len(pathSegs) < 3 || !(pathSegs[0] == "" && pathSegs[1] == "ipfs") {
		return nil, fmt.Errorf("invalid path")
	}
	pathSegs = pathSegs[2:]
	rootCidStr := pathSegs[0]
	rootCid, err := cid.Decode(rootCidStr)
	if err != nil {
		return nil, err
	}

	ipfspath, err := path.ParsePath(p)
	if err != nil {
		return nil, err
	}

	r, w := io.Pipe()
	// Setup header for the output car
	err = car.WriteHeader(&car.CarHeader{
		Roots:   []cid.Cid{rootCid},
		Version: 1,
	}, w)
	if err != nil {
		return nil, fmt.Errorf("writing car header: %w", err)
	}

	rangeStr, hasRange := params.Get("bytes"), params.Has("bytes")
	depthStr, hasDepth := params.Get("depth"), params.Has("depth")

	if hasDepth && !(depthStr == "0" || depthStr == "1" || depthStr == "all") {
		return nil, fmt.Errorf("depth type: %s not supported", depthStr)
	}
	var getRange *gateway.GetRange
	if hasRange {
		getRange, err = rangeStrToGetRange(rangeStr)
		if err != nil {
			return nil, err
		}
	}

	go func() {
		defer w.Close()
		blockGetter := merkledag.NewDAGService(bsrv).Session(ctx)
		blockGetter = &nodeGetterToCarExporer{
			ng:    blockGetter,
			w:     w,
			mhSet: make(map[string]struct{}),
		}
		dsrv := merkledag.NewReadOnlyDagService(blockGetter)

		// Setup the UnixFS resolver.
		fetcherConfig := bsfetcher.NewFetcherConfig(bsrv)
		fetcherConfig.PrototypeChooser = dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
			if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
				return tlnkNd.LinkTargetNodePrototype(), nil
			}
			return basicnode.Prototype.Any, nil
		})
		fetcher := fetcherConfig.WithReifier(unixfsnode.Reify)
		r := resolver.NewBasicResolver(fetcher)

		lastCid, remainder, err := r.ResolveToLastNode(ctx, ipfspath)
		if err != nil {
			goLog.Error(err)
			return
		}

		if hasDepth && depthStr == "0" {
			return
		}

		lastCidNode, err := dsrv.Get(ctx, lastCid)
		if err != nil {
			goLog.Error(err)
			return
		}

		ufsNode, err := unixfile.NewUnixfsFile(ctx, dsrv, lastCidNode)
		if err != nil {
			// It's not UnixFS

			// If it's all fetch the graph recursively
			if depthStr == "all" {
				if err := merkledag.FetchGraph(ctx, lastCid, dsrv); err != nil {
					goLog.Error(err)
				}
				return
			}

			//if not then either this is an error (which we can't report) or this is the last block for us to return
			return
		}
		if f, ok := ufsNode.(files.File); ok {
			if len(remainder) > 0 {
				// this is an error, so we're done
				return
			}
			if hasRange {
				// TODO: testing + check off by one errors
				var numToRead int64
				if *getRange.To < 0 {
					size, err := f.Seek(0, io.SeekEnd)
					if err != nil {
						return
					}
					numToRead = (size - *getRange.To) - int64(getRange.From)
				} else {
					numToRead = int64(getRange.From) - *getRange.To
				}

				if _, err := f.Seek(int64(getRange.From), io.SeekStart); err != nil {
					return
				}
				_, _ = io.CopyN(io.Discard, f, numToRead)
				return
			}
		} else if d, ok := ufsNode.(files.Directory); ok {
			if depthStr == "1" {
				for d.Entries().Next() {
				}
				return
			}
			if depthStr == "all" {
				// TODO: being lazy here
				w, err := files.NewTarWriter(io.Discard)
				if err != nil {
					goLog.Error(fmt.Errorf("could not create tar write %w", err))
					return
				}
				if err := w.WriteFile(d, "tmp"); err != nil {
					goLog.Error(err)
					return
				}
				return
			}
		} else {
			return
		}
	}()
	return r, nil
}

type nodeGetterToCarExporer struct {
	ng format.NodeGetter
	w  io.Writer

	lk    sync.RWMutex
	mhSet map[string]struct{}
}

func (n *nodeGetterToCarExporer) Get(ctx context.Context, c cid.Cid) (format.Node, error) {
	nd, err := n.ng.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	if err := n.trySendBlock(nd); err != nil {
		return nil, err
	}

	return nd, nil
}

func (n *nodeGetterToCarExporer) GetMany(ctx context.Context, cids []cid.Cid) <-chan *format.NodeOption {
	ndCh := n.ng.GetMany(ctx, cids)
	outCh := make(chan *format.NodeOption)
	go func() {
		defer close(outCh)
		for nd := range ndCh {
			if nd.Err == nil {
				if err := n.trySendBlock(nd.Node); err != nil {
					select {
					case outCh <- &format.NodeOption{Err: err}:
					case <-ctx.Done():
					}
					return
				}
				select {
				case outCh <- nd:
				case <-ctx.Done():
				}
			}
		}
	}()
	return outCh
}

func (n *nodeGetterToCarExporer) trySendBlock(block blocks.Block) error {
	h := string(block.Cid().Hash())
	n.lk.RLock()
	_, found := n.mhSet[h]
	n.lk.RUnlock()
	if !found {
		doSend := false
		n.lk.Lock()
		_, found := n.mhSet[h]
		if !found {
			doSend = true
			n.mhSet[h] = struct{}{}
		}
		n.lk.Unlock()
		if doSend {
			err := util.LdWrite(n.w, block.Cid().Bytes(), block.RawData()) // write to the output car
			if err != nil {
				return fmt.Errorf("writing to output car: %w", err)
			}
		}
	}
	return nil
}

var _ format.NodeGetter = (*nodeGetterToCarExporer)(nil)

func rangeStrToGetRange(rangeStr string) (*gateway.GetRange, error) {
	rangeElems := strings.Split(rangeStr, ":")
	if len(rangeElems) > 2 {
		return nil, fmt.Errorf("invalid range")
	}
	first, err := strconv.ParseUint(rangeElems[0], 10, 64)
	if err != nil {
		return nil, err
	}

	if rangeElems[1] == "*" {
		return &gateway.GetRange{
			From: first,
			To:   nil,
		}, nil
	}

	second, err := strconv.ParseInt(rangeElems[1], 10, 64)
	if err != nil {
		return nil, err
	}

	if second < 0 {
		// TODO: fix, might also require a fix in boxo/gateway
		return nil, fmt.Errorf("unsupported")
	}

	if uint64(second) < first {
		return nil, fmt.Errorf("invalid range")
	}

	return &gateway.GetRange{
		From: first,
		To:   &second,
	}, nil
}

func makeGatewayBlockHandler(bsrv blockservice.BlockService, port int) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ipfs/", func(w http.ResponseWriter, r *http.Request) {
		// the hour is a hard fallback, we don't expect it to happen, but just in case
		ctx, cancel := context.WithTimeout(r.Context(), time.Hour)
		defer cancel()
		r = r.WithContext(ctx)

		defer func() {
			if r := recover(); r != nil {
				goLog.Error("A panic occurred in the gateway handler!")
				goLog.Error(r)
				debug.PrintStack()
			}
		}()

		if r.Method != http.MethodGet {
			w.Header().Add("Allow", http.MethodGet)

			errmsg := "Method " + r.Method + " not allowed"
			http.Error(w, errmsg, http.StatusMethodNotAllowed)
			return
		}

		isBlock := false
		if formatParam := r.URL.Query().Get("format"); formatParam != "" {
			isBlock = formatParam == "raw"
			if !isBlock {
				http.Error(w, "only raw format supported", http.StatusBadRequest)
				return
			}
		} else {
			for _, header := range r.Header.Values("Accept") {
				for _, value := range strings.Split(header, ",") {
					accept := strings.TrimSpace(value)
					if accept == "application/vnd.ipld.raw" {
						isBlock = true
						break
					}
				}
			}
		}
		if !isBlock {
			http.Error(w, "only raw format supported", http.StatusBadRequest)
			return
		}

		contentPath := ipath.New(r.URL.Path)
		if contentPath.Mutable() {
			http.Error(w, "only immutable block requests supported", http.StatusBadRequest)
			return
		} else if contentPath.Namespace() != "ipfs" {
			http.Error(w, "only the ipfs names is supported", http.StatusBadRequest)
			return
		}

		strComps := strings.Split(strings.TrimRight(contentPath.String(), "/"), "/")
		if len(strComps) != 3 {
			http.Error(w, "requests must be for single raw blocks", http.StatusBadRequest)
			return
		}
		c, err := cid.Decode(strComps[2])
		if err != nil {
			http.Error(w, fmt.Sprintf("not a valid cid %s", strComps[2]), http.StatusBadRequest)
			return
		}

		blk, err := bsrv.GetBlock(r.Context(), c)
		if err != nil {
			http.Error(w, fmt.Sprintf("could not get cid %s", c), http.StatusInternalServerError)
			return
		}

		const immutableCacheControl = "public, max-age=29030400, immutable"
		// immutable! CACHE ALL THE THINGS, FOREVER! wolololol
		w.Header().Set("Cache-Control", immutableCacheControl)

		// Set modtime to 'zero time' to disable Last-Modified header (superseded by Cache-Control)

		http.ServeContent(w, r, c.String()+".bin", noModtime, bytes.NewReader(blk.RawData()))
		return
	})

	// Creates metrics handler for total response size. Matches the same metrics
	// from Kubo:
	// https://github.com/ipfs/kubo/blob/e550d9e4761ea394357c413c02ade142c0dea88c/core/corehttp/metrics.go#L79-L152
	sum := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "ipfs",
		Subsystem:  "http",
		Name:       "response_size_bytes",
		Help:       "The HTTP response sizes in bytes.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, nil)
	err := prometheus.Register(sum)
	if err != nil {
		return nil, err
	}

	// Construct the HTTP handler for the gateway.
	handler := promhttp.InstrumentHandlerResponseSize(sum, mux)

	// Add logging
	handler = withRequestLogger(handler)

	return &http.Server{
		Handler: handler,
		Addr:    ":" + strconv.Itoa(port),
	}, nil
}

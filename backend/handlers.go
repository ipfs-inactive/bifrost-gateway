package main

import (
	"bytes"
	"context"
	"fmt"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"io"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
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
				http.Error(w, "only raw format supported", http.StatusBadRequest)
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

		carStream, err := simpleSelectorToCar(contentPath)
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

func simpleSelectorToCar(ipfsPath ipath.Path) (io.ReadCloser, error) {
	pathSegs := strings.Split(ipfsPath.String(), "/")
	if len(pathSegs) < 3 || !(pathSegs[0] == "" && pathSegs[1] == "ipfs") {
		return nil, fmt.Errorf("invalid path")
	}
	pathSegs = pathSegs[2:]
	rootCidStr := pathSegs[0]
	rootCid, err := cid.Decode(rootCidStr)
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

	go func() {
		defer w.Close()
		remainingPath := pathSegs[1:]
		unixfile.NewUnixfsFile()

		err = util.LdWrite(os.Stdout, block.Cid().Bytes(), block.RawData()) // write to the output car
		if err != nil {
			return fmt.Errorf("writing to output car: %w", err)
		}
	}()
	_ = rootCid
	return r, nil
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

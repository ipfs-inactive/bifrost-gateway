package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"time"

	_ "net/http/pprof"

	"github.com/ipfs/bifrost-gateway/lib"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/boxo/blockservice"
	bstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/gateway"
	servertiming "github.com/mitchellh/go-server-timing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func makeMetricsAndDebuggingHandler(port int) (*http.Server, error) {
	mux := http.NewServeMux()

	gatherers := prometheus.Gatherers{
		prometheus.DefaultGatherer,
		caboose.CabooseMetrics,
	}
	options := promhttp.HandlerOpts{}
	mux.Handle("/debug/metrics/prometheus", promhttp.HandlerFor(gatherers, options))
	mux.Handle("/debug/vars", http.DefaultServeMux)
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.Handle("/debug/stack", http.DefaultServeMux)
	MutexFractionOption("/debug/pprof-mutex/", mux)
	BlockProfileRateOption("/debug/pprof-block/", mux)

	return &http.Server{
		Handler: mux,
		Addr:    ":" + strconv.Itoa(port),
	}, nil
}

func withConnect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ServeMux does not support requests with CONNECT method,
		// so we need to handle them separately
		// https://golang.org/src/net/http/request.go#L111
		if r.Method == http.MethodConnect {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func withRequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goLog.Infow(r.Method, "url", r.URL, "host", r.Host)
		// TODO: if debug is enabled, show more? goLog.Infow("request received", "url", r.URL, "host", r.Host, "method", r.Method, "ua", r.UserAgent(), "referer", r.Referer())
		next.ServeHTTP(w, r)
	})
}

func makeGatewayHandler(bs bstore.Blockstore, kuboRPC []string, port int, blockCacheSize int, cdns *cachedDNS, useGraphBackend bool) (*http.Server, error) {
	// Sets up the routing system, which will proxy the IPNS routing requests to the given gateway.
	routing := newProxyRouting(kuboRPC, cdns)

	// Sets up a cache to store blocks in
	cacheBlockStore, err := lib.NewCacheBlockStore(blockCacheSize)
	if err != nil {
		return nil, err
	}

	// Set up support for identity hashes (https://github.com/ipfs/bifrost-gateway/issues/38)
	cacheBlockStore = bstore.NewIdStore(cacheBlockStore)

	var gwAPI gateway.IPFSBackend
	if !useGraphBackend {
		// Sets up an exchange based on the given Block Store
		exch, err := newExchange(bs)
		if err != nil {
			return nil, err
		}

		// Sets up a blockservice which tries the cache and falls back to the exchange
		blockService := blockservice.New(cacheBlockStore, exch)

		// Creates the gateway with the block service and the routing.
		gwAPI, err = gateway.NewBlocksGateway(blockService, gateway.WithValueStore(routing))
		if err != nil {
			return nil, err
		}
	} else {
		// Sets up an exchange based on the given Block Store
		exch, err := newExchange(bs)
		if err != nil {
			return nil, err
		}

		gwAPI, err = lib.NewGraphGatewayBackend(bs.(lib.CarFetcher), exch, lib.WithValueStore(routing), lib.WithBlockstore(cacheBlockStore))
		if err != nil {
			return nil, err
		}
	}

	headers := map[string][]string{}
	gateway.AddAccessControlHeaders(headers)

	gwConf := gateway.Config{
		Headers: headers,
	}

	gwHandler := gateway.NewHandler(gwConf, gwAPI)
	mux := http.NewServeMux()
	mux.Handle("/ipfs/", gwHandler)
	mux.Handle("/ipns/", gwHandler)
	mux.Handle("/api/v0/", newKuboRPCHandler(kuboRPC))

	// Note: in the future we may want to make this more configurable.
	noDNSLink := false
	publicGateways := map[string]*gateway.Specification{
		"localhost": {
			Paths:         []string{"/ipfs", "/ipns"},
			NoDNSLink:     noDNSLink,
			UseSubdomains: true,
		},
		"dweb.link": {
			Paths:         []string{"/ipfs", "/ipns"},
			NoDNSLink:     noDNSLink,
			UseSubdomains: true,
		},
	}

	// If we're doing tests, ensure the right public gateways are enabled.
	if os.Getenv("GATEWAY_CONFORMANCE_TEST") == "true" {
		publicGateways["example.com"] = &gateway.Specification{
			Paths:         []string{"/ipfs", "/ipns"},
			NoDNSLink:     noDNSLink,
			UseSubdomains: true,
		}
	}

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
	err = prometheus.Register(sum)
	if err != nil {
		return nil, err
	}

	// Construct the HTTP handler for the gateway.
	handler := withConnect(mux)
	handler = http.Handler(gateway.WithHostname(handler, gwAPI, publicGateways, noDNSLink))
	handler = servertiming.Middleware(handler, nil)
	handler = promhttp.InstrumentHandlerResponseSize(sum, handler)

	// Add logging.
	handler = withRequestLogger(handler)

	// Add tracing.
	handler = otelhttp.NewHandler(handler, "Gateway")

	return &http.Server{
		Handler: handler,
		Addr:    ":" + strconv.Itoa(port),
	}, nil
}

func newKuboRPCHandler(endpoints []string) http.Handler {
	mux := http.NewServeMux()

	// Endpoints that can be redirected to the gateway itself as they can be handled
	// by the path gateway. We use 303 See Other here to ensure that the API requests
	// are transformed to GET requests to the gateway.
	// - https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/303
	redirectToGateway := func(format string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			path := path.New(r.URL.Query().Get("arg"))
			url := path.String()
			if format != "" {
				url += "?format=" + format
			}

			goLog.Debugw("api request redirected to gateway", "url", r.URL, "redirect", url)
			http.Redirect(w, r, url, http.StatusSeeOther)
		}
	}

	mux.HandleFunc("/api/v0/cat", redirectToGateway(""))
	mux.HandleFunc("/api/v0/dag/export", redirectToGateway("car"))
	mux.HandleFunc("/api/v0/block/get", redirectToGateway("raw"))
	mux.HandleFunc("/api/v0/dag/get", func(w http.ResponseWriter, r *http.Request) {
		path := path.New(r.URL.Query().Get("arg"))
		codec := r.URL.Query().Get("output-codec")
		if codec == "" {
			codec = "dag-json"
		}
		url := fmt.Sprintf("%s?format=%s", path.String(), codec)
		goLog.Debugw("api request redirected to gateway", "url", r.URL, "redirect", url)
		http.Redirect(w, r, url, http.StatusSeeOther)
	})

	// Endpoints that have high traffic volume. We will keep redirecting these
	// for now to Kubo endpoints that are able to handle these requests. We use
	// 307 Temporary Redirect in order to preserve the original HTTP Method.
	// - https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/307
	s := rand.NewSource(time.Now().Unix())
	rand := rand.New(s)
	redirectToKubo := func(w http.ResponseWriter, r *http.Request) {
		// Naively choose one of the Kubo RPC clients.
		endpoint := endpoints[rand.Intn(len(endpoints))]
		url := endpoint + r.URL.Path + "?" + r.URL.RawQuery
		goLog.Debugw("api request redirected to kubo", "url", r.URL, "redirect", url)
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
	}

	mux.HandleFunc("/api/v0/name/resolve", redirectToKubo)
	mux.HandleFunc("/api/v0/resolve", redirectToKubo)
	mux.HandleFunc("/api/v0/dag/resolve", redirectToKubo)
	mux.HandleFunc("/api/v0/dns", redirectToKubo)

	// Remaining requests to the API receive a 501, as well as an explanation.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotImplemented)
		goLog.Debugw("api request returned 501", "url", r.URL)
		w.Write([]byte("The /api/v0 Kubo RPC is now discontinued on this server as it is not part of the gateway specification. If you need this API, please self-host a Kubo instance yourself: https://docs.ipfs.tech/install/command-line/"))
	})

	return mux
}

// MutexFractionOption allows to set runtime.SetMutexProfileFraction via HTTP
// using POST request with parameter 'fraction'.
func MutexFractionOption(path string, mux *http.ServeMux) *http.ServeMux {
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		asfr := r.Form.Get("fraction")
		if len(asfr) == 0 {
			http.Error(w, "parameter 'fraction' must be set", http.StatusBadRequest)
			return
		}

		fr, err := strconv.Atoi(asfr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		runtime.SetMutexProfileFraction(fr)
	})

	return mux
}

// BlockProfileRateOption allows to set runtime.SetBlockProfileRate via HTTP
// using POST request with parameter 'rate'.
// The profiler tries to sample 1 event every <rate> nanoseconds.
// If rate == 1, then the profiler samples every blocking event.
// To disable, set rate = 0.
func BlockProfileRateOption(path string, mux *http.ServeMux) *http.ServeMux {
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		rateStr := r.Form.Get("rate")
		if len(rateStr) == 0 {
			http.Error(w, "parameter 'rate' must be set", http.StatusBadRequest)
			return
		}

		rate, err := strconv.Atoi(rateStr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		runtime.SetBlockProfileRate(rate)
	})
	return mux
}

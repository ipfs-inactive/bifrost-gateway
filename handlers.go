package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-libipfs/gateway"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func makeMetricsHandler(port int) (*http.Server, error) {
	mux := http.NewServeMux()

	gatherers := prometheus.Gatherers{
		prometheus.DefaultGatherer,
		caboose.CabooseMetrics,
	}
	options := promhttp.HandlerOpts{}
	mux.Handle("/debug/metrics/prometheus", promhttp.HandlerFor(gatherers, options))

	return &http.Server{
		Handler: mux,
		Addr:    ":" + strconv.Itoa(port),
	}, nil
}

func makeGatewayHandler(saturnOrchestrator, saturnLogger string, kuboRPC []string, port int, blockCacheSize int) (*http.Server, error) {
	// Sets up an exchange based on using Saturn as block storage
	exch, err := newExchange(saturnOrchestrator, saturnLogger)
	if err != nil {
		return nil, err
	}

	// Sets up an LRU cache to store blocks in
	cacheBlockStore, err := newCacheBlockStore(blockCacheSize)
	if err != nil {
		return nil, err
	}

	// Sets up a blockservice which tries the LRU cache and falls back to the exchange
	blockService := blockservice.New(cacheBlockStore, exch)

	// Sets up the routing system, which will proxy the IPNS routing requests to the given gateway.
	routing := newProxyRouting(kuboRPC)

	// Creates the gateway with the block service and the routing.
	gwAPI, err := newBifrostGateway(blockService, routing)
	if err != nil {
		return nil, err
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
	handler := http.Handler(gateway.WithHostname(mux, gwAPI, publicGateways, noDNSLink))
	handler = promhttp.InstrumentHandlerResponseSize(sum, handler)

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
		http.Redirect(w, r, endpoint+r.URL.Path+"?"+r.URL.RawQuery, http.StatusTemporaryRedirect)
	}

	mux.HandleFunc("/api/v0/name/resolve", redirectToKubo)
	mux.HandleFunc("/api/v0/resolve", redirectToKubo)
	mux.HandleFunc("/api/v0/dag/resolve", redirectToKubo)
	mux.HandleFunc("/api/v0/dns", redirectToKubo)

	// Remaining requests to the API receive a 501, as well as an explanation.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotImplemented)
		w.Write([]byte("The /api/v0 Kubo RPC is now discontinued on this server as it is not part of the gateway specification. If you need this API, please self-host a Kubo instance yourself: https://docs.ipfs.tech/install/command-line/"))
	})

	return mux
}

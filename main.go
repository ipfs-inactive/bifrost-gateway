package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-blockservice"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-libipfs/gateway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().String("saturn-orchestrator", "", "url of the saturn orchestrator endpoint")
	rootCmd.Flags().String("saturn-logger", "", "url of the saturn logging endpoint")
	rootCmd.Flags().StringSlice("kubo-rpc", []string{}, "Kubo RPC nodes that will handle /api/v0 requests (can be set multiple times)")
	rootCmd.Flags().Int("gateway-port", 8080, "gateway port")
	rootCmd.Flags().Int("metrics-port", 8040, "metrics port")

	rootCmd.MarkFlagRequired("saturn-orchestrator")
	rootCmd.MarkFlagRequired("saturn-logger")
	rootCmd.MarkFlagRequired("kubo-rpc")
}

var rootCmd = &cobra.Command{
	Use:               "bifrost-gateway",
	CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	Short:             "IPFS Gateway implementation for https://github.com/protocol/bifrost-infra",
	RunE: func(cmd *cobra.Command, args []string) error {
		saturnOrchestrator, _ := cmd.Flags().GetString("saturn-orchestrator")
		saturnLogger, _ := cmd.Flags().GetString("saturn-logger")
		kuboRPC, _ := cmd.Flags().GetStringSlice("kubo-rpc")
		gatewayPort, _ := cmd.Flags().GetInt("gateway-port")
		metricsPort, _ := cmd.Flags().GetInt("metrics-port")

		gatewaySrv, err := makeGatewayHandler(saturnOrchestrator, saturnLogger, kuboRPC, gatewayPort)
		if err != nil {
			return err
		}

		metricsSrv, err := makeMetricsHandler(metricsPort)
		if err != nil {
			return err
		}

		quit := make(chan os.Signal, 1)
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()

			log.Printf("Path gateway listening on http://127.0.0.1:%d", gatewayPort)
			log.Printf("Subdomain gateway listening on dweb.link and http://localhost:%d", gatewayPort)
			log.Printf("Legacy RPC at /api/v0 provided by %s", strings.Join(kuboRPC, " "))
			err := gatewaySrv.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("Failed to start gateway: %s", err)
				quit <- os.Interrupt
			}
		}()

		go func() {
			defer wg.Done()
			log.Printf("Metrics exposed at http://127.0.0.1:%d/debug/metrics/prometheus", metricsPort)
			err := metricsSrv.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("Failed to start metrics: %s", err)
				quit <- os.Interrupt
			}
		}()

		signal.Notify(quit, os.Interrupt)
		<-quit
		log.Printf("Closing servers...")
		go gatewaySrv.Close()
		go metricsSrv.Close()
		wg.Wait()
		return nil
	},
}

func makeGatewayHandler(saturnOrchestrator, saturnLogger string, kuboRPC []string, port int) (*http.Server, error) {
	blockStore, err := newBlockStore(saturnOrchestrator, saturnLogger)
	if err != nil {
		return nil, err
	}

	// Sets up an offline (no exchange) blockService based on the Saturn block store.
	blockService := blockservice.New(blockStore, offline.Exchange(blockStore))

	// // Sets up the routing system, which will proxy the IPNS routing requests to the given gateway.
	routing := newProxyRouting(kuboRPC, nil)

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
	mux.Handle("/api/v0/", newAPIHandler(kuboRPC))

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

	return &http.Server{
		Handler: gateway.WithHostname(mux, gwAPI, publicGateways, noDNSLink),
		Addr:    ":" + strconv.Itoa(port),
	}, nil
}

func makeMetricsHandler(port int) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.Handle("/debug/metrics/prometheus", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))

	return &http.Server{
		Handler: mux,
		Addr:    ":" + strconv.Itoa(port),
	}, nil
}

func newAPIHandler(endpoints []string) http.Handler {
	mux := http.NewServeMux()

	// Endpoints that can be redirected to the gateway itself as they can be handled
	// by the path gateway.
	mux.HandleFunc("/api/v0/cat", func(w http.ResponseWriter, r *http.Request) {
		cid := r.URL.Query().Get("arg")
		url := fmt.Sprintf("/ipfs/%s", cid)
		http.Redirect(w, r, url, http.StatusFound)
	})

	mux.HandleFunc("/api/v0/dag/get", func(w http.ResponseWriter, r *http.Request) {
		cid := r.URL.Query().Get("arg")
		codec := r.URL.Query().Get("output-codec")
		if codec == "" {
			codec = "dag-json"
		}
		url := fmt.Sprintf("/ipfs/%s?format=%s", cid, codec)
		http.Redirect(w, r, url, http.StatusFound)
	})

	mux.HandleFunc("/api/v0/dag/export", func(w http.ResponseWriter, r *http.Request) {
		cid := r.URL.Query().Get("arg")
		url := fmt.Sprintf("/ipfs/%s?format=car", cid)
		http.Redirect(w, r, url, http.StatusFound)
	})

	mux.HandleFunc("/api/v0/block/get", func(w http.ResponseWriter, r *http.Request) {
		cid := r.URL.Query().Get("arg")
		url := fmt.Sprintf("/ipfs/%s?format=raw", cid)
		http.Redirect(w, r, url, http.StatusFound)
	})

	// Endpoints that have high traffic volume. We will keep redirecting these
	// for now to Kubo endpoints that are able to handle these requests.
	s := rand.NewSource(time.Now().Unix())
	rand := rand.New(s)
	redirectToKubo := func(w http.ResponseWriter, r *http.Request) {
		// Naively choose one of the Kubo RPC clients.
		endpoint := endpoints[rand.Intn(len(endpoints))]
		http.Redirect(w, r, endpoint+r.URL.Path+"?"+r.URL.RawQuery, http.StatusFound)
	}

	mux.HandleFunc("/api/v0/name/resolve", redirectToKubo)
	mux.HandleFunc("/api/v0/resolve", redirectToKubo)
	mux.HandleFunc("/api/v0/dag/resolve", redirectToKubo)
	mux.HandleFunc("/api/v0/dns", redirectToKubo)

	// Remaining requests to the API receive a 501, as well as an explanation.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotImplemented)
		w.Write([]byte("/api/v0 RPC will be removed from gateways as it is not part of the gateway specification, but a legacy feature from Kubo. If you need this API, please self-host a Kubo instance yourself: docs.ipfs.tech/install/command-line"))
	})

	return mux
}

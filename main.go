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

func makeMetricsHandler(port int) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.Handle("/debug/metrics/prometheus", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))

	return &http.Server{
		Handler: mux,
		Addr:    ":" + strconv.Itoa(port),
	}, nil
}

func newAPIHandler(endpoints []string) http.Handler {
	s := rand.NewSource(time.Now().Unix())
	rand := rand.New(s)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Naively choose one of the Kubo RPC clients.
		endpoint := endpoints[rand.Intn(len(endpoints))]
		http.Redirect(w, r, endpoint+r.URL.Path+"?"+r.URL.RawQuery, http.StatusFound)
	})
}

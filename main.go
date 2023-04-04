package main

import (
	_ "embed"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/ipfs/bifrost-gateway/lib"
	"go.opentelemetry.io/contrib/propagators/autoprop"
	"go.opentelemetry.io/otel"

	blockstore "github.com/ipfs/boxo/blockstore"
	golog "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
)

var goLog = golog.Logger("bifrost-gateway")

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

const (
	EnvKuboRPC        = "KUBO_RPC_URL"
	EnvBlockCacheSize = "BLOCK_CACHE_SIZE"
	EnvGraphBackend   = "GRAPH_BACKEND"
)

func init() {
	rootCmd.Flags().Int("gateway-port", 8081, "gateway port")
	rootCmd.Flags().Int("metrics-port", 8041, "metrics port")
}

var rootCmd = &cobra.Command{
	Use:               name,
	Version:           version,
	CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	Short:             "IPFS Gateway implementation for https://github.com/protocol/bifrost-infra",
	Long: `bifrost-gateway provides HTTP Gateway backed by a remote blockstore.
See documentation at: https://github.com/ipfs/bifrost-gateway/#readme`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flags.
		gatewayPort, _ := cmd.Flags().GetInt("gateway-port")
		metricsPort, _ := cmd.Flags().GetInt("metrics-port")

		// Get env variables.
		saturnOrchestrator := getEnv(EnvSaturnOrchestrator, "")
		proxyGateway := getEnvs(EnvProxyGateway, "")
		kuboRPC := getEnvs(EnvKuboRPC, DefaultKuboRPC)

		blockCacheSize, err := getEnvInt(EnvBlockCacheSize, lib.DefaultCacheBlockStoreSize)
		if err != nil {
			return err
		}

		useGraphBackend, err := getEnvBool(EnvGraphBackend, false)
		if err != nil {
			return err
		}

		log.Printf("Starting %s %s", name, version)
		registerVersionMetric(version)

		tp, shutdown, err := newTracerProvider(cmd.Context())
		if err != nil {
			return err
		}
		defer func() {
			_ = shutdown(cmd.Context())
		}()
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(autoprop.NewTextMapPropagator())

		cdns := newCachedDNS(dnsCacheRefreshInterval)
		defer cdns.Close()

		var bs blockstore.Blockstore

		if saturnOrchestrator != "" {
			saturnLogger := getEnv(EnvSaturnLogger, DefaultSaturnLogger)
			log.Printf("Saturn backend (%s) at %s", EnvSaturnOrchestrator, saturnOrchestrator)
			log.Printf("Saturn logger  (%s) at %s", EnvSaturnLogger, saturnLogger)
			if os.Getenv(EnvSaturnLoggerSecret) == "" {
				log.Printf("")
				log.Printf("  WARNING: %s is not set", EnvSaturnLoggerSecret)
				log.Printf("")
			}
			bs, err = newCabooseBlockStore(saturnOrchestrator, saturnLogger, cdns)
			if err != nil {
				return err
			}
		} else if len(proxyGateway) != 0 {
			log.Printf("Proxy backend (PROXY_GATEWAY_URL) at %s", strings.Join(proxyGateway, " "))
			bs = newProxyBlockStore(proxyGateway, cdns)
		} else {
			log.Fatalf("Unable to start. bifrost-gateway requires either PROXY_GATEWAY_URL or STRN_ORCHESTRATOR_URL to be set.\n\nRead docs at https://github.com/ipfs/bifrost-gateway/blob/main/docs/environment-variables.md\n\n")
		}

		gatewaySrv, err := makeGatewayHandler(bs, kuboRPC, gatewayPort, blockCacheSize, cdns, useGraphBackend)
		if err != nil {
			return err
		}

		metricsSrv, err := makeMetricsAndDebuggingHandler(metricsPort)
		if err != nil {
			return err
		}

		quit := make(chan os.Signal, 1)
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()

			// log important configuration flags
			log.Printf("%s: %d", EnvBlockCacheSize, blockCacheSize)
			log.Printf("%s: %t", EnvGraphBackend, useGraphBackend)

			log.Printf("Legacy RPC at /api/v0 (%s) provided by %s", EnvKuboRPC, strings.Join(kuboRPC, " "))
			log.Printf("Path gateway listening on http://127.0.0.1:%d", gatewayPort)
			log.Printf("  Smoke test (JPG): http://127.0.0.1:%d/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi", gatewayPort)
			log.Printf("Subdomain gateway configured on dweb.link and http://localhost:%d", gatewayPort)
			log.Printf("  Smoke test (Subdomain+DNSLink+UnixFS+HAMT): http://localhost:%d/ipns/en.wikipedia-on-ipfs.org/wiki/", gatewayPort)
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

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvs(key, defaultValue string) []string {
	value := os.Getenv(key)
	if value == "" {
		if defaultValue == "" {
			return []string{}
		}
		value = defaultValue
	}
	value = strings.TrimSpace(value)
	return strings.Split(value, ",")
}

func getEnvInt(key string, defaultValue int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue, nil
	}
	return strconv.Atoi(value)
}

func getEnvBool(key string, defaultValue bool) (bool, error) {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue, nil
	}
	return strconv.ParseBool(value)
}

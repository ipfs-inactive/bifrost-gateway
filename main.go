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
	DefaultSaturnLogger       = "https://logs.strn.network"
	DefaultSaturnOrchestrator = "https://orchestrator.strn.pl/nodes/nearby"

	EnvSaturnLogger       = "STRN_LOGGER_URL"
	EnvSaturnOrchestrator = "STRN_ORCHESTRATOR_URL"
	EnvBlockCacheSize     = "BLOCK_CACHE_SIZE"
	EnvKuboRPC            = "KUBO_RPC_URL"
)

func init() {
	rootCmd.Flags().Int("gateway-port", 8081, "gateway port")
	rootCmd.Flags().Int("metrics-port", 8041, "metrics port")

	rootCmd.MarkFlagRequired("saturn-orchestrator")
	rootCmd.MarkFlagRequired("saturn-logger")
	rootCmd.MarkFlagRequired("kubo-rpc")
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
		saturnOrchestrator := getEnv(EnvSaturnOrchestrator, DefaultSaturnOrchestrator)
		saturnLogger := getEnv(EnvSaturnLogger, DefaultSaturnLogger)
		kuboRPC := strings.Split(os.Getenv(EnvKuboRPC), ",")
		blockCacheSize, err := getEnvInt(EnvBlockCacheSize, DefaultCacheBlockStoreSize)
		if err != nil {
			return err
		}

		log.Printf("Starting %s %s", name, version)

		cdns := newCachedDNS(dnsCacheRefreshInterval)
		defer cdns.Close()

		gatewaySrv, err := makeGatewayHandler(saturnOrchestrator, saturnLogger, kuboRPC, gatewayPort, blockCacheSize, cdns)
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

			log.Printf("Block cache size: %d", blockCacheSize)
			log.Printf("Legacy RPC at /api/v0 provided by %s", strings.Join(kuboRPC, " "))
			log.Printf("Path gateway listening on http://127.0.0.1:%d", gatewayPort)
			log.Printf("Subdomain gateway configured on dweb.link and http://localhost:%d", gatewayPort)
			log.Printf("Smoke test (JPG): http://127.0.0.1:%d/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi", gatewayPort)
			log.Printf("Smoke test (Subdomain+DNSLink+UnixFS+HAMT): http://localhost:%d/ipns/en.wikipedia-on-ipfs.org/wiki/", gatewayPort)
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

func getEnvInt(key string, defaultValue int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue, nil
	}
	return strconv.Atoi(value)
}

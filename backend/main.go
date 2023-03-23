package main

import (
	"errors"
	"fmt"
	"github.com/ipfs/bifrost-gateway/lib"
	"github.com/ipfs/go-blockservice"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-libipfs/bitswap/client"
	"github.com/ipfs/go-libipfs/bitswap/network"
	golog "github.com/ipfs/go-log/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/spf13/cobra"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
)

var goLog = golog.Logger("bifrost-gateway-backend")

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().Int("gateway-port", 8081, "gateway port")
	rootCmd.Flags().Int("metrics-port", 8041, "metrics port")
	rootCmd.Flags().String("car-blockstore", "", "a CAR file to use for serving data instead of network requests")
	golog.SetLogLevel("bifrost-gateway-backend", "debug")
}

var rootCmd = &cobra.Command{
	Use:               name,
	Version:           version,
	CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	Short:             "IPFS Gateway backend implementation for https://github.com/protocol/bifrost-infra",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flags.
		gatewayPort, _ := cmd.Flags().GetInt("gateway-port")
		metricsPort, _ := cmd.Flags().GetInt("metrics-port")
		carbsLocation, _ := cmd.Flags().GetString("car-blockstore")

		var bsrv blockservice.BlockService
		if carbsLocation != "" {
			bs, err := carbs.OpenReadOnly(carbsLocation)
			if err != nil {
				return err
			}
			bsrv = blockservice.New(bs, offline.Exchange(bs))
		} else {
			//blockCacheSize, err := getEnvInt(EnvBlockCacheSize, lib.DefaultCacheBlockStoreSize)
			//if err != nil {
			//	return err
			//}
			blockCacheSize := lib.DefaultCacheBlockStoreSize

			bs, err := lib.NewCacheBlockStore(blockCacheSize)
			if err != nil {
				return err
			}

			var r routing.Routing
			h, err := libp2p.New(libp2p.Routing(func(host host.Host) (routing.PeerRouting, error) {
				r, err = dht.New(cmd.Context(), host, dht.BootstrapPeersFunc(dht.GetDefaultBootstrapPeerAddrInfos))
				return r, err
			}))
			if err != nil {
				return err
			}
			n := network.NewFromIpfsHost(h, r)
			bsc := client.New(cmd.Context(), n, bs)
			n.Start(bsc)
			defer n.Stop()

			bsrv = blockservice.New(bs, bsc)
		}

		log.Printf("Starting %s %s", name, version)

		var gatewaySrv *http.Server
		var err error

		if true {
			gatewaySrv, err = makeGatewayCARHandler(bsrv, gatewayPort)
			if err != nil {
				return err
			}
		} else {
			gatewaySrv, err = makeGatewayBlockHandler(bsrv, gatewayPort)
			if err != nil {
				return err
			}
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

			//log.Printf("%s: %d", EnvBlockCacheSize, blockCacheSize)
			log.Printf("Path gateway listening on http://127.0.0.1:%d", gatewayPort)
			log.Printf("  Smoke test (JPG): http://127.0.0.1:%d/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?format=raw", gatewayPort)
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

package main

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"time"

	"github.com/filecoin-saturn/caboose"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

const DefaultSaturnLogger = "http://set-STRN_LOGGER_URL"

func newCabooseBlockStore(orchestrator, loggingEndpoint string, cdns *cachedDNS) (blockstore.Blockstore, error) {
	var (
		orchURL *url.URL
		loggURL *url.URL
		err     error
	)

	if orchestrator != "" {
		orchURL, err = url.Parse(orchestrator)
		if err != nil {
			return nil, err
		}
	}

	if loggingEndpoint != "" {
		loggURL, err = url.Parse(loggingEndpoint)
		if err != nil {
			return nil, err
		}
	}

	saturnServiceClient := &http.Client{
		Timeout: caboose.DefaultSaturnRequestTimeout,
		Transport: &withUserAgent{
			RoundTripper: &http.Transport{
				DialContext: cdns.dialWithCachedDNS,
			},
		},
	}

	saturnRetrievalClient := &http.Client{
		Timeout: caboose.DefaultSaturnRequestTimeout,
		Transport: &withUserAgent{
			RoundTripper: &http.Transport{
				// Increasing concurrency defaults from http.DefaultTransport
				MaxIdleConns:        1000,
				MaxConnsPerHost:     100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,

				DialContext: cdns.dialWithCachedDNS,

				// Saturn Weltschmerz
				TLSClientConfig: &tls.Config{
					// Saturn use TLS in controversial ways, which sooner or
					// later will force them to switch away to different domain
					// name and certs, in which case they will break us. Since
					// we are fetching raw blocks and dont really care about
					// TLS cert being legitimate, let's disable verification
					// to save CPU and to avoid catastrophic failure when
					// Saturn L1s suddenly switch to certs with different DNS name.
					InsecureSkipVerify: true,
					// ServerName:         "strn.pl",
				},
			},
		},
	}

	return caboose.NewCaboose(&caboose.Config{
		OrchestratorEndpoint: orchURL,
		OrchestratorClient:   saturnServiceClient,

		LoggingEndpoint: *loggURL,
		LoggingClient:   saturnServiceClient,
		LoggingInterval: 5 * time.Second,

		DoValidation: true,
		PoolRefresh:  5 * time.Minute,
		SaturnClient: saturnRetrievalClient,
	})
}

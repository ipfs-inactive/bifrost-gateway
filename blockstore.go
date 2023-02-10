package main

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"time"

	"github.com/filecoin-saturn/caboose"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

func newBlockStore(orchestrator, loggingEndpoint string) (blockstore.Blockstore, error) {
	oe, err := url.Parse(orchestrator)
	if err != nil {
		return nil, err
	}

	le, err := url.Parse(loggingEndpoint)
	if err != nil {
		return nil, err
	}

	saturnClient := &http.Client{
		Transport: &withUserAgent{
			RoundTripper: &http.Transport{
				TLSClientConfig: &tls.Config{
					ServerName: "strn.pl",
				},
			},
		},
	}

	return caboose.NewCaboose(&caboose.Config{
		OrchestratorEndpoint: *oe,
		OrchestratorClient:   http.DefaultClient,

		LoggingEndpoint: *le,
		LoggingClient:   http.DefaultClient,
		LoggingInterval: 5 * time.Second,

		DoValidation: true,
		PoolRefresh:  5 * time.Minute,
		Client:       saturnClient,
	})
}

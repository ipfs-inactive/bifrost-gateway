package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	blocks "github.com/ipfs/go-libipfs/blocks"
)

const GetBlockTimeout = time.Second * 30

func newExchange(orchestrator, loggingEndpoint string) (exchange.Interface, error) {
	b, err := newBlockStore(orchestrator, loggingEndpoint)
	if err != nil {
		return nil, err
	}
	return &exchangeBsWrapper{bstore: b}, nil
}

type exchangeBsWrapper struct {
	bstore blockstore.Blockstore
}

func (e *exchangeBsWrapper) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, cancel := context.WithTimeout(ctx, GetBlockTimeout)
	defer cancel()

	return e.bstore.Get(ctx, c)
}

func (e *exchangeBsWrapper) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	out := make(chan blocks.Block)

	go func() {
		defer close(out)
		for _, c := range cids {
			blk, err := e.GetBlock(ctx, c)
			if err != nil {
				return
			}
			out <- blk
		}
	}()
	return out, nil
}

func (e *exchangeBsWrapper) NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error {
	return nil
}

func (e *exchangeBsWrapper) Close() error {
	return nil
}

var _ exchange.Interface = (*exchangeBsWrapper)(nil)

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

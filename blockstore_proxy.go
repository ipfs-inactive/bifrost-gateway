package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-libipfs/blocks"
)

type proxyBlockStore struct {
	httpClient *http.Client
	gatewayURL []string
	validate   bool
	rand       *rand.Rand
}

func newProxyBlockStore(gatewayURL []string, cdns *cachedDNS) blockstore.Blockstore {
	s := rand.NewSource(time.Now().Unix())
	rand := rand.New(s)

	return &proxyBlockStore{
		gatewayURL: gatewayURL,
		httpClient: &http.Client{
			Timeout: GetBlockTimeout,
			Transport: &withUserAgent{
				// Roundtripper with increased defaults than http.Transport such that retrieving
				// multiple blocks from a single gateway concurrently is fast.
				RoundTripper: &http.Transport{
					MaxIdleConns:        1000,
					MaxConnsPerHost:     100,
					MaxIdleConnsPerHost: 100,
					IdleConnTimeout:     90 * time.Second,
					DialContext:         cdns.dialWithCachedDNS,
				},
			},
		},
		// Enables block validation by default. Important since we are
		// proxying block requests to an untrusted gateway.
		validate: true,
		rand:     rand,
	}
}

func (ps *proxyBlockStore) fetch(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	u, err := url.Parse(fmt.Sprintf("%s/ipfs/%s?format=raw", ps.getRandomGatewayURL(), c))
	if err != nil {
		return nil, err
	}
	resp, err := ps.httpClient.Do(&http.Request{
		Method: http.MethodGet,
		URL:    u,
		Header: http.Header{
			"Accept": []string{"application/vnd.ipld.raw"},
		},
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http error from block gateway: %s", resp.Status)
	}

	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if ps.validate {
		nc, err := c.Prefix().Sum(rb)
		if err != nil {
			return nil, blocks.ErrWrongHash
		}
		if !nc.Equals(c) {
			return nil, blocks.ErrWrongHash
		}
	}

	return blocks.NewBlockWithCid(rb, c)
}

func (ps *proxyBlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	blk, err := ps.fetch(ctx, c)
	if err != nil {
		return false, err
	}
	return blk != nil, nil
}

func (ps *proxyBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blk, err := ps.fetch(ctx, c)
	if err != nil {
		return nil, err
	}
	return blk, nil
}

func (ps *proxyBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	blk, err := ps.fetch(ctx, c)
	if err != nil {
		return 0, err
	}
	return len(blk.RawData()), nil
}

func (ps *proxyBlockStore) HashOnRead(enabled bool) {
	ps.validate = enabled
}

func (c *proxyBlockStore) Put(context.Context, blocks.Block) error {
	return errNotImplemented
}

func (c *proxyBlockStore) PutMany(context.Context, []blocks.Block) error {
	return errNotImplemented
}

func (c *proxyBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errNotImplemented
}

func (c *proxyBlockStore) DeleteBlock(context.Context, cid.Cid) error {
	return errNotImplemented
}

func (ps *proxyBlockStore) getRandomGatewayURL() string {
	return ps.gatewayURL[ps.rand.Intn(len(ps.gatewayURL))]
}

var _ blockstore.Blockstore = (*proxyBlockStore)(nil)

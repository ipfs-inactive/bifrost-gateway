package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type proxyRouting struct {
	gatewayURLs []string
	httpClient  *http.Client
	rand        *rand.Rand
}

func newProxyRouting(gatewayURLs []string, cdns *cachedDNS) routing.ValueStore {
	s := rand.NewSource(time.Now().Unix())
	rand := rand.New(s)

	return &proxyRouting{
		gatewayURLs: gatewayURLs,
		httpClient: &http.Client{
			Transport: otelhttp.NewTransport(&customTransport{
				// RoundTripper with increased defaults than http.Transport such that retrieving
				// multiple lookups concurrently is fast.
				RoundTripper: &http.Transport{
					MaxIdleConns:        1000,
					MaxConnsPerHost:     100,
					MaxIdleConnsPerHost: 100,
					IdleConnTimeout:     90 * time.Second,
					DialContext:         cdns.dialWithCachedDNS,
					ForceAttemptHTTP2:   true,
				},
			}),
		},
		rand: rand,
	}
}

func (ps *proxyRouting) PutValue(context.Context, string, []byte, ...routing.Option) error {
	return routing.ErrNotSupported
}

func (ps *proxyRouting) GetValue(ctx context.Context, k string, opts ...routing.Option) ([]byte, error) {
	if !strings.HasPrefix(k, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(k))
	if err != nil {
		return nil, err
	}

	return ps.fetch(ctx, name)
}

func (ps *proxyRouting) SearchValue(ctx context.Context, k string, opts ...routing.Option) (<-chan []byte, error) {
	if !strings.HasPrefix(k, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(k))
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte)

	go func() {
		v, err := ps.fetch(ctx, name)
		if err != nil {
			close(ch)
		} else {
			ch <- v
			close(ch)
		}
	}()

	return ch, nil
}

func (ps *proxyRouting) fetch(ctx context.Context, name ipns.Name) ([]byte, error) {
	urlStr := fmt.Sprintf("%s/ipns/%s", ps.getRandomGatewayURL(), name.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.ipfs.ipns-record")

	goLog.Debugw("routing proxy fetch", "key", name.String(), "from", req.URL.String())
	defer func() {
		if err != nil {
			goLog.Debugw("routing proxy fetch error", "key", name.String(), "from", req.URL.String(), "error", err.Error())
		}
	}()

	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status from remote gateway: %s", resp.Status)
	}

	rb, err := io.ReadAll(io.LimitReader(resp.Body, int64(ipns.MaxRecordSize)))
	if err != nil {
		return nil, err
	}

	rec, err := ipns.UnmarshalRecord(rb)
	if err != nil {
		return nil, err
	}

	err = ipns.ValidateWithName(rec, name)
	if err != nil {
		return nil, err
	}

	return rb, nil
}

func (ps *proxyRouting) getRandomGatewayURL() string {
	return strings.TrimSuffix(ps.gatewayURLs[ps.rand.Intn(len(ps.gatewayURLs))], "/")
}

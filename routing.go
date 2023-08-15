package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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

type rpcProxyRouting struct {
	kuboRPC    []string
	httpClient *http.Client
	rand       *rand.Rand
}

func newRPCProxyRouting(kuboRPC []string, cdns *cachedDNS) routing.ValueStore {
	s := rand.NewSource(time.Now().Unix())
	rand := rand.New(s)

	return &rpcProxyRouting{
		kuboRPC: kuboRPC,
		httpClient: &http.Client{
			Transport: otelhttp.NewTransport(&customTransport{
				// Roundtripper with increased defaults than http.Transport such that retrieving
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

func (ps *rpcProxyRouting) PutValue(context.Context, string, []byte, ...routing.Option) error {
	return routing.ErrNotSupported
}

func (ps *rpcProxyRouting) GetValue(ctx context.Context, k string, opts ...routing.Option) ([]byte, error) {
	return ps.fetch(ctx, k)
}

func (ps *rpcProxyRouting) SearchValue(ctx context.Context, k string, opts ...routing.Option) (<-chan []byte, error) {
	if !strings.HasPrefix(k, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	ch := make(chan []byte)

	go func() {
		v, err := ps.fetch(ctx, k)
		if err != nil {
			close(ch)
		} else {
			ch <- v
			close(ch)
		}
	}()

	return ch, nil
}

func (ps *rpcProxyRouting) fetch(ctx context.Context, key string) (rb []byte, err error) {
	name, err := ipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return nil, err
	}

	key = "/ipns/" + name.String()

	urlStr := fmt.Sprintf("%s/api/v0/dht/get?arg=%s", ps.getRandomKuboURL(), key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, nil)
	if err != nil {
		return nil, err
	}

	goLog.Debugw("routing proxy fetch", "key", key, "from", req.URL.String())
	defer func() {
		if err != nil {
			goLog.Debugw("routing proxy fetch error", "key", key, "from", req.URL.String(), "error", err.Error())
		}
	}()

	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read at most 10 KiB (max size of IPNS record).
	rb, err = io.ReadAll(io.LimitReader(resp.Body, 10240))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("routing/get RPC returned unexpected status: %s, body: %q", resp.Status, string(rb))
	}

	parts := bytes.Split(bytes.TrimSpace(rb), []byte("\n"))
	var b64 string

	for _, part := range parts {
		var evt routing.QueryEvent
		err = json.Unmarshal(part, &evt)
		if err != nil {
			return nil, fmt.Errorf("routing/get RPC response cannot be parsed: %w", err)
		}

		if evt.Type == routing.Value {
			b64 = evt.Extra
			break
		}
	}

	if b64 == "" {
		return nil, errors.New("routing/get RPC returned no value")
	}

	rb, err = base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}

	entry, err := ipns.UnmarshalRecord(rb)
	if err != nil {
		return nil, err
	}

	err = ipns.ValidateWithName(entry, name)
	if err != nil {
		return nil, err
	}

	return rb, nil
}

func (ps *rpcProxyRouting) getRandomKuboURL() string {
	return ps.kuboRPC[ps.rand.Intn(len(ps.kuboRPC))]
}

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

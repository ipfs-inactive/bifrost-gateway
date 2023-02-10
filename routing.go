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
	"net/url"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/go-ipns"
	ipns_pb "github.com/ipfs/go-ipns/pb"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

type proxyRouting struct {
	kuboRPC    []string
	httpClient *http.Client
	rand       *rand.Rand
}

func newProxyRouting(kuboRPC []string) routing.ValueStore {
	s := rand.NewSource(time.Now().Unix())
	rand := rand.New(s)

	return &proxyRouting{
		kuboRPC: kuboRPC,
		httpClient: &http.Client{
			Transport: &withUserAgent{
				RoundTripper: http.DefaultTransport,
			},
		},
		rand: rand,
	}
}

func (ps *proxyRouting) PutValue(context.Context, string, []byte, ...routing.Option) error {
	return routing.ErrNotSupported
}

func (ps *proxyRouting) GetValue(ctx context.Context, k string, opts ...routing.Option) ([]byte, error) {
	return ps.fetch(ctx, k)
}

func (ps *proxyRouting) SearchValue(ctx context.Context, k string, opts ...routing.Option) (<-chan []byte, error) {
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

func (ps *proxyRouting) fetch(ctx context.Context, key string) (rb []byte, err error) {
	key = strings.TrimPrefix(key, "/ipns/")
	id, err := peer.IDFromBytes([]byte(key))
	if err != nil {
		return nil, err
	}

	key = "/ipns/" + peer.ToCid(id).String()

	// Naively choose one of the Kubo RPC clients.
	endpoint := ps.kuboRPC[rand.Intn(len(ps.kuboRPC))]

	u, err := url.Parse(fmt.Sprintf("%s/api/v0/dht/get?arg=%s", endpoint, key))
	if err != nil {
		return nil, err
	}

	goLog.Debugw("routing proxy fetch", "key", key, "from", u.String())
	defer func() {
		if err != nil {
			goLog.Debugw("routing proxy fetch error", "key", key, "from", u.String(), "error", err.Error())
		}
	}()

	resp, err := ps.httpClient.Do(&http.Request{
		Method: http.MethodPost,
		URL:    u,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("routing/get RPC returned unexpected status: %s", resp.Status)
	}

	rb, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
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

	var entry ipns_pb.IpnsEntry
	err = proto.Unmarshal(rb, &entry)
	if err != nil {
		return nil, err
	}

	pub, err := id.ExtractPublicKey()
	if err != nil {
		// Make sure it works with all those RSA that cannot be embedded into the
		// Peer ID.
		if len(entry.PubKey) > 0 {
			pub, err = ic.UnmarshalPublicKey(entry.PubKey)
		}
	}
	if err != nil {
		return nil, err
	}

	err = ipns.Validate(pub, &entry)
	if err != nil {
		return nil, err
	}

	return rb, nil
}

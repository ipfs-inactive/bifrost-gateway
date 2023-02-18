package main

import (
	"context"
	"net"
	"time"

	"github.com/rs/dnscache"
)

// Local DNS cache because in this world things are ephemeral
var dnsCache = &dnscache.Resolver{}

// How often should we check for successful updates to cached entries
const dnsCacheRefreshInterval = 5 * time.Minute

func init() {
	// Configure DNS cache to not remove stale records to protect gateway from
	// catastrophic failures like https://github.com/ipfs/bifrost-gateway/issues/34
	options := dnscache.ResolverRefreshOptions{}
	options.ClearUnused = false
	options.PersistOnFailure = false

	// Every dnsCacheRefreshInterval we check for updates, but if there is
	// none, or if domain disappears, we keep the last cached version
	go func() {
		t := time.NewTicker(dnsCacheRefreshInterval)
		defer t.Stop()
		for range t.C {
			dnsCache.RefreshWithOptions(options)
		}
	}()
}

// dialWithCachedDNS implements DialContext that uses dnsCache
func dialWithCachedDNS(ctx context.Context, network string, addr string) (conn net.Conn, err error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := dnsCache.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	// Try all IPs returned by DNS
	for _, ip := range ips {
		var dialer net.Dialer
		conn, err = dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if err == nil {
			break
		}
	}
	return
}

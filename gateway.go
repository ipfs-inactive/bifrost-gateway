package main

import (
	"context"
	"fmt"
	gopath "path"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-namesys"
	"github.com/ipfs/go-namesys/resolve"
	ipfspath "github.com/ipfs/go-path"
	"github.com/ipfs/go-path/resolver"
	"github.com/ipfs/go-unixfs"
	ufile "github.com/ipfs/go-unixfs/file"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/ipfs/go-unixfsnode"
	iface "github.com/ipfs/interface-go-ipfs-core"
	nsopts "github.com/ipfs/interface-go-ipfs-core/options/namesys"
	ifacepath "github.com/ipfs/interface-go-ipfs-core/path"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/libp2p/go-libp2p/core/routing"
)

type bifrostGateway struct {
	blockStore   blockstore.Blockstore
	blockService blockservice.BlockService
	dagService   format.DAGService
	resolver     resolver.Resolver
	namesys      namesys.NameSystem
	routing      routing.ValueStore
}

func newBifrostGateway(blockService blockservice.BlockService, routing routing.ValueStore) (*bifrostGateway, error) {
	// Setup the DAG services, which use the CAR block store.
	dagService := merkledag.NewDAGService(blockService)

	// Setup the UnixFS resolver.
	fetcherConfig := bsfetcher.NewFetcherConfig(blockService)
	fetcherConfig.PrototypeChooser = dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})
	fetcher := fetcherConfig.WithReifier(unixfsnode.Reify)
	resolver := resolver.NewBasicResolver(fetcher)

	// Setup name system for DNSLink and IPNS resolution.
	namesys, err := namesys.NewNameSystem(routing)
	if err != nil {
		return nil, err
	}

	return &bifrostGateway{
		blockStore:   blockService.Blockstore(),
		blockService: blockService,
		dagService:   dagService,
		resolver:     resolver,
		namesys:      namesys,
		routing:      routing,
	}, nil
}

func (api *bifrostGateway) GetUnixFsNode(ctx context.Context, p ifacepath.Resolved) (files.Node, error) {
	nd, err := api.resolveNode(ctx, p)
	if err != nil {
		return nil, err
	}

	return ufile.NewUnixfsFile(ctx, api.dagService, nd)
}

func (api *bifrostGateway) LsUnixFsDir(ctx context.Context, p ifacepath.Resolved) (<-chan iface.DirEntry, error) {
	node, err := api.resolveNode(ctx, p)
	if err != nil {
		return nil, err
	}

	dir, err := uio.NewDirectoryFromNode(api.dagService, node)
	if err != nil {
		return nil, err
	}

	out := make(chan iface.DirEntry, uio.DefaultShardWidth)

	go func() {
		defer close(out)
		for l := range dir.EnumLinksAsync(ctx) {
			select {
			case out <- api.processLink(ctx, l):
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

func (api *bifrostGateway) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return api.blockService.GetBlock(ctx, c)
}

func (api *bifrostGateway) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	return api.routing.GetValue(ctx, "/ipns/"+c.String())
}

func (api *bifrostGateway) GetDNSLinkRecord(ctx context.Context, hostname string) (ifacepath.Path, error) {
	p, err := api.namesys.Resolve(ctx, "/ipns/"+hostname, nsopts.Depth(1))
	if err == namesys.ErrResolveRecursion {
		err = nil
	}
	return ifacepath.New(p.String()), err
}

func (api *bifrostGateway) IsCached(ctx context.Context, p ifacepath.Path) bool {
	rp, err := api.ResolvePath(ctx, p)
	if err != nil {
		return false
	}

	has, _ := api.blockStore.Has(ctx, rp.Cid())
	return has
}

func (api *bifrostGateway) ResolvePath(ctx context.Context, p ifacepath.Path) (ifacepath.Resolved, error) {
	if _, ok := p.(ifacepath.Resolved); ok {
		return p.(ifacepath.Resolved), nil
	}

	err := p.IsValid()
	if err != nil {
		return nil, err
	}

	ipath := ipfspath.Path(p.String())
	if ipath.Segments()[0] == "ipns" {
		ipath, err = resolve.ResolveIPNS(ctx, api.namesys, ipath)
		if err != nil {
			return nil, err
		}
	}

	if ipath.Segments()[0] != "ipfs" {
		return nil, fmt.Errorf("unsupported path namespace: %s", p.Namespace())
	}

	node, rest, err := api.resolver.ResolveToLastNode(ctx, ipath)
	if err != nil {
		return nil, err
	}

	root, err := cid.Parse(ipath.Segments()[1])
	if err != nil {
		return nil, err
	}

	return ifacepath.NewResolvedPath(ipath, node, root, gopath.Join(rest...)), nil
}

func (api *bifrostGateway) resolveNode(ctx context.Context, p ifacepath.Path) (format.Node, error) {
	rp, err := api.ResolvePath(ctx, p)
	if err != nil {
		return nil, err
	}

	node, err := api.dagService.Get(ctx, rp.Cid())
	if err != nil {
		return nil, fmt.Errorf("get node: %w", err)
	}
	return node, nil
}

func (api *bifrostGateway) processLink(ctx context.Context, result unixfs.LinkResult) iface.DirEntry {
	if result.Err != nil {
		return iface.DirEntry{Err: result.Err}
	}

	link := iface.DirEntry{
		Name: result.Link.Name,
		Cid:  result.Link.Cid,
	}

	switch link.Cid.Type() {
	case cid.Raw:
		link.Type = iface.TFile
		link.Size = result.Link.Size
	case cid.DagProtobuf:
		link.Size = result.Link.Size
	}

	return link
}

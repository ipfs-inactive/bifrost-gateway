package lib

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/filecoin-saturn/caboose"
	bsfetcher "github.com/ipfs/boxo/fetcher/impl/blockservice"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/verifcid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipld/go-car"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
)

type getBlock func(ctx context.Context, cid cid.Cid) (blocks.Block, error)

var ErrNilBlock = caboose.ErrInvalidResponse{Message: "received a nil block with no error"}

func carToLinearBlockGetter(ctx context.Context, reader io.Reader, metrics *GraphGatewayMetrics) (getBlock, error) {
	cr, err := car.NewCarReaderWithOptions(reader, car.WithErrorOnEmptyRoots(false))
	if err != nil {
		return nil, err
	}

	cbCtx, cncl := context.WithCancel(ctx)

	type blockRead struct {
		block blocks.Block
		err   error
	}

	blkCh := make(chan blockRead, 1)
	go func() {
		defer cncl()
		defer close(blkCh)
		for {
			blk, rdErr := cr.Next()
			select {
			case blkCh <- blockRead{blk, rdErr}:
				if rdErr != nil {
					cncl()
				}
			case <-cbCtx.Done():
				return
			}
		}
	}()

	isFirstBlock := true
	mx := sync.Mutex{}

	return func(ctx context.Context, c cid.Cid) (blocks.Block, error) {
		mx.Lock()
		defer mx.Unlock()
		if err := verifcid.ValidateCid(c); err != nil {
			return nil, err
		}

		// initially set a higher timeout here so that if there's an initial timeout error we get it from the car reader.
		var t *time.Timer
		if isFirstBlock {
			t = time.NewTimer(GetBlockTimeout * 2)
		} else {
			t = time.NewTimer(GetBlockTimeout)
		}
		var blkRead blockRead
		var ok bool
		select {
		case blkRead, ok = <-blkCh:
			if !t.Stop() {
				<-t.C
			}
			t.Reset(GetBlockTimeout)
		case <-t.C:
			return nil, gateway.ErrGatewayTimeout
		}
		if !ok || blkRead.err != nil {
			if !ok || errors.Is(blkRead.err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, GatewayError(blkRead.err)
		}
		if blkRead.block != nil {
			metrics.carBlocksFetchedMetric.Inc()
			if !blkRead.block.Cid().Equals(c) {
				return nil, caboose.ErrInvalidResponse{Message: fmt.Sprintf("received block with cid %s, expected %s", blkRead.block.Cid(), c)}
			}
			return blkRead.block, nil
		}
		return nil, ErrNilBlock
	}, nil
}

func getLinksystem(fn getBlock) *ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(linkContext linking.LinkContext, link datamodel.Link) (io.Reader, error) {
		c := link.(cidlink.Link).Cid
		blk, err := fn(linkContext.Ctx, c)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(blk.RawData()), nil
	}
	lsys.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
	return &lsys
}

// walkGatewaySimpleSelector walks the subgraph described by the path and terminal element parameters
func walkGatewaySimpleSelector(ctx context.Context, terminalBlk blocks.Block, params gateway.CarParams, lsys *ipld.LinkSystem) error {
	lctx := ipld.LinkContext{Ctx: ctx}
	var err error

	// If the scope is the block, we only need the root block of the last element of the path, which we have.
	if params.Scope == gateway.DagScopeBlock {
		return nil
	}

	// decode the terminal block into a node
	pc := dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})

	pathTerminalCidLink := cidlink.Link{Cid: terminalBlk.Cid()}
	np, err := pc(pathTerminalCidLink, lctx)
	if err != nil {
		return err
	}

	decoder, err := lsys.DecoderChooser(pathTerminalCidLink)
	if err != nil {
		return err
	}
	nb := np.NewBuilder()
	blockData := terminalBlk.RawData()
	if err := decoder(nb, bytes.NewReader(blockData)); err != nil {
		return err
	}
	lastCidNode := nb.Build()

	// TODO: Evaluate:
	// Does it matter that we're ignoring the "remainder" portion of the traversal in GetCAR?
	// Does it matter that we're using a linksystem with the UnixFS reifier for dagscope=all?

	// If we're asking for everything then give it
	if params.Scope == gateway.DagScopeAll {
		sel, err := selector.ParseSelector(selectorparse.CommonSelector_ExploreAllRecursively)
		if err != nil {
			return err
		}

		progress := traversal.Progress{
			Cfg: &traversal.Config{
				Ctx:                            ctx,
				LinkSystem:                     *lsys,
				LinkTargetNodePrototypeChooser: bsfetcher.DefaultPrototypeChooser,
				LinkVisitOnlyOnce:              true, // This is safe for the "all" selector
			},
		}

		if err := progress.WalkMatching(lastCidNode, sel, func(progress traversal.Progress, node datamodel.Node) error {
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	// From now on, dag-scope=entity!
	// Since we need more of the graph load it to figure out what we have
	// This includes determining if the terminal node is UnixFS or not
	if pbn, ok := lastCidNode.(dagpb.PBNode); !ok {
		// If it's not valid dag-pb then we're done
		return nil
	} else if !pbn.FieldData().Exists() {
		// If it's not valid UnixFS then we're done
		return nil
	} else if unixfsFieldData, decodeErr := data.DecodeUnixFSData(pbn.Data.Must().Bytes()); decodeErr != nil {
		// If it's not valid dag-pb and UnixFS then we're done
		return nil
	} else {
		switch unixfsFieldData.FieldDataType().Int() {
		case data.Data_Directory, data.Data_Symlink:
			// These types are non-recursive so we're done
			return nil
		case data.Data_Raw, data.Data_Metadata:
			// TODO: for now, we decided to return nil here. The different implementations are inconsistent
			// and UnixFS is not properly specified: https://github.com/ipfs/specs/issues/316.
			// 		- Is Data_Raw different from Data_File?
			//		- Data_Metadata is handled differently in boxo/ipld/unixfs and go-unixfsnode.
			return nil
		case data.Data_HAMTShard:
			// Return all elements in the map
			_, err := lsys.KnownReifiers["unixfs-preload"](lctx, lastCidNode, lsys)
			if err != nil {
				return err
			}
			return nil
		case data.Data_File:
			nd, err := unixfsnode.Reify(lctx, lastCidNode, lsys)
			if err != nil {
				return err
			}

			fnd, ok := nd.(datamodel.LargeBytesNode)
			if !ok {
				return fmt.Errorf("could not process file since it did not present as large bytes")
			}
			f, err := fnd.AsLargeBytes()
			if err != nil {
				return err
			}

			// Get the entity range. If it's empty, assume the defaults (whole file).
			entityRange := params.Range
			if entityRange == nil {
				entityRange = &gateway.DagByteRange{
					From: 0,
				}
			}

			from := entityRange.From

			// If we're starting to read based on the end of the file, find out where that is.
			var fileLength int64
			foundFileLength := false
			if entityRange.From < 0 {
				fileLength, err = f.Seek(0, io.SeekEnd)
				if err != nil {
					return err
				}
				from = fileLength + entityRange.From
				foundFileLength = true
			}

			// If we're reading until the end of the file then do it
			if entityRange.To == nil {
				if _, err := f.Seek(from, io.SeekStart); err != nil {
					return err
				}
				_, err = io.Copy(io.Discard, f)
				return err
			}

			to := *entityRange.To
			if (*entityRange.To) < 0 && !foundFileLength {
				fileLength, err = f.Seek(0, io.SeekEnd)
				if err != nil {
					return err
				}
				to = fileLength + *entityRange.To
				foundFileLength = true
			}

			numToRead := 1 + to - from
			if numToRead < 0 {
				return fmt.Errorf("tried to read less than zero bytes")
			}

			if _, err := f.Seek(from, io.SeekStart); err != nil {
				return err
			}
			_, err = io.CopyN(io.Discard, f, numToRead)
			return err
		default:
			// Not a supported type, so we're done
			return nil
		}
	}
}

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "embed"

	"github.com/ipfs/bifrost-gateway/lib"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/storage"
)

//go:embed lib/fixtures/directory-with-multilayer-hamt-and-multiblock-files.car
var dirWithMultiblockHAMTandFiles []byte

func TestTar(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requestNum := 0
	s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNum++
		switch requestNum {
		case 1:
			// Expect the full request, but return one that terminates in the middle of the HAMT
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
				"bafybeifdv255wmsrh75vcsrtkcwyktvewgihegeeyhhj2ju4lzt4lqfoze", // basicDir
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
				"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
				"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
				"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
				"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
				"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
			}); err != nil {
				panic(err)
			}
		case 2:
			// Expect a request for the HAMT only and give it
			// Note: this is an implementation detail, it could be in the future that we request less or more data
			// (e.g. requesting the blocks to fill out the HAMT, or with spec changes asking for HAMT ranges, or asking for the HAMT and its children)
			expectedUri := "/ipfs/bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
				"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm",
				"bafybeihjydob4eq5j4m43whjgf5cgftthc42kjno3g24sa3wcw7vonbmfy",
			}); err != nil {
				panic(err)
			}
		case 3:
			// Starting here expect requests for each file in the directory
			expectedUri := "/ipfs/bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
				"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
				"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
				"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
				"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
			}); err != nil {
				panic(err)
			}
		case 4:
			// Expect a request for one of the directory items and give it
			expectedUri := "/ipfs/bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", // exampleD
			}); err != nil {
				panic(err)
			}
		case 5:
			// Expect a request for one of the directory items and give it
			expectedUri := "/ipfs/bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", // exampleC
			}); err != nil {
				panic(err)
			}
		case 6:
			// Expect a request for one of the directory items and give part of it
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
			}); err != nil {
				panic(err)
			}
		case 7:
			// Expect a partial request for one of the directory items and give it
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
			}); err != nil {
				panic(err)
			}
		default:
			t.Fatal("unsupported request number")
		}
	}))
	defer s.Close()

	bs := newProxyBlockStore([]string{s.URL}, newCachedDNS(dnsCacheRefreshInterval))
	exch, err := newExchange(bs)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := lib.NewGraphGatewayBackend(&retryFetcher{inner: bs.(lib.CarFetcher), allowedRetries: 3, retriesRemaining: 3}, exch)
	if err != nil {
		t.Fatal(err)
	}

	p, err := gateway.NewImmutablePath(path.New("/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"))
	if err != nil {
		t.Fatal(err)
	}
	_, nd, err := backend.GetAll(ctx, p)
	if err != nil {
		t.Fatal(err)
	}

	assertNextEntryNameEquals := func(t *testing.T, dirIter files.DirIterator, expectedName string) {
		t.Helper()
		if !dirIter.Next() {
			iterErr := dirIter.Err()
			t.Fatalf("expected entry, but errored with %s", iterErr.Error())
		}
		if expectedName != dirIter.Name() {
			t.Fatalf("expected %s, got %s", expectedName, dirIter.Name())
		}
	}

	robs, err := carbs.NewReadOnly(bytes.NewReader(dirWithMultiblockHAMTandFiles), nil)
	if err != nil {
		t.Fatal(err)
	}

	dsrv := merkledag.NewDAGService(blockservice.New(robs, offline.Exchange(robs)))
	assertFileEqual := func(t *testing.T, expectedCidString string, receivedFile files.File) {
		t.Helper()

		expected := cid.MustParse(expectedCidString)
		receivedFileData, err := io.ReadAll(receivedFile)
		if err != nil {
			t.Fatal(err)
		}
		nd, err := dsrv.Get(ctx, expected)
		if err != nil {
			t.Fatal(err)
		}
		expectedFile, err := unixfile.NewUnixfsFile(ctx, dsrv, nd)
		if err != nil {
			t.Fatal(err)
		}

		expectedFileData, err := io.ReadAll(expectedFile.(files.File))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(expectedFileData, receivedFileData) {
			t.Fatalf("expected %s, got %s", string(expectedFileData), string(receivedFileData))
		}
	}

	rootDirIter := nd.(files.Directory).Entries()
	assertNextEntryNameEquals(t, rootDirIter, "basicDir")

	basicDirIter := rootDirIter.Node().(files.Directory).Entries()
	assertNextEntryNameEquals(t, basicDirIter, "exampleA")
	assertFileEqual(t, "bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", basicDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, basicDirIter, "exampleB")
	assertFileEqual(t, "bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", basicDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, rootDirIter, "hamtDir")
	hamtDirIter := rootDirIter.Node().(files.Directory).Entries()

	assertNextEntryNameEquals(t, hamtDirIter, "exampleB")
	assertFileEqual(t, "bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleD-hamt-collide-exampleB-seed-364")
	assertFileEqual(t, "bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleC-hamt-collide-exampleA-seed-52")
	assertFileEqual(t, "bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleA")
	assertFileEqual(t, "bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", hamtDirIter.Node().(files.File))

	if rootDirIter.Next() || basicDirIter.Next() || hamtDirIter.Next() {
		t.Fatal("expected directories to be fully enumerated")
	}
}

func TestTarAtEndOfPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requestNum := 0
	s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNum++
		switch requestNum {
		case 1:
			// Expect the full request, but return one that terminates in the middle of the path
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
			}); err != nil {
				panic(err)
			}
		case 2:
			// Expect the full request and give the path and the children from one of the HAMT nodes but not the other
			// Note: this is an implementation detail, it could be in the future that we request less or more data
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
				"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm",
				"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
				"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
				"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
				"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
				"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
				"bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", // exampleD
			}); err != nil {
				panic(err)
			}
		case 3:
			// Expect a request for the HAMT only and give it
			// Note: this is an implementation detail, it could be in the future that we request less or more data
			// (e.g. requesting the blocks to fill out the HAMT, or with spec changes asking for HAMT ranges, or asking for the HAMT and its children)
			expectedUri := "/ipfs/bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
				"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm",
				"bafybeihjydob4eq5j4m43whjgf5cgftthc42kjno3g24sa3wcw7vonbmfy",
			}); err != nil {
				panic(err)
			}
		case 4:
			// Expect a request for one of the directory items and give it
			expectedUri := "/ipfs/bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", // exampleC
			}); err != nil {
				panic(err)
			}
		case 5:
			// Expect a request for the multiblock file in the directory and give some of it
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
			}); err != nil {
				panic(err)
			}
		case 6:
			// Expect a request for the rest of the multiblock file in the directory and give it
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa?dag-scope=entity&entity-bytes=768:*"
			if request.RequestURI != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
			}); err != nil {
				panic(err)
			}
		default:
			t.Fatal("unsupported request number")
		}
	}))
	defer s.Close()

	bs := newProxyBlockStore([]string{s.URL}, newCachedDNS(dnsCacheRefreshInterval))
	exch, err := newExchange(bs)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := lib.NewGraphGatewayBackend(&retryFetcher{inner: bs.(lib.CarFetcher), allowedRetries: 3, retriesRemaining: 3}, exch)
	if err != nil {
		t.Fatal(err)
	}

	p, err := gateway.NewImmutablePath(path.New("/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir"))
	if err != nil {
		t.Fatal(err)
	}
	_, nd, err := backend.GetAll(ctx, p)
	if err != nil {
		t.Fatal(err)
	}

	assertNextEntryNameEquals := func(t *testing.T, dirIter files.DirIterator, expectedName string) {
		t.Helper()
		if !dirIter.Next() {
			t.Fatal("expected entry")
		}
		if expectedName != dirIter.Name() {
			t.Fatalf("expected %s, got %s", expectedName, dirIter.Name())
		}
	}

	robs, err := carbs.NewReadOnly(bytes.NewReader(dirWithMultiblockHAMTandFiles), nil)
	if err != nil {
		t.Fatal(err)
	}

	dsrv := merkledag.NewDAGService(blockservice.New(robs, offline.Exchange(robs)))
	assertFileEqual := func(t *testing.T, expectedCidString string, receivedFile files.File) {
		t.Helper()

		expected := cid.MustParse(expectedCidString)
		receivedFileData, err := io.ReadAll(receivedFile)
		if err != nil {
			t.Fatal(err)
		}
		nd, err := dsrv.Get(ctx, expected)
		if err != nil {
			t.Fatal(err)
		}
		expectedFile, err := unixfile.NewUnixfsFile(ctx, dsrv, nd)
		if err != nil {
			t.Fatal(err)
		}

		expectedFileData, err := io.ReadAll(expectedFile.(files.File))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(expectedFileData, receivedFileData) {
			t.Fatalf("expected %s, got %s", string(expectedFileData), string(receivedFileData))
		}
	}

	hamtDirIter := nd.(files.Directory).Entries()

	assertNextEntryNameEquals(t, hamtDirIter, "exampleB")
	assertFileEqual(t, "bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleD-hamt-collide-exampleB-seed-364")
	assertFileEqual(t, "bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleC-hamt-collide-exampleA-seed-52")
	assertFileEqual(t, "bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleA")
	assertFileEqual(t, "bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", hamtDirIter.Node().(files.File))

	if hamtDirIter.Next() {
		t.Fatal("expected directories to be fully enumerated")
	}
}

func sendBlocks(ctx context.Context, carFixture []byte, writer io.Writer, cidStrList []string) error {
	rd, err := storage.OpenReadable(bytes.NewReader(carFixture))
	if err != nil {
		return err
	}

	cw, err := storage.NewWritable(writer, []cid.Cid{cid.MustParse("bafkqaaa")}, carv2.WriteAsCarV1(true))
	if err != nil {
		return err
	}

	for _, s := range cidStrList {
		c := cid.MustParse(s)
		blockData, err := rd.Get(ctx, c.KeyString())
		if err != nil {
			return err
		}

		if err := cw.Put(ctx, c.KeyString(), blockData); err != nil {
			return err
		}
	}
	return nil
}

func TestGetCAR(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requestNum := 0
	s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNum++
		switch requestNum {
		case 1:
			// Expect the full request, but return one that terminates in the middle of the path
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
			}); err != nil {
				panic(err)
			}
		case 2:
			// Expect the full request, but return one that terminates in the middle of the HAMT
			// Note: this is an implementation detail, it could be in the future that we request less data (e.g. partial path)
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamt root
			}); err != nil {
				panic(err)
			}

		case 3:
			// Expect the full request and return the full HAMT
			// Note: this is an implementation detail, it could be in the future that we request less data (e.g. requesting the blocks to fill out the HAMT, or with spec changes asking for HAMT ranges)
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
				"bafybeifdv255wmsrh75vcsrtkcwyktvewgihegeeyhhj2ju4lzt4lqfoze", // basicDir
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
				"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
				"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
				"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
				"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
				"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
				"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm",
				"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
				"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
				"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
				"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
				"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
				"bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", // exampleD
				"bafybeihjydob4eq5j4m43whjgf5cgftthc42kjno3g24sa3wcw7vonbmfy",
				"bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", // exampleC
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
			}); err != nil {
				panic(err)
			}

		default:
			t.Fatal("unsupported request number")
		}
	}))
	defer s.Close()

	bs := newProxyBlockStore([]string{s.URL}, newCachedDNS(dnsCacheRefreshInterval))
	exch, err := newExchange(bs)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := lib.NewGraphGatewayBackend(&retryFetcher{inner: bs.(lib.CarFetcher), allowedRetries: 3, retriesRemaining: 3}, exch)
	if err != nil {
		t.Fatal(err)
	}

	p, err := gateway.NewImmutablePath(path.New("/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"))
	if err != nil {
		t.Fatal(err)
	}
	var carReader io.Reader
	_, carReader, err = backend.GetCAR(ctx, p, gateway.CarParams{Scope: gateway.DagScopeAll})
	if err != nil {
		t.Fatal(err)
	}

	carBytes, err := io.ReadAll(carReader)
	if err != nil {
		t.Fatal(err)
	}
	carReader = bytes.NewReader(carBytes)

	blkReader, err := carv2.NewBlockReader(carReader)
	if err != nil {
		t.Fatal(err)
	}

	responseCarBlock := []string{
		"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
		"bafybeifdv255wmsrh75vcsrtkcwyktvewgihegeeyhhj2ju4lzt4lqfoze", // basicDir
		"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
		"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
		"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
		"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
		"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
		"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
		"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
		"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
		"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
		"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
		"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
		"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
		"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm",
		"bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", // exampleD
		"bafybeihjydob4eq5j4m43whjgf5cgftthc42kjno3g24sa3wcw7vonbmfy",
		"bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", // exampleC
	}

	for i := 0; i < len(responseCarBlock); i++ {
		expectedCid := cid.MustParse(responseCarBlock[i])
		blk, err := blkReader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !blk.Cid().Equals(expectedCid) {
			t.Fatalf("expected cid %s, got %s", expectedCid, blk.Cid())
		}
	}
	_, err = blkReader.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatal("expected an EOF")
	}
}

type retryFetcher struct {
	inner            lib.CarFetcher
	allowedRetries   int
	retriesRemaining int
}

func (r *retryFetcher) Fetch(ctx context.Context, path string, cb lib.DataCallback) error {
	err := r.inner.Fetch(ctx, path, cb)
	if err == nil {
		return nil
	}

	if r.retriesRemaining > 0 {
		r.retriesRemaining--
	} else {
		return fmt.Errorf("retry fetcher out of retries: %w", err)
	}

	switch t := err.(type) {
	case *lib.ErrPartialResponse:
		if len(t.StillNeed) > 1 {
			panic("only a single request at a time supported")
		}

		// Mimicking the Caboose logic reset the number of retries for partials
		r.retriesRemaining = r.allowedRetries

		return r.Fetch(ctx, t.StillNeed[0], cb)
	default:
		return r.Fetch(ctx, path, cb)
	}
}

var _ lib.CarFetcher = (*retryFetcher)(nil)

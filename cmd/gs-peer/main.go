package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/ipfs/go-cid"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"ipfs-streaming-bench/pkg/discovery"
	"ipfs-streaming-bench/pkg/store"
)

func main() {
	var (
		storeDir  = flag.String("store", "./data", "data directory created by seed")
		seedDir   = flag.String("seed-dir", "", "seed directory containing manifest and blocks")
		listen    = flag.String("listen", "/ip4/0.0.0.0/udp/4001/quic-v1", "libp2p listen multiaddr")
		status    = flag.String("status-addr", ":7001", "HTTP status listen address (empty disables)")
		rootFlag  = flag.String("root", "", "root CID (optional, read from manifest if empty)")
		bootstrap = flag.String("bootstrap", "", "comma-separated bootstrap multiaddrs")
		identity  = flag.String("identity", "", "path to libp2p private key (optional)")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seedPath := *storeDir
	if *seedDir != "" {
		seedPath = *seedDir
	}
	fileStore := store.NewFileBlockstore(filepath.Join(seedPath, "blocks"))
	linkSystem := storeutil.LinkSystemForBlockstore(fileStore)

	host, err := newHost(*listen, *identity)
	if err != nil {
		log.Fatalf("failed to start libp2p host: %v", err)
	}
	defer host.Close()

	network := gsnet.NewFromLibp2pHost(host)
	_ = gsimpl.New(ctx, network, linkSystem)

	rootCID, err := resolveRoot(*rootFlag, filepath.Join(seedPath, "manifest.json"))
	if err != nil {
		log.Fatalf("failed to resolve root CID: %v", err)
	}
	if err := store.ValidateRoot(ctx, linkSystem, rootCID); err != nil {
		log.Fatalf("invalid root blocks for %s: %v", rootCID, err)
	}
	startStatusServer(*status, host.ID().String(), rootCID.String())

	bootstrapAddrs := parseBootstrap(*bootstrap)
	dhtNode, err := dht.New(ctx, host, dht.Mode(dht.ModeServer))
	if err != nil {
		log.Fatalf("failed to start DHT: %v", err)
	}
	if err := dhtNode.Bootstrap(ctx); err != nil {
		log.Printf("DHT bootstrap error: %v", err)
	}
	if err := connectBootstrap(ctx, host, bootstrapAddrs); err != nil {
		log.Printf("bootstrap connect error: %v", err)
	}
	if err := dhtNode.Provide(ctx, rootCID, true); err != nil {
		log.Printf("failed to provide CID: %v", err)
	} else {
		log.Printf("Provided root CID on DHT: %s", rootCID.String())
	}

	log.Printf("GraphSync peer ready: %s", host.ID())
	for _, addr := range host.Addrs() {
		log.Printf("Listening on: %s/p2p/%s", addr, host.ID())
	}
	waitForSignal()
}

func startStatusServer(addr string, peerID string, root string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/id", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"peerId": peerID,
			"root":   root,
		})
	})
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("status server failed: %v", err)
		}
	}()
}

func newHost(listen string, identityPath string) (host.Host, error) {
	addr, err := multiaddr.NewMultiaddr(listen)
	if err != nil {
		return nil, err
	}
	options := []libp2p.Option{libp2p.ListenAddrs(addr)}
	if identityPath != "" {
		privKey, err := loadIdentity(identityPath)
		if err != nil {
			return nil, err
		}
		options = append(options, libp2p.Identity(privKey))
	}
	return libp2p.New(options...)
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	fmt.Println("Shutting down peer")
}

func loadIdentity(path string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	privKey, err := crypto.UnmarshalPrivateKey(data)
	if err != nil {
		return nil, err
	}
	return privKey, nil
}

type manifest struct {
	Root string `json:"root"`
}

func resolveRoot(flagValue string, manifestPath string) (cid.Cid, error) {
	if flagValue != "" {
		return cid.Parse(flagValue)
	}
	payload, err := os.ReadFile(manifestPath)
	if err != nil {
		return cid.Undef, err
	}
	var data manifest
	if err := json.Unmarshal(payload, &data); err != nil {
		return cid.Undef, err
	}
	if data.Root == "" {
		return cid.Undef, fmt.Errorf("manifest missing root")
	}
	return cid.Parse(data.Root)
}

func parseBootstrap(value string) []string {
	if strings.TrimSpace(value) == "" {
		return discovery.DefaultBootstrapAddrs
	}
	parts := strings.Split(value, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			addrs = append(addrs, trimmed)
		}
	}
	return addrs
}

func connectBootstrap(ctx context.Context, host host.Host, addrs []string) error {
	for _, addr := range addrs {
		multiaddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return err
		}
		info, err := peer.AddrInfoFromP2pAddr(multiaddr)
		if err != nil {
			return err
		}
		if err := host.Connect(ctx, *info); err != nil {
			log.Printf("bootstrap connect failed: %v", err)
		}
	}
	return nil
}

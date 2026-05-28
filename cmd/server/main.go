package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"path/filepath"

	"github.com/multiformats/go-multiaddr"
	"ipfs-streaming-bench/pkg/store"
	"ipfs-streaming-bench/pkg/transport"
)

func main() {
	var (
		storeDir = flag.String("store", "./data", "data directory created by seed")
		listen   = flag.String("listen", "/ip4/127.0.0.1/tcp/4001", "listen multiaddr")
	)
	flag.Parse()

	addr, err := parseListen(*listen)
	if err != nil {
		log.Fatalf("failed to parse listen address: %v", err)
	}
	blocksDir := filepath.Join(*storeDir, "blocks")
	fileStore := store.NewFileStore(blocksDir)
	server := &transport.Server{Store: fileStore, Logger: log.Printf}

	log.Printf("Mock selector server listening on %s", addr)
	if err := http.ListenAndServe(addr, server.Handler()); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func parseListen(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("empty listen address")
	}
	if value[0] != '/' {
		return value, nil
	}
	maddr, err := multiaddr.NewMultiaddr(value)
	if err != nil {
		return "", err
	}
	ip, err := maddr.ValueForProtocol(multiaddr.P_IP4)
	if err != nil {
		return "", fmt.Errorf("multiaddr missing ip4: %w", err)
	}
	port, err := maddr.ValueForProtocol(multiaddr.P_TCP)
	if err != nil {
		return "", fmt.Errorf("multiaddr missing tcp: %w", err)
	}
	return fmt.Sprintf("%s:%s", ip, port), nil
}

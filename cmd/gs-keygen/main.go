package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"os"

	"github.com/libp2p/go-libp2p/core/crypto"
)

const PrivateFile os.FileMode = 0o600

func main() {
	out := flag.String("out", "peer.key", "output key file")
	flag.Parse()

	privKey, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, -1, rand.Reader)
	if err != nil {
		fmt.Printf("failed to generate key: %v\n", err)
		os.Exit(1)
	}
	data, err := crypto.MarshalPrivateKey(privKey)
	if err != nil {
		fmt.Printf("failed to marshal key: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, data, PrivateFile); err != nil {
		fmt.Printf("failed to write key: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Wrote key to %s\n", *out)
}

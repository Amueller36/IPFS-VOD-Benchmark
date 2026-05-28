package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"ipfs-streaming-bench/pkg/posl"
	"ipfs-streaming-bench/pkg/store"
)

type manifest struct {
	Root        string `json:"root"`
	Size        int64  `json:"size"`
	ChunkSize   int64  `json:"chunkSize"`
	SegmentSize int64  `json:"segmentSize"`
	Segments    int    `json:"segments"`
	Chunks      int    `json:"chunks"`
}

func buildLayout(ctx context.Context, filePath string, sizeMB int64, chunkSize int64, segmentSize int64, store store.BlockStore) (posl.Layout, error) {
	if filePath != "" {
		file, err := os.Open(filePath)
		if err != nil {
			return posl.Layout{}, err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return posl.Layout{}, err
		}
		return posl.BuildLayoutFromReader(ctx, file, info.Size(), chunkSize, segmentSize, store)
	}
	dataSize := sizeMB * 1024 * 1024
	data := make([]byte, dataSize)
	seed := rand.New(rand.NewSource(time.Now().UnixNano()))
	if _, err := seed.Read(data); err != nil {
		return posl.Layout{}, err
	}
	return posl.BuildLayout(ctx, data, chunkSize, segmentSize, store)
}

func main() {
	var (
		sizeMB     = flag.Int("size-mb", 50, "size of fake video in MB")
		chunkKB    = flag.Int("chunk-kb", 256, "chunk size in KB")
		segmentKB  = flag.Int("segment-kb", 512, "segment size in KB")
		layoutType = flag.String("layout", "posl", "layout type")
		outDir     = flag.String("out", "./data", "output directory")
		filePath   = flag.String("file", "", "optional input file to seed")
	)
	flag.Parse()

	if *layoutType != "posl" && *layoutType != "fbl" {
		fmt.Printf("unsupported layout: %s\n", *layoutType)
		os.Exit(1)
	}

	blocksDir := filepath.Join(*outDir, "blocks")
	store := store.NewFileStore(blocksDir)

	ctx := context.Background()
	layout, err := buildLayout(ctx, *filePath, int64(*sizeMB), int64(*chunkKB)*1024, int64(*segmentKB)*1024, store)
	if err != nil {
		fmt.Printf("failed to build layout: %v\n", err)
		os.Exit(1)
	}

	manifestPath := filepath.Join(*outDir, "manifest.json")
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Printf("failed to create output dir: %v\n", err)
		os.Exit(1)
	}
	payload := manifest{
		Root:        layout.Root.String(),
		Size:        layout.Size,
		ChunkSize:   layout.ChunkSize,
		SegmentSize: layout.SegmentSize,
		Segments:    layout.SegmentCount,
		Chunks:      layout.ChunkCount,
	}
	tmpManifestPath := manifestPath + ".tmp"
	_ = os.Remove(tmpManifestPath)
	file, err := os.Create(tmpManifestPath)
	if err != nil {
		fmt.Printf("failed to write manifest: %v\n", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(payload); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpManifestPath)
		fmt.Printf("failed to encode manifest: %v\n", err)
		os.Exit(1)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpManifestPath)
		fmt.Printf("failed to close manifest: %v\n", err)
		os.Exit(1)
	}
	if err := os.Rename(tmpManifestPath, manifestPath); err != nil {
		_ = os.Remove(tmpManifestPath)
		fmt.Printf("failed to publish manifest: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("RootCID: %s\n", layout.Root.String())
	fmt.Printf("Blocks: chunks=%d segments=%d\n", layout.ChunkCount, layout.SegmentCount)
	actualSizeMB := float64(layout.Size) / (1024 * 1024)
	fmt.Printf("Layout: chunk=%dKB segment=%dKB size=%.2fMB\n", *chunkKB, *segmentKB, actualSizeMB)
	fmt.Printf("Manifest: %s\n", manifestPath)
}

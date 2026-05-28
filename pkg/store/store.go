package store

import (
	"context"
	"os"
	"path/filepath"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type BlockStore interface {
	Get(ctx context.Context, cid cid.Cid) (blocks.Block, error)
	Put(ctx context.Context, block blocks.Block) error
}

type FileStore struct {
	dir string
}

func NewFileStore(dir string) *FileStore {
	return &FileStore{dir: dir}
}

func (f *FileStore) Get(_ context.Context, cid cid.Cid) (blocks.Block, error) {
	path := filepath.Join(f.dir, cid.String())
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(data, cid)
}

func (f *FileStore) Put(_ context.Context, block blocks.Block) error {
	if err := os.MkdirAll(f.dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(f.dir, block.Cid().String())
	return os.WriteFile(path, block.RawData(), 0o644)
}

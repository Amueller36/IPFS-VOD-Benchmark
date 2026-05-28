package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	blockstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

type FileBlockstore struct {
	dir string
}

func NewFileBlockstore(dir string) *FileBlockstore {
	return &FileBlockstore{dir: dir}
}

var _ blockstore.Blockstore = (*FileBlockstore)(nil)

func (f *FileBlockstore) DeleteBlock(_ context.Context, cid cid.Cid) error {
	path := filepath.Join(f.dir, cid.String())
	return os.Remove(path)
}

func (f *FileBlockstore) Has(_ context.Context, cid cid.Cid) (bool, error) {
	path := filepath.Join(f.dir, cid.String())
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (f *FileBlockstore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	return f.readBlock(ctx, cid)
}

func (f *FileBlockstore) GetSize(_ context.Context, cid cid.Cid) (int, error) {
	path := filepath.Join(f.dir, cid.String())
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return int(info.Size()), nil
}

func (f *FileBlockstore) Put(ctx context.Context, block blocks.Block) error {
	if err := os.MkdirAll(f.dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(f.dir, block.Cid().String())
	return os.WriteFile(path, block.RawData(), 0o644)
}

func (f *FileBlockstore) PutMany(ctx context.Context, blocksList []blocks.Block) error {
	for _, block := range blocksList {
		if err := f.Put(ctx, block); err != nil {
			return err
		}
	}
	return nil
}

func (f *FileBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, err
	}
	out := make(chan cid.Cid)
	go func() {
		defer close(out)
		for _, entry := range entries {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if entry.IsDir() {
				continue
			}
			cidValue, err := cid.Parse(entry.Name())
			if err != nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case out <- cidValue:
			}
		}
	}()
	return out, nil
}

func (f *FileBlockstore) readBlock(_ context.Context, cid cid.Cid) (blocks.Block, error) {
	path := filepath.Join(f.dir, cid.String())
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, err := blocks.NewBlockWithCid(data, cid)
	if err != nil {
		return nil, fmt.Errorf("invalid block for %s: %w", cid, err)
	}
	return block, nil
}

func ValidateRoot(ctx context.Context, linkSystem ipld.LinkSystem, root cid.Cid) error {
	_, err := linkSystem.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: root}, basicnode.Prototype.Any)
	return err
}

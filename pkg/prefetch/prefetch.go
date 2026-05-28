package prefetch

import (
	"context"
	"fmt"
	"sync"
)

type SegmentFetcher interface {
	Fetch(ctx context.Context, index int) ([]byte, error)
}

type SegmentRescuer interface {
	ShouldRescue(index int) bool
	RescueFetch(ctx context.Context, index int) ([]byte, error)
}

type Prefetcher struct {
	fetcher          SegmentFetcher
	prefetchSegments int
	workers          int
	maxIndex         int

	mu             sync.RWMutex
	cache          map[int][]byte
	hits           int
	misses         int
	inflight       map[int]struct{}
	inflightWait   map[int]chan struct{}
	inflightCancel map[int]context.CancelFunc
	inflightErr    map[int]error
	rescued        map[int]struct{}

	queue  chan int
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewWithWorkers(fetcher SegmentFetcher, prefetchSegments int, workers int) *Prefetcher {
	if workers <= 0 {
		workers = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Prefetcher{
		fetcher:          fetcher,
		prefetchSegments: prefetchSegments,
		workers:          workers,
		maxIndex:         -1,
		cache:            make(map[int][]byte),
		inflight:         make(map[int]struct{}),
		inflightWait:     make(map[int]chan struct{}),
		inflightCancel:   make(map[int]context.CancelFunc),
		inflightErr:      make(map[int]error),
		rescued:          make(map[int]struct{}),
		queue:            make(chan int, 128),
		ctx:              ctx,
		cancel:           cancel,
	}
}

func (p *Prefetcher) SetMaxIndex(maxIndex int) {
	p.mu.Lock()
	p.maxIndex = maxIndex
	p.mu.Unlock()
}

func (p *Prefetcher) Start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case <-p.ctx.Done():
					return
				case index := <-p.queue:
					p.fetchAndStore(p.ctx, index)
				}
			}
		}()
	}
}

func (p *Prefetcher) Stop() {
	p.cancel()
	p.wg.Wait()
}

func (p *Prefetcher) Get(ctx context.Context, index int) ([]byte, bool, error) {
	p.mu.RLock()
	cached, ok := p.cache[index]
	p.mu.RUnlock()
	if ok {
		p.mu.Lock()
		p.hits++
		p.mu.Unlock()
		p.enqueuePrefetch(index + 1)
		return cached, true, nil
	}

	p.mu.Lock()
	p.misses++
	if waitCh, inflight := p.inflightWait[index]; inflight {
		if rescuer, ok := p.fetcher.(SegmentRescuer); ok && rescuer.ShouldRescue(index) {
			if p.markRescueLocked(index) {
				p.mu.Unlock()
				data, hit, err := p.rescueAndWait(ctx, index, waitCh, rescuer)
				if err == nil {
					p.enqueuePrefetch(index + 1)
				}
				return data, hit, err
			}
		}
		p.mu.Unlock()
		data, hit, err := p.waitForInflight(ctx, index, waitCh)
		if err == nil {
			p.enqueuePrefetch(index + 1)
		}
		return data, hit, err
	}
	waitCh := make(chan struct{})
	p.inflightWait[index] = waitCh
	p.inflight[index] = struct{}{}
	p.mu.Unlock()

	data, err := p.fetchAndStore(ctx, index)
	if err != nil {
		return nil, false, err
	}
	p.enqueuePrefetch(index + 1)
	return data, false, nil
}

func (p *Prefetcher) RequestRescue(index int) bool {
	rescuer, ok := p.fetcher.(SegmentRescuer)
	if !ok || !rescuer.ShouldRescue(index) {
		return false
	}
	p.mu.Lock()
	waitCh, inflight := p.inflightWait[index]
	if !inflight {
		p.mu.Unlock()
		return false
	}
	if !p.markRescueLocked(index) {
		p.mu.Unlock()
		return false
	}
	p.mu.Unlock()
	go func() {
		_, _, _ = p.rescueAndWait(p.ctx, index, waitCh, rescuer)
	}()
	return true
}

func (p *Prefetcher) RescueInflightRange(start int, end int) int {
	if end < start {
		return 0
	}
	p.mu.RLock()
	indexes := make([]int, 0, end-start+1)
	for index := start; index <= end; index++ {
		if _, inflight := p.inflightWait[index]; inflight {
			indexes = append(indexes, index)
		}
	}
	p.mu.RUnlock()

	for _, index := range indexes {
		if p.RequestRescue(index) {
			return 1
		}
	}
	return 0
}

func (p *Prefetcher) enqueuePrefetch(start int) {
	if p.prefetchSegments <= 0 {
		return
	}
	for i := 0; i < p.prefetchSegments; i++ {
		index := start + i
		p.mu.Lock()
		if p.maxIndex >= 0 && index > p.maxIndex {
			p.mu.Unlock()
			break
		}
		if _, ok := p.cache[index]; ok {
			p.mu.Unlock()
			continue
		}
		if _, ok := p.inflight[index]; ok {
			p.mu.Unlock()
			continue
		}
		p.inflight[index] = struct{}{}
		p.inflightWait[index] = make(chan struct{})
		p.mu.Unlock()
		select {
		case p.queue <- index:
		default:
			p.mu.Lock()
			delete(p.inflight, index)
			delete(p.inflightWait, index)
			p.mu.Unlock()
		}
	}
}

func (p *Prefetcher) fetchAndStore(ctx context.Context, index int) ([]byte, error) {
	fetchCtx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	if data, ok := p.cache[index]; ok {
		delete(p.inflight, index)
		delete(p.rescued, index)
		delete(p.inflightCancel, index)
		p.mu.Unlock()
		cancel()
		return data, nil
	}
	if _, inflight := p.inflight[index]; inflight {
		p.inflightCancel[index] = cancel
	}
	p.mu.Unlock()

	data, err := p.fetcher.Fetch(fetchCtx, index)
	cancel()

	p.mu.Lock()
	if cached, ok := p.cache[index]; ok {
		delete(p.inflight, index)
		delete(p.rescued, index)
		delete(p.inflightErr, index)
		delete(p.inflightCancel, index)
		if waitCh, ok := p.inflightWait[index]; ok {
			close(waitCh)
			delete(p.inflightWait, index)
		}
		p.mu.Unlock()
		return cached, nil
	}
	delete(p.inflight, index)
	delete(p.rescued, index)
	delete(p.inflightCancel, index)
	if err == nil {
		delete(p.inflightErr, index)
		if _, cached := p.cache[index]; !cached {
			p.cache[index] = data
		}
	} else if _, cached := p.cache[index]; !cached {
		p.inflightErr[index] = err
	}
	if waitCh, ok := p.inflightWait[index]; ok {
		close(waitCh)
		delete(p.inflightWait, index)
	}
	p.mu.Unlock()
	return data, err
}

func (p *Prefetcher) markRescueLocked(index int) bool {
	if _, already := p.rescued[index]; already {
		return false
	}
	p.rescued[index] = struct{}{}
	return true
}

func (p *Prefetcher) rescueAndWait(ctx context.Context, index int, waitCh <-chan struct{}, rescuer SegmentRescuer) ([]byte, bool, error) {
	type rescueResult struct {
		data []byte
		err  error
	}
	rescueCh := make(chan rescueResult, 1)
	go func() {
		data, err := rescuer.RescueFetch(ctx, index)
		rescueCh <- rescueResult{data: data, err: err}
	}()

	rescueDone := false
	for {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-waitCh:
			return p.consumeInflightResult(index)
		case result := <-rescueCh:
			rescueDone = true
			if result.err == nil {
				p.storeRescueResult(index, result.data)
				return result.data, false, nil
			}
		}
		if rescueDone {
			data, hit, err := p.waitForInflight(ctx, index, waitCh)
			return data, hit, err
		}
	}
}

func (p *Prefetcher) storeRescueResult(index int, data []byte) {
	p.mu.Lock()
	p.cache[index] = data
	delete(p.inflight, index)
	delete(p.rescued, index)
	delete(p.inflightErr, index)
	if cancel, ok := p.inflightCancel[index]; ok {
		cancel()
		delete(p.inflightCancel, index)
	}
	if waitCh, ok := p.inflightWait[index]; ok {
		close(waitCh)
		delete(p.inflightWait, index)
	}
	p.mu.Unlock()
}

func (p *Prefetcher) waitForInflight(ctx context.Context, index int, waitCh <-chan struct{}) ([]byte, bool, error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case <-waitCh:
	}
	return p.consumeInflightResult(index)
}

func (p *Prefetcher) consumeInflightResult(index int) ([]byte, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if data, ok := p.cache[index]; ok {
		return data, false, nil
	}
	if err, ok := p.inflightErr[index]; ok {
		delete(p.inflightErr, index)
		return nil, false, err
	}
	return nil, false, fmt.Errorf("segment %d not available", index)
}

func (p *Prefetcher) HitRate() float64 {
	p.mu.RLock()
	hits := p.hits
	misses := p.misses
	p.mu.RUnlock()
	if hits+misses == 0 {
		return 0
	}
	return float64(hits) / float64(hits+misses)
}

func (p *Prefetcher) CachedCount() int {
	p.mu.RLock()
	count := len(p.cache)
	p.mu.RUnlock()
	return count
}

func (p *Prefetcher) CachedRange(start int, end int) bool {
	if end < start {
		return true
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for index := start; index <= end; index++ {
		if _, ok := p.cache[index]; !ok {
			return false
		}
	}
	return true
}

func (p *Prefetcher) CoveredRange(start int, end int) bool {
	if end < start {
		return true
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for index := start; index <= end; index++ {
		if _, ok := p.cache[index]; ok {
			continue
		}
		if _, ok := p.inflight[index]; ok {
			continue
		}
		return false
	}
	return true
}

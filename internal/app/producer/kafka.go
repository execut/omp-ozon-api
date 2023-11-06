package producer

import (
    "context"
    "github.com/execut/omp-ozon-api/internal/app/repo"
    "github.com/execut/omp-ozon-api/internal/app/sender"
    "github.com/execut/omp-ozon-api/internal/model"
    "github.com/gammazero/workerpool"
    "sync"
)

type Producer interface {
    Start()
    Close()
}

func NewProducer(eventCh chan *model.KeywordEvent, sender sender.EventSender, producersCount uint64,
    workerPool *workerpool.WorkerPool, repo repo.EventRepo) Producer {
    wg := sync.WaitGroup{}
    ctx, cancel := context.WithCancel(context.Background())
    return &producer{eventCh, sender, producersCount, ctx, cancel, &wg, workerPool, repo}
}

type producer struct {
    eventCh        chan *model.KeywordEvent
    sender         sender.EventSender
    producersCount uint64
    ctx            context.Context
    cancel         context.CancelFunc
    wg             *sync.WaitGroup
    workerPool     *workerpool.WorkerPool
    repo           repo.EventRepo
}

func (p *producer) Start() {
    p.wg.Add(int(p.producersCount))
    for i := uint64(0); i < p.producersCount; i++ {
        go func() {
            for {
                select {
                case <-p.ctx.Done():
                    p.wg.Done()
                    return
                case event, ok := <-p.eventCh:
                    if !ok {
                        return
                    }
                    p.wg.Add(1)
                    if err := p.sender.Send(event); err == nil {
                        p.workerPool.Submit(func() {
                            ids := []uint64{event.ID}
                            p.repo.Remove(ids)
                            p.wg.Done()
                        })
                    } else {
                        p.workerPool.Submit(func() {
                            ids := []uint64{event.ID}
                            p.repo.Unlock(ids)
                            p.wg.Done()
                        })
                    }
                }
            }
        }()
    }
}

func (p *producer) Close() {
    p.cancel()
    p.wg.Wait()
}

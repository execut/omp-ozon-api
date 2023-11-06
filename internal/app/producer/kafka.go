package producer

import (
    "context"
    "fmt"
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
    workerPool *workerpool.WorkerPool) Producer {
    wg := sync.WaitGroup{}
    ctx, cancel := context.WithCancel(context.Background())
    return &producer{eventCh, sender, producersCount, ctx, cancel, &wg, workerPool}
}

type producer struct {
    eventCh        chan *model.KeywordEvent
    sender         sender.EventSender
    producersCount uint64
    ctx            context.Context
    cancel         context.CancelFunc
    wg             *sync.WaitGroup
    workerPool     *workerpool.WorkerPool
}

func (p *producer) Start() {
    for i := uint64(0); i < p.producersCount; i++ {
        p.wg.Add(1)
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
                    if err := p.sender.Send(event); err == nil {
                        p.workerPool.Submit(func() {
                            fmt.Printf("%v event processed\n", event.ID)
                        })
                    } else {
                        p.workerPool.Submit(func() {
                            fmt.Printf("%v event failed\n", event.ID)
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

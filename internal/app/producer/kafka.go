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
    workerPool *workerpool.WorkerPool, repo repo.EventRepo, workerpoolBatchSize uint64) Producer {
    wg := sync.WaitGroup{}
    ctx, cancel := context.WithCancel(context.Background())
    failedEventsIds := make(chan uint64, workerpoolBatchSize)
    successEventsIds := make(chan uint64, workerpoolBatchSize)
    batchWg := sync.WaitGroup{}
    return &producer{eventCh, sender, producersCount, ctx, cancel, &wg,
        workerPool, repo, workerpoolBatchSize, failedEventsIds, successEventsIds, &batchWg}
}

type producer struct {
    eventCh             chan *model.KeywordEvent
    sender              sender.EventSender
    producersCount      uint64
    ctx                 context.Context
    cancel              context.CancelFunc
    wg                  *sync.WaitGroup
    workerPool          *workerpool.WorkerPool
    repo                repo.EventRepo
    workerpoolBatchSize uint64
    failedEventsIds     chan uint64
    successEventsIds    chan uint64
    batchWg             *sync.WaitGroup
}

func (p *producer) Start() {
    p.wg.Add(int(p.producersCount))
    for i := uint64(0); i < p.producersCount; i++ {
        go func() {
            for {
                select {
                case event, ok := <-p.eventCh:
                    if !ok {
                        continue
                    }

                    if err := p.sender.Send(event); err == nil {
                        p.successEventsIds <- event.ID
                        if uint64(len(p.successEventsIds)) == p.workerpoolBatchSize {
                            p.processSuccessJobs()
                        }
                    } else {
                        p.failedEventsIds <- event.ID
                        if uint64(len(p.failedEventsIds)) == p.workerpoolBatchSize {
                            p.processFailedJobs()
                        }
                    }
                case <-p.ctx.Done():
                    if len(p.eventCh) != 0 {
                        continue
                    }

                    p.wg.Done()
                    return
                }
            }
        }()
    }
}

func (p *producer) Close() {
    p.cancel()
    p.wg.Wait()

    if len(p.successEventsIds) > 0 {
        p.processSuccessJobs()
    }
    if len(p.failedEventsIds) > 0 {
        p.processFailedJobs()
    }

    p.wg.Wait()
}

func (p *producer) processFailedJobs() {
    p.sendIdsToRepo(p.failedEventsIds, func(ids []uint64) {
        p.repo.Unlock(ids)
    })
}

func (p *producer) processSuccessJobs() {
    p.sendIdsToRepo(p.successEventsIds, func(ids []uint64) {
        p.repo.Remove(ids)
    })
}

func (p *producer) sendIdsToRepo(eventsIdsCh chan uint64, sendIds func(ids []uint64)) {
    p.wg.Add(1)
    ids := []uint64{}
    idsLen := len(eventsIdsCh)
    for i := 0; i < idsLen; i++ {
        ids = append(ids, <-eventsIdsCh)
    }

    p.workerPool.Submit(func() {
        defer p.wg.Done()
        sendIds(ids)
    })
}

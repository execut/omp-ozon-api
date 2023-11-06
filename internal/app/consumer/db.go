package consumer

import (
    "context"
    "github.com/execut/omp-ozon-api/internal/app/repo"
    "github.com/execut/omp-ozon-api/internal/model"
    "sync"
    "time"
)

type Consumer interface {
    Start()
    Close()
}

func NewConsumer(consumersCount uint64, batchSize uint64, eventCh chan<- *model.KeywordEvent, repo repo.EventRepo, tickDuration time.Duration) Consumer {
    timeoutContext, cancelFunc := context.WithCancel(context.Background())
    wg := sync.WaitGroup{}

    return &consumer{batchSize: batchSize, eventCh: eventCh, repo: repo, consumersCount: consumersCount, timeoutContext: timeoutContext, cancelFunc: cancelFunc, wg: wg, tickDuration: tickDuration}
}

type consumer struct {
    cancelFunc     context.CancelFunc
    timeoutContext context.Context
    consumersCount uint64
    batchSize      uint64
    eventCh        chan<- *model.KeywordEvent
    repo           repo.EventRepo
    tickDuration   time.Duration
    wg             sync.WaitGroup
}

func (c *consumer) Start() {
    for i := uint64(0); i < c.consumersCount; i++ {
        c.wg.Add(1)
        ticker := time.NewTicker(c.tickDuration)
        go func(ticker *time.Ticker) {
            defer c.wg.Done()
            for {
                events, _ := c.repo.Lock(c.batchSize)
                for i, _ := range events {
                    c.eventCh <- &events[i]
                }

                select {
                case <-c.timeoutContext.Done():
                    return
                case <-ticker.C:
                }
            }
        }(ticker)
    }
}

func (c *consumer) Close() {
    c.cancelFunc()
    c.wg.Wait()
}

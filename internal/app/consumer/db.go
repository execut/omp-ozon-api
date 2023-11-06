package consumer

import (
    "context"
    "github.com/execut/omp-ozon-api/internal/app/repo"
    "github.com/execut/omp-ozon-api/internal/model"
    "time"
)

type Consumer interface {
    Start()
    Close()
}

func NewConsumer(consumersCount uint64, batchSize uint64, eventCh chan<- *model.KeywordEvent, repo repo.EventRepo, tickDuration time.Duration) Consumer {
    ticker := time.NewTicker(tickDuration)
    timeoutContext, cancelFunc := context.WithCancel(context.Background())
    return &consumer{batchSize: batchSize, eventCh: eventCh, repo: repo, ticker: ticker, consumersCount: consumersCount, timeoutContext: timeoutContext, cancelFunc: cancelFunc}
}

type consumer struct {
    cancelFunc     context.CancelFunc
    timeoutContext context.Context
    consumersCount uint64
    batchSize      uint64
    eventCh        chan<- *model.KeywordEvent
    repo           repo.EventRepo
    ticker         *time.Ticker
}

func (c *consumer) Start() {
    for i := uint64(0); i < c.consumersCount; i++ {
        go func() {
            for {
                events, _ := c.repo.Lock(c.batchSize)
                for i, _ := range events {
                    c.eventCh <- &events[i]
                }

                select {
                case <-c.timeoutContext.Done():
                    return
                case <-c.ticker.C:
                }
            }
        }()
    }
}

func (c *consumer) Close() {
    c.cancelFunc()
}

package consumer

import (
    "github.com/execut/omp-ozon-api/internal/app/repo"
    "github.com/execut/omp-ozon-api/internal/model"
)

type Consumer interface {
    Start()
    Close()
}

func NewConsumer(batchSize uint64, eventCh chan *model.KeywordEvent, repo repo.EventRepo) Consumer {
    return &consumer{batchSize: batchSize, eventCh: eventCh, repo: repo}
}

type consumer struct {
    batchSize uint64
    eventCh   chan *model.KeywordEvent
    repo      repo.EventRepo
}

func (c *consumer) Start() {
    defer close(c.eventCh)
}

func (c *consumer) Close() {
    //TODO implement me
    panic("implement me")
}

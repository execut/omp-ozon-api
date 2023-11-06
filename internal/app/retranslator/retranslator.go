package retranslator

import (
    "github.com/execut/omp-ozon-api/internal/app/consumer"
    "github.com/execut/omp-ozon-api/internal/app/producer"
    "github.com/execut/omp-ozon-api/internal/app/repo"
    "github.com/execut/omp-ozon-api/internal/app/sender"
    "github.com/execut/omp-ozon-api/internal/model"
    "github.com/gammazero/workerpool"
    "time"
)

type Retranslator interface {
    Start()
    Close()
}

type retranslator struct {
    consumer consumer.Consumer
    producer producer.Producer
    eventCh  chan *model.KeywordEvent
}

func (r *retranslator) Start() {
    r.consumer.Start()
    r.producer.Start()

}

func (r *retranslator) Close() {
    r.consumer.Close()
    r.producer.Close()
}

func NewRetranslator(config Config) Retranslator {
    eventCh := make(chan *model.KeywordEvent)
    newProducer := producer.NewProducer(eventCh, config.sender, config.producersCount, workerpool.New(1))
    r := retranslator{consumer: consumer.NewConsumer(config.consumersCount, config.consumerBatchSize, eventCh, config.repo, config.consumerInterval), eventCh: eventCh, producer: newProducer}

    return &r
}

type Config struct {
    repo              repo.EventRepo
    sender            sender.EventSender
    consumersCount    uint64
    consumerBatchSize uint64
    consumerInterval  time.Duration
    producersCount    uint64
}

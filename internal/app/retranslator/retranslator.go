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
    consumer   consumer.Consumer
    producer   producer.Producer
    eventCh    chan *model.KeywordEvent
    workerPool *workerpool.WorkerPool
}

func (r *retranslator) Start() {
    r.consumer.Start()
    r.producer.Start()

}

func (r *retranslator) Close() {
    r.consumer.Close()
    r.producer.Close()
    //r.workerPool.StopWait()
}

func NewRetranslator(config Config) Retranslator {
    eventCh := make(chan *model.KeywordEvent, config.ChannelSize)
    workerPool := workerpool.New(config.WorkerCount)
    newProducer := producer.NewProducer(eventCh, config.Sender, config.ProducersCount, workerPool)
    r := retranslator{consumer: consumer.NewConsumer(config.ConsumersCount, config.ConsumerBatchSize, eventCh, config.Repo, config.ConsumerInterval), eventCh: eventCh, producer: newProducer, workerPool: workerPool}

    return &r
}

type Config struct {
    ChannelSize       uint64
    Repo              repo.EventRepo
    Sender            sender.EventSender
    ConsumersCount    uint64
    ConsumerBatchSize uint64
    ConsumerInterval  time.Duration
    ProducersCount    uint64
    WorkerCount       int
}

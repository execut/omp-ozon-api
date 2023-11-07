package main

import (
    "github.com/execut/omp-ozon-api/internal/app/repo"
    "github.com/execut/omp-ozon-api/internal/app/sender"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/execut/omp-ozon-api/internal/app/retranslator"
)

func main() {

    sigs := make(chan os.Signal, 1)

    cfg := retranslator.Config{
        ChannelSize:         100,
        ConsumersCount:      100,
        ConsumerInterval:    time.Millisecond,
        ConsumerBatchSize:   100,
        ProducersCount:      100,
        WorkerCount:         10,
        Repo:                repo.NewStubEventRepo(100000, time.Millisecond*100),
        Sender:              sender.NewStubEventSender(),
        WorkerpoolBatchSize: 100,
    }

    retranslator := retranslator.NewRetranslator(cfg)
    retranslator.Start()

    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

    <-sigs
}

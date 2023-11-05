package main

import (
    "github.com/execut/omp-ozon-api/internal/app/repo"
    "github.com/execut/omp-ozon-api/internal/app/sender"
    "os"
    "os/signal"
    "syscall"

    "github.com/execut/omp-ozon-api/internal/app/retranslator"
)

func main() {

    sigs := make(chan os.Signal, 1)

    cfg := retranslator.Config{
        ChannelSize:    512,
        ConsumerCount:  2,
        ConsumeTimeout: 30,
        ConsumeSize:    10,
        ProducerCount:  28,
        WorkerCount:    2,
        Repo:           repo.NewStubEventRepo(300),
        Sender:         sender.NewStubEventSender(),
    }

    retranslator := retranslator.NewRetranslator(cfg)
    retranslator.Start()

    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

    <-sigs
}

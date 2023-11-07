package retranslator

import (
    "github.com/execut/omp-ozon-api/internal/app/repo"
    "github.com/execut/omp-ozon-api/internal/mocks"
    "go.uber.org/mock/gomock"
    "testing"
    "time"
)

func TestStart(t *testing.T) {
    t.Parallel()
    ctrl := gomock.NewController(t)
    rep := repo.NewStubEventRepo(1, 0)
    sender := mocks.NewMockEventSender(ctrl)
    sender.EXPECT().
        Send(gomock.Any()).Times(1)
    cfg := Config{
        Repo:                rep,
        Sender:              sender,
        ConsumersCount:      2,
        ConsumerBatchSize:   1,
        ConsumerInterval:    time.Nanosecond,
        ProducersCount:      1,
        WorkerpoolBatchSize: 1,
    }
    sut := NewRetranslator(cfg)

    sut.Start()
    sut.Close()
}

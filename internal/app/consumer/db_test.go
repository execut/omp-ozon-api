package consumer

import (
    "github.com/execut/omp-ozon-api/internal/model"
    "testing"
)

func Test_consumer_Close(t *testing.T) {
    t.Run("Start for batch 2 will pass 2 events to chan", func(t *testing.T) {
        eventCh := make(chan *model.KeywordEvent)
        //ctrl := mock.NewController(t)
        //repo := mocks.NewMockEventRepo(ctrl)
        //c := NewConsumer(2, eventCh, repo)
        //repo.EXPECT().Lock(mock.Any()).AnyTimes()

        c.Start()
    })
}

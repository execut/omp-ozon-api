package consumer

import (
    "github.com/execut/omp-ozon-api/internal/app/repo"
    "github.com/execut/omp-ozon-api/internal/model"
    "gotest.tools/v3/assert"
    "testing"
    "time"
)

func Test_consumer_Close(t *testing.T) {
    t.Parallel()
    t.Run("Start for a batch of 2 events will pass 2 events to chan", func(t *testing.T) {
        t.Parallel()
        eventCh := make(chan *model.KeywordEvent)
        defer close(eventCh)
        c := NewConsumer(1, 2, eventCh, repo.NewStubEventRepo(2, 0), time.Nanosecond)

        c.Start()

        var event *model.KeywordEvent
        event = <-eventCh
        assert.Equal(t, uint64(1), event.ID)
        event = <-eventCh
        assert.Equal(t, uint64(2), event.ID)
    })
    t.Run("Start for a batch of 1 events and with 2 events will pass 2 events to chan", func(t *testing.T) {
        t.Parallel()
        eventCh := make(chan *model.KeywordEvent)
        defer close(eventCh)
        c := NewConsumer(1, 1, eventCh, repo.NewStubEventRepo(2, 0), time.Nanosecond)

        c.Start()

        var event *model.KeywordEvent
        event = <-eventCh
        assert.Equal(t, uint64(1), event.ID)
        event = <-eventCh
        assert.Equal(t, uint64(2), event.ID)
    })
    t.Run("Only one lock after start with 1 second tick", func(t *testing.T) {
        t.Parallel()
        eventCh := make(chan *model.KeywordEvent, 2)
        defer close(eventCh)
        c := NewConsumer(1, 1, eventCh, repo.NewStubEventRepo(2, 0), time.Second)
        defer c.Close()

        c.Start()
        time.Sleep(time.Microsecond * 10)

        assert.Equal(t, len(eventCh), 1)
    })
    t.Run("Start will stop after Close", func(t *testing.T) {
        t.Parallel()
        eventCh := make(chan *model.KeywordEvent, 2)
        defer close(eventCh)
        c := NewConsumer(1, 1, eventCh, repo.NewStubEventRepo(2, 0), time.Microsecond*10)
        defer c.Close()

        c.Start()
        c.Close()
        time.Sleep(time.Microsecond * 15)

        assert.Equal(t, len(eventCh), 1)
    })
}

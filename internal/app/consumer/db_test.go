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
        defer c.Close()

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
        defer c.Close()

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
        c := NewConsumer(1, 1, eventCh, repo.NewStubEventRepo(5, 0), time.Second)
        defer c.Close()

        c.Start()
        <-eventCh

        time.Sleep(time.Microsecond * 15)
        assert.Equal(t, 0, len(eventCh))
    })
    t.Run("Start will stop after Close", func(t *testing.T) {
        t.Parallel()
        eventCh := make(chan *model.KeywordEvent, 200)
        defer close(eventCh)
        c := NewConsumer(1, 1, eventCh, repo.NewStubEventRepo(200, 0), time.Nanosecond)
        defer c.Close()

        c.Start()
        c.Close()

        time.Sleep(time.Microsecond * 15)
        assert.Equal(t, true, len(eventCh) < 5, len(eventCh))
    })
    t.Run("Close waits for the repository to done", func(t *testing.T) {
        t.Parallel()
        eventCh := make(chan *model.KeywordEvent, 2)
        defer close(eventCh)
        c := NewConsumer(1, 1, eventCh, repo.NewStubEventRepo(2, time.Microsecond*100), time.Microsecond*40)

        c.Start()
        c.Close()

        assert.Equal(t, true, len(eventCh) > 0)
    })
    t.Run("Retry lock if error", func(t *testing.T) {
        t.Parallel()
        eventCh := make(chan *model.KeywordEvent, 2)
        defer close(eventCh)
        c := NewConsumer(1, 1, eventCh, repo.NewStubEventRepo(0, 0), time.Microsecond*40)
        defer c.Close()

        c.Start()

        time.Sleep(time.Microsecond * 15)
        assert.Equal(t, 0, len(eventCh))
    })
    t.Run("Run with 3 customers and 2 events return 2 events", func(t *testing.T) {
        t.Parallel()
        eventCh := make(chan *model.KeywordEvent)
        defer close(eventCh)
        c := NewConsumer(3, 1, eventCh, repo.NewStubEventRepo(2, 0), time.Nanosecond)
        defer c.Close()

        c.Start()
        <-eventCh
        <-eventCh
    })
}

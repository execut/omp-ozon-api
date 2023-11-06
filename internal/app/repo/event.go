package repo

import (
    "errors"
    "github.com/execut/omp-ozon-api/internal/model"
    "sync"
    "time"
)

type EventRepo interface {
    Lock(n uint64) ([]model.KeywordEvent, error)
    Unlock(eventIDs []uint64) error

    Add(event []model.KeywordEvent) error
    Remove(eventIDs []uint64) error
}

var ErrEventNotFound error = errors.New("Event not found")

var ErrNoMoreEvents error = errors.New("No more events for lock")

func NewStubEventRepo(eventsCount uint64, lockDuration time.Duration) EventRepo {
    eventsMap := make([]model.KeywordEvent, eventsCount)
    for i := uint64(0); i < eventsCount; i++ {
        keyword := model.NewTestKeyword((i + 1) * 100)
        keywordEvent := model.NewTestKeywordEvent(i+1, &keyword)
        eventsMap[i] = keywordEvent
    }

    return &StubEventRepo{mutex: sync.Mutex{}, currentN: 0, lockedEvents: make(map[uint64]bool), events: eventsMap, lockDuration: lockDuration}
}

type StubEventRepo struct {
    events       []model.KeywordEvent
    lockedEvents map[uint64]bool
    mutex        sync.Mutex
    currentN     uint64
    lockDuration time.Duration
}

func (r *StubEventRepo) Lock(n uint64) ([]model.KeywordEvent, error) {
    time.Sleep(r.lockDuration)
    r.mutex.Lock()
    defer r.mutex.Unlock()
    var lockedEvents []model.KeywordEvent
    l := uint64(len(r.events))
    if l == 0 {
        return nil, ErrNoMoreEvents
    }

    if l < n {
        n = l
    }

    lockedEvents = r.events[:n]
    r.events = r.events[n:]
    for _, event := range lockedEvents {
        r.lockedEvents[event.ID] = true
    }

    return lockedEvents, nil
}

func (r *StubEventRepo) Unlock(eventIDs []uint64) error {
    r.mutex.Lock()
    defer r.mutex.Unlock()
    for _, n := range eventIDs {
        if _, ok := r.lockedEvents[n]; !ok {
            return ErrEventNotFound
        }
    }
    return nil
}

func (r *StubEventRepo) Add(event []model.KeywordEvent) error {
    //TODO implement me
    panic("implement me")
}

func (r *StubEventRepo) Remove(eventIDs []uint64) error {
    //TODO implement me
    panic("implement me")
}

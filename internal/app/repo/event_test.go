package repo

import (
    "gotest.tools/v3/assert"
    "sync"
    "testing"
)

func TestStubEventRepo_LockUnlock(t *testing.T) {
    t.Parallel()
    sut := NewStubEventRepo(7)
    t.Run("Lock test", func(t *testing.T) {
        expectedN := 5
        events, err := sut.Lock(uint64(expectedN))
        assert.NilError(t, err)
        assert.Equal(t, expectedN, len(events))
    })

    t.Run("Unlock test", func(t *testing.T) {
        eventsIds := []uint64{1, 2}
        err := sut.Unlock(eventsIds)
        assert.NilError(t, err)
    })

    t.Run("Unlock not found test", func(t *testing.T) {
        eventsIds := []uint64{123}
        err := sut.Unlock(eventsIds)
        assert.ErrorIs(t, err, ErrEventNotFound)
    })
}

func TestStubEventRepo_Lock(t *testing.T) {
    t.Parallel()
    sut := NewStubEventRepo(7)
    t.Run("Lock twice", func(t *testing.T) {
        sut.Lock(uint64(5))
        events, err := sut.Lock(uint64(5))
        assert.NilError(t, err)
        assert.Equal(t, 2, len(events))
        assert.Equal(t, uint64(6), events[0].ID)
    })
    t.Run("Lock three times throw error", func(t *testing.T) {
        _, err := sut.Lock(uint64(5))
        assert.ErrorIs(t, ErrNoMoreEvents, err)
    })
}

func TestStubEventRepo_LockUnlockParallel(t *testing.T) {
    t.Parallel()
    t.Run("Lock and unlock at parallel", func(t *testing.T) {
        chunksCount := 200
        t.Parallel()
        sut := NewStubEventRepo(uint64(chunksCount * 5))
        wg := sync.WaitGroup{}
        eventsIdsCh := make(chan []uint64, chunksCount)
        for i := 0; i < chunksCount; i++ {
            wg.Add(1)
            go func(i int) {
                expectedN := 5
                events, err := sut.Lock(uint64(expectedN))
                assert.NilError(t, err)
                var ids []uint64
                for _, e := range events {
                    ids = append(ids, e.ID)
                }
                eventsIdsCh <- ids
                wg.Done()
            }(i)
        }

        wg.Wait()
        close(eventsIdsCh)

        for {
            eventsChunk, ok := <-eventsIdsCh
            if !ok {
                break
            }
            wg.Add(1)
            go func(eventsChunk []uint64) {
                err := sut.Unlock(eventsChunk)
                assert.NilError(t, err)
                wg.Done()
            }(eventsChunk)
        }

        wg.Wait()
    })
}

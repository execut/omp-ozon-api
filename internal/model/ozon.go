package model

import "strconv"

type Keyword struct {
    ID uint64
}

type EventType uint8

type EventStatus uint8

const (
    Created EventType = iota
    Updated
    Removed

    Deferred EventStatus = iota
    Processed
)

type KeywordEvent struct {
    ID     uint64
    Type   EventType
    Status EventStatus
    Entity *Keyword
}

func NewTestKeyword(id uint64) Keyword {
    return Keyword{ID: id}
}

func NewTestKeywordEvent(id uint64, keyword *Keyword) KeywordEvent {
    return KeywordEvent{ID: id, Type: Created, Status: Deferred, Entity: keyword}
}

func (k KeywordEvent) String() string {
    return strconv.FormatUint(k.ID, 10)
}

package sender

import (
    "github.com/execut/omp-ozon-api/internal/model"
    "testing"
)

func TestStubEventSender_Send(t *testing.T) {
    t.Run("Send success", func(t *testing.T) {
        sut := StubEventSender{}
        keyword := model.NewTestKeyword(2)
        event := model.NewTestKeywordEvent(1, &keyword)
        sut.Send(&event)
    })
}

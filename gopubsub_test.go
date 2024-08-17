package gopubsub

import (
	"fmt"
	"testing"
)

func TestMessageBroker(t *testing.T) {
	mb := NewMessageBroker()
	subscriber, err := mb.NewSubscriber(10, []string{"names"}...)
	if err != nil {
		t.Fatal(fmt.Errorf("failed to create new subscriber: %w", err))
	}
	subscriber.Listen(func(message Message) {
		data, ok := message.Data.(string)
		if !ok {
			t.Error("received message is not a string")
		}
		if data != "Ercross" {
			t.Error("received message is not the right data")
		}
	})
	err = mb.Publish(Message{Data: "Ercross", Topic: "names"})

	if err != nil {
		t.Fatal("failed to publish message: %w", err)
	}
}

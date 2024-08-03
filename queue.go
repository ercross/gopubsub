package gopubsub

import (
	"errors"
	"sync"
)

var errQueueEmpty = errors.New("queue is empty")

type messageQueue struct {
	mu       sync.Mutex
	size     int
	messages []Message
}

func newMessageQueue(size int) *messageQueue {
	return &messageQueue{
		size:     size,
		messages: make([]Message, size),
	}
}

func (q *messageQueue) enqueue(message Message) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.messages = append(q.messages, message)
}

func (q *messageQueue) dequeue() (Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return Message{}, errQueueEmpty
	}

	message := q.messages[0]
	q.messages = q.messages[1:]
	return message, nil
}

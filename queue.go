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

// dequeue returns the next Message on the queue
// and subsequently removes it from the queue.
// dequeue can panic if messageQueue is empty, so check isEmpty
func (q *messageQueue) dequeue() Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		panic("queue is empty")
	}

	message := q.messages[0]
	q.messages = q.messages[1:]
	return message
}

func (q *messageQueue) isEmpty() bool {
	return q.size == 0
}

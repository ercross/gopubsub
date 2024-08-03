package gopubsub

import (
	"errors"
	"sync"
	"sync/atomic"
)

// https://hackernoon.com/understanding-synccond-in-go-a-guide-for-beginners
// https://www.reddit.com/r/golang/comments/1dnsqjm/no_sleep_until_we_build_the_perfect_pubsub/
// https://blog.logrocket.com/building-pub-sub-service-go/
// https://lukechampine.com/cond.html

// Topic is case-sensitive
type Topic string

type Message struct {
	Topic Topic

	Data any
}

var (
	ErrTopicsEmpty        = errors.New("topic is empty")
	ErrNoActiveSubscriber = errors.New("no active subscriber found")
)

func NewMessage(topic Topic, data any) Message {
	return Message{
		Topic: topic,
		Data:  data,
	}
}

// MessageBroker implements a multi-publisher / multi-subscriber messaging model
type MessageBroker struct {
	buffer           *messageQueue
	nextSubscriberID *atomic.Uint64
	topicSubscribers topicSubscribers
	allSubscribers   allSubscribers
}

type topicSubscribers struct {
	entries map[Topic][]*Subscriber
	mu      sync.RWMutex
}

type allSubscribers struct {
	entries map[uint64]*Subscriber
	mu      sync.RWMutex
}

type Subscriber struct {
	id uint64

	messageChan chan Message
	topics      map[Topic]struct{}
}

var defaultMessageQueueSize = 1000

func NewMessageBroker() *MessageBroker {
	return &MessageBroker{
		topicSubscribers: topicSubscribers{
			entries: make(map[Topic][]*Subscriber),
		},
		allSubscribers: allSubscribers{
			entries: make(map[uint64]*Subscriber),
		},
		nextSubscriberID: new(atomic.Uint64),
		buffer:           newMessageQueue(defaultMessageQueueSize),
	}
}

// NewSubscriber register a new Subscriber on given topics.
// topics must not be empty.
// bufferSize is the maximum number of unread messages that could be queued in Subscriber's messaging box
// before Subscriber is unable to receive further message
func (mb *MessageBroker) NewSubscriber(bufferSize int, topics ...Topic) (*Subscriber, error) {

	if len(topics) == 0 {
		return nil, ErrTopicsEmpty
	}

	s := &Subscriber{
		id:          mb.nextSubscriberID.Add(1),
		messageChan: make(chan Message, bufferSize),
		topics:      make(map[Topic]struct{}),
	}

	mb.allSubscribers.mu.Lock()
	mb.allSubscribers.entries[s.id] = s
	mb.allSubscribers.mu.Unlock()

	mb.topicSubscribers.mu.Lock()
	defer mb.topicSubscribers.mu.Unlock()
	for _, topic := range topics {
		s.topics[topic] = struct{}{}
		if subscribers, ok := mb.topicSubscribers.entries[topic]; !ok {
			mb.topicSubscribers.entries[topic] = []*Subscriber{s}
		} else {
			subscribers = append(subscribers, s)
			mb.topicSubscribers.entries[topic] = subscribers
		}
	}

	return s, nil
}

// Unsubscribe de-registers s as an active Subscriber
func (mb *MessageBroker) Unsubscribe(subscriber *Subscriber) {

	mb.allSubscribers.mu.Lock()
	delete(mb.allSubscribers.entries, subscriber.id)
	mb.allSubscribers.mu.Unlock()

	// remove subscriber from all topics subscribed
	mb.topicSubscribers.mu.Lock()
	defer mb.topicSubscribers.mu.Unlock()
	for topic := range subscriber.topics {
		if topicSubscribers, ok := mb.topicSubscribers.entries[topic]; ok {
			reducedSubscribers := make([]*Subscriber, 0, len(topicSubscribers))
			for _, topicSubscriber := range topicSubscribers {
				if topicSubscriber.id != subscriber.id {
					reducedSubscribers = append(reducedSubscribers, topicSubscriber)
				}
			}
			mb.topicSubscribers.entries[topic] = reducedSubscribers
		}
	}
}

// Publish message on topic provided there is at least one Subscriber listening on topic
func (mb *MessageBroker) Publish(message Message) error {

	// to improve availability of the MessageBroker,
	// new messages are queued first before publishing
	go mb.buffer.enqueue(message)

	// publish the next message from the queue
	if mb.buffer.isEmpty() {
		return nil
	}

	qm := mb.buffer.dequeue()
	mb.topicSubscribers.mu.RLock()
	defer mb.topicSubscribers.mu.RUnlock()

	subscribers, ok := mb.topicSubscribers.entries[qm.Topic]
	if !ok {
		return ErrNoActiveSubscriber
	}

	for _, subscriber := range subscribers {
		subscriber.messageChan <- qm
	}

	return nil
}

func (mb *MessageBroker) Broadcast(message Message) {
	mb.topicSubscribers.mu.RLock()
	defer mb.topicSubscribers.mu.RUnlock()

	for _, subscriber := range mb.allSubscribers.entries {
		subscriber.messageChan <- message
	}
}

// Listen is a blocking operation and should be run in a goroutine.
// To avoid blocking this Subscriber.messageChan,
// handle body can be wrapped in a goroutine.
func (s *Subscriber) Listen(handle func(message Message)) {
	for {
		if msg, ok := <-s.messageChan; ok {
			handle(msg)
		}
	}
}

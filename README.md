# gopubsub
GoPubSub is a pubsub messaging library for Go. 
It allows any component to publish or broadcast messages and any component to subscribe to those messages,
enabling decoupled and scalable application architecture.


## Features

- Multi-publisher and multi-subscriber support through message broker
- Topic-based messaging
- Synchronous and asynchronous message handling
- Lightweight and easy to integrate


## Limitations
gopubsub does not provide a pubsub server, hence it has limited real-life use-cases.
One of which is a messaging component in a multi-module architecture 
where extremely low-latency is critical
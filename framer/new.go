package framer

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/pkg/errors"
)

// Framer provides convenience channels for producing messages.
type Framer struct {
	Command  chan<- *model.Command
	Document chan<- *model.Document
	Event    chan<- *model.Event
}

// TopicConfig is the configuration for topics on which respective
// entities should be produced. In case of Command and Document,
// this is the default topic if one is not set in Command or Document.
type TopicConfig struct {
	CommandTopic  string
	DocumentTopic string
	EventTopic    string
}

// New creates a new Framer instance.
func New(
	ctx context.Context,
	prodConfig *kafka.ProducerConfig,
	topicConfig *TopicConfig,
) (*Framer, error) {
	if topicConfig.EventTopic == "" {
		return nil, errors.New("EventTopic in TopicConfig is required")
	}

	command := make(chan *model.Command, 256)
	document := make(chan *model.Document, 256)
	event := make(chan *model.Event, 256)

	producer, err := kafka.NewProducer(prodConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating Producer")
		return nil, err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-producer.Errors():
				if err != nil && err.Err != nil {
					parsedErr := errors.Wrap(err.Err, "Error in Framer-producer")
					log.Println(parsedErr)
					log.Println(err)
				}
			}
		}
	}()

	lock := &sync.RWMutex{}
	closed := false
	go func() {
		<-ctx.Done()
		lock.Lock()
		closed = true
		lock.Unlock()
	}()

	go func() {
		for msg := range command {
			lock.RLock()
			isClosed := closed
			lock.RUnlock()
			if isClosed {
				return
			}

			if msg != nil {
				marshalMsg, err := json.Marshal(msg)
				if err != nil {
					err = errors.Wrap(err, "Error marshalling Command")
					log.Println(err)
					continue
				}

				topic := msg.SourceTopic
				if topic == "" {
					topic = topicConfig.CommandTopic
				}
				prodMsg := kafka.CreateMessage(topic, marshalMsg)
				producer.Input() <- prodMsg
			}
		}
	}()

	go func() {
		for msg := range document {
			lock.RLock()
			isClosed := closed
			lock.RUnlock()
			if isClosed {
				return
			}

			if msg != nil {
				marshalMsg, err := json.Marshal(msg)
				if err != nil {
					err = errors.Wrap(err, "Error marshalling Document")
					log.Println(err)
					continue
				}

				topic := msg.Topic
				if topic == "" {
					topic = topicConfig.DocumentTopic
				}
				prodMsg := kafka.CreateMessage(topic, marshalMsg)
				producer.Input() <- prodMsg
			}
		}
	}()

	go func() {
		for msg := range event {
			lock.RLock()
			isClosed := closed
			lock.RUnlock()
			if isClosed {
				return
			}

			if msg != nil {
				marshalMsg, err := json.Marshal(msg)
				if err != nil {
					err = errors.Wrap(err, "Error marshalling Event")
					log.Println(err)
					continue
				}

				prodMsg := kafka.CreateMessage(topicConfig.EventTopic, marshalMsg)
				producer.Input() <- prodMsg
			}
		}
	}()

	return &Framer{
		Command:  (chan<- *model.Command)(command),
		Document: (chan<- *model.Document)(document),
		Event:    (chan<- *model.Event)(event),
	}, nil
}

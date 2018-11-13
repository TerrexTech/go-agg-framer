package framer

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"testing"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/uuuid"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-kafkautils/kafka"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func TestFramer(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../test.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_GROUP",
		"COMMAND_TOPIC",
		"DOCUMENT_TOPIC",
		"EVENT_TOPIC",
	)

	if err != nil {
		err = errors.Wrapf(
			err,
			"Env-var %s is required for testing, but is not set", missingVar,
		)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "Framer Suite")
}

var _ = Describe("Framer", func() {
	var (
		brokers  []string
		consumer *kafka.Consumer

		commandTopic  string
		documentTopic string
		eventTopic    string
	)

	BeforeEach(func() {
		brokersStr := os.Getenv("KAFKA_BROKERS")
		brokers = *commonutil.ParseHosts(brokersStr)

		var err error

		commandTopic = os.Getenv("COMMAND_TOPIC")
		documentTopic = os.Getenv("DOCUMENT_TOPIC")
		eventTopic = os.Getenv("EVENT_TOPIC")

		consumer, err = kafka.NewConsumer(&kafka.ConsumerConfig{
			GroupName:    os.Getenv("KAFKA_CONSUMER_GROUP"),
			KafkaBrokers: brokers,
			Topics: []string{
				commandTopic,
				documentTopic,
				eventTopic,
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		err := consumer.Close()
		Expect(err).ToNot(HaveOccurred())
	})

	It("should produce Command result", func(done Done) {
		prodCfg := &kafka.ProducerConfig{
			KafkaBrokers: brokers,
		}
		topicCfg := &TopicConfig{
			CommandTopic: commandTopic,
		}
		f, err := New(context.Background(), prodCfg, topicCfg)
		Expect(err).ToNot(HaveOccurred())

		uuid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		cmd := &model.Command{
			AggregateID:   1,
			EventAction:   "test-eventaction",
			ServiceAction: "test-svcaction",
			Data:          []byte(uuid.String()),
		}
		f.Command <- cmd

		msgCallback := func(msg *sarama.ConsumerMessage) bool {
			defer GinkgoRecover()
			log.Println("A Command was received, now verifying")
			cmdMsg := &model.Command{}
			err := json.Unmarshal(msg.Value, cmdMsg)

			if err == nil && string(cmdMsg.Data) == string(cmd.Data) {
				Expect(cmdMsg).To(Equal(cmd))
				close(done)
				return true
			}
			return false
		}

		handler := &msgHandler{msgCallback}
		err = consumer.Consume(context.Background(), handler)
		Expect(err).ToNot(HaveOccurred())
	}, 15)

	It("should produce Document result", func(done Done) {
		prodCfg := &kafka.ProducerConfig{
			KafkaBrokers: brokers,
		}
		topicCfg := &TopicConfig{
			DocumentTopic: documentTopic,
		}
		f, err := New(context.Background(), prodCfg, topicCfg)
		Expect(err).ToNot(HaveOccurred())

		uuid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		doc := &model.Document{
			AggregateID:   4,
			EventAction:   "test-action",
			ServiceAction: "service-action",
			CorrelationID: cid,
			Input:         []byte("test-input"),
			Result:        []byte("test-result"),
			Topic:         "test-topic",
			UUID:          uuid,
		}
		f.Document <- doc

		msgCallback := func(msg *sarama.ConsumerMessage) bool {
			defer GinkgoRecover()
			log.Println("A Document was received, now verifying")
			docMsg := &model.Document{}
			err := json.Unmarshal(msg.Value, docMsg)

			if err == nil && docMsg.UUID == doc.UUID {
				Expect(docMsg).To(Equal(doc))
				close(done)
				return true
			}
			return false
		}

		handler := &msgHandler{msgCallback}
		err = consumer.Consume(context.Background(), handler)
		Expect(err).ToNot(HaveOccurred())
	}, 15)

	It("should produce Event result", func(done Done) {
		prodCfg := &kafka.ProducerConfig{
			KafkaBrokers: brokers,
		}
		topicCfg := &TopicConfig{
			EventTopic: eventTopic,
		}
		f, err := New(context.Background(), prodCfg, topicCfg)
		Expect(err).ToNot(HaveOccurred())

		uuid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		event := &model.Event{
			AggregateID:   4,
			EventAction:   "test-action",
			ServiceAction: "service-action",
			CorrelationID: cid,
			Data:          []byte("test-event"),
			UUID:          uuid,
		}
		f.Event <- event

		msgCallback := func(msg *sarama.ConsumerMessage) bool {
			defer GinkgoRecover()
			log.Println("A Document was received, now verifying")
			eventMsg := &model.Event{}
			err := json.Unmarshal(msg.Value, eventMsg)

			if err == nil && eventMsg.UUID == event.UUID {
				Expect(eventMsg).To(Equal(event))
				close(done)
				return true
			}
			return false
		}

		handler := &msgHandler{msgCallback}
		err = consumer.Consume(context.Background(), handler)
		Expect(err).ToNot(HaveOccurred())
	}, 15)
})

package kafka

import (
	"fmt"

	"github.com/arrrden/kafka/messaging/kafka/logger"
	kafka "github.com/segmentio/kafka-go"
)

type KafkaClient struct {
	Host   string
	Topics map[string]int
	logger *logger.Logger
}

func NewKafkaClient(host string, logger *logger.Logger) (*KafkaClient, error) {
	// dial the client to ensure it's valid
	conn, err := Dial(host)
	if err != nil {
		return nil, err
	}

	conn.Close()

	client := &KafkaClient{
		Host:   host,
		logger: logger,
	}

	return client, nil
}

func (c *KafkaClient) NewConnection() (*Connection, error) {
	conn := &Connection{}

	kconn, err := Dial(c.Host)
	if err != nil {
		return nil, err
	}

	conn.Conn = kconn
	conn.Client = c
	conn.Retries = 5
	return conn, nil
}

func (c *KafkaClient) GetTopics(conn *kafka.Conn) error {
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	c.Topics = map[string]int{}

	for _, p := range partitions {
		c.Topics[p.Topic] = p.ID
	}

	return nil
}

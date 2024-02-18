package kafka

import (
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

type KafkaClient struct {
	Host   string
	Topics map[string]int
}

func NewKafkaClient(host string) (*KafkaClient, error) {
	// dial the client to ensure it's valid
	conn, err := Dial(host)
	if err != nil {
		return nil, err
	}

	conn.Close()

	client := &KafkaClient{
		Host: host,
	}
	// if err := client.getTopics(conn); err != nil {
	// 	return nil, err
	// }

	return client, nil
}

func (c *KafkaClient) NewConnection() (*Connection, error) {
	conn := &Connection{}

	kconn, err := Dial(c.Host)
	if err != nil {
		return nil, err
	}

	conn.conn = kconn
	conn.Client = c
	conn.Retries = 5
	return conn, nil
}

func (c *KafkaClient) getTopics(conn *kafka.Conn) error {
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	for _, p := range partitions {
		c.Topics[p.Topic] = p.ID
	}

	return nil
}

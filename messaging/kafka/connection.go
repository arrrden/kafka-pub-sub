package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type Connection struct {
	Client  *KafkaClient
	Retries int
	conn    *kafka.Conn
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func (c *Connection) Produce(ctx context.Context, topic string, messages ...kafka.Message) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	writer := &kafka.Writer{
		Addr:     kafka.TCP(c.Client.Host),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	for i := 0; i < c.Retries; i++ {
		err := writer.WriteMessages(ctx, messages...)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			return fmt.Errorf("failed to write messages, unexpected error: %w", err)
		}
		break
	}

	return nil
}

func (c *Connection) Consume(ctx context.Context, topic, groupId string, messageCh chan<- kafka.Message, quit <-chan struct{}) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{c.Client.Host},
		GroupID:        groupId,
		Topic:          topic,
		MaxBytes:       10e6, // 10MB
		CommitInterval: 5 * time.Second,
	})

loop:
	for {
		select {
		case <-quit:
			break loop
		default:
			fmt.Println("[*] reading message")
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				err = fmt.Errorf("failed to read message: %w", err)
				return err
			}

			messageCh <- msg

		}
	}

	close(messageCh)
	return nil
}

func (c *Connection) Subscribe(ctx context.Context, topic, groupId string, messageCh chan<- kafka.Message, errCh chan<- error, subscribed *bool) {
	fmt.Println("[*] subscribing")
	var wg sync.WaitGroup
	wg.Add(1)

	quit := make(chan struct{}, 1)

	go func() {
		for {
			if !*subscribed {
				wg.Done()
				break
			}
		}
	}()

	go func() {
		errCh <- c.Consume(ctx, topic, groupId, messageCh, quit)
	}()

	fmt.Println("[*] subscribed")
	wg.Wait()
	close(quit)

	fmt.Println("[*] unsubscribed")
	close(errCh)
}

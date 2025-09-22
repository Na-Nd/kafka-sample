package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type User struct {
	ID        int64  `json:"id,omitempty"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	CreatedAt string `json:"created_at,omitempty"`
}

func main() {
	// env или дефолт
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:29092,localhost:29093,localhost:29094"
	}
	brokers := strings.Split(brokersEnv, ",")

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "users"
	}
	groupID := os.Getenv("KAFKA_GROUP_ID")
	if groupID == "" {
		groupID = "users-consumer-group"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1e2, // 100B
		MaxBytes:    1e6, // 1MB
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Failed to close kafka reader: %v", err)
		}
	}()

	log.Printf("Consumer started. brokers=%v topic=%s group=%s", brokers, topic, groupID)

	// Правильное выключение
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("Shutdown signal received")
		cancel()
	}()

	// Цикл чтения сообщений
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			// При отмене контекста просто выходим
			if ctx.Err() != nil {
				log.Println("Context cancelled, exiting")
				return
			}
			log.Printf("Read error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var u User
		if err := json.Unmarshal(m.Value, &u); err != nil {
			log.Printf("Failed to unmarshal message at offset=%d partition=%d: %v\nraw: %s",
				m.Offset, m.Partition, err, string(m.Value))
			continue
		}

		fmt.Printf("Message offset=%d partition=%d: %+v\n", m.Offset, m.Partition, u)
	}
}

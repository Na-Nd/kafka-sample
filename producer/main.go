package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// User — структура сообщения, которое принимаем и отправляем в Kafka.
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

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // балансировщик: отправляет в партицию с наименьшим размером
	}
	// Закрываем writer при выходе
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("failed to close kafka writer: %v", err)
		}
	}()

	// HTTP handler для POST /users
	http.HandleFunc("/users", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { // Только POST
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Десериализация
		var u User
		if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
			http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if u.Name == "" || u.Email == "" {
			http.Error(w, "Name and email are required", http.StatusBadRequest)
			return
		}
		if u.CreatedAt == "" {
			u.CreatedAt = time.Now().UTC().Format(time.RFC3339)
		}

		// Сериализуем сообщение для Kafka
		msgBytes, err := json.Marshal(u)
		if err != nil {
			http.Error(w, "failed to marshal user: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Контекст с таймаутом для отправки в Kafka
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Email как ключ для записи в одну партицию "сообщений" с одинаковой почтой
		msg := kafka.Message{
			Key:   []byte(u.Email),
			Value: msgBytes,
		}

		// Блокирующий подход записи
		if err := writer.WriteMessages(ctx, msg); err != nil {
			http.Error(w, "Failed to write to kafka: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write(msgBytes)
	})

	// H-C
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Запуск HTTP сервера
	port := os.Getenv("APP_PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port
	log.Printf("Producer listening on %s, writing to topic %q (brokers: %v)", addr, topic, brokers)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

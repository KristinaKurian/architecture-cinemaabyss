package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const maxEventBodySize = 1 << 20

type eventEnvelope struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	OccurredAt time.Time      `json:"occurred_at"`
	Payload    map[string]any `json:"payload"`
}

type eventServer struct {
	broker  EventBroker
	brokers string
}

func newEventServer(broker EventBroker, brokers string) http.Handler {
	server := &eventServer{broker: broker, brokers: brokers}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", server.health)
	mux.HandleFunc("/api/events/movie", server.create("movie", "movie-events"))
	mux.HandleFunc("/api/events/user", server.create("user", "user-events"))
	mux.HandleFunc("/api/events/payment", server.create("payment", "payment-events"))
	return mux
}

func (server *eventServer) health(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeEventJSON(writer, http.StatusOK, map[string]any{
		"status":        true,
		"service":       "events-service",
		"kafka_brokers": server.brokers,
	})
}

func (server *eventServer) create(eventType, topic string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		request.Body = http.MaxBytesReader(writer, request.Body, maxEventBodySize)
		decoder := json.NewDecoder(request.Body)
		decoder.UseNumber()
		payload := make(map[string]any)
		if err := decoder.Decode(&payload); err != nil {
			writeEventJSON(writer, http.StatusBadRequest, map[string]string{"error": "request body must be a JSON object"})
			return
		}
		if len(payload) == 0 {
			writeEventJSON(writer, http.StatusBadRequest, map[string]string{"error": "event payload must not be empty"})
			return
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			writeEventJSON(writer, http.StatusBadRequest, map[string]string{"error": "request body must contain one JSON object"})
			return
		}

		envelope := eventEnvelope{
			ID:         newEventID(),
			Type:       eventType,
			OccurredAt: time.Now().UTC(),
			Payload:    payload,
		}
		message, err := json.Marshal(envelope)
		if err != nil {
			writeEventJSON(writer, http.StatusInternalServerError, map[string]string{"error": "cannot encode event"})
			return
		}

		publishContext, cancel := context.WithTimeout(request.Context(), 8*time.Second)
		defer cancel()
		if err = server.broker.Publish(publishContext, topic, message); err != nil {
			log.Printf("failed to publish event id=%s type=%s topic=%s: %v", envelope.ID, eventType, topic, err)
			writeEventJSON(writer, http.StatusServiceUnavailable, map[string]string{
				"status": "error",
				"error":  "Kafka is unavailable",
			})
			return
		}

		writeEventJSON(writer, http.StatusCreated, map[string]string{
			"status":   "success",
			"event_id": envelope.ID,
			"topic":    topic,
		})
	}
}

func newEventID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("event-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value)
}

func writeEventJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		log.Printf("failed to encode response: %v", err)
	}
}

func main() {
	port := envOr("PORT", "8082")
	brokers := envOr("KAFKA_BROKERS", "localhost:9092")
	kafka, err := newNativeKafka(brokers)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, topic := range []string{"movie-events", "user-events", "payment-events"} {
		topic := topic
		go kafka.Consume(ctx, topic, func(message []byte) {
			log.Printf("processed Kafka event topic=%s payload=%s", topic, strings.TrimSpace(string(message)))
		})
	}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           newEventServer(kafka, brokers),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("events-service listening on :%s, brokers=%s", port, brokers)
	log.Fatal(server.ListenAndServe())
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

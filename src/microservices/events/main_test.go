package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeBroker struct {
	topic   string
	message []byte
	err     error
}

func (broker *fakeBroker) Publish(_ context.Context, topic string, message []byte) error {
	broker.topic = topic
	broker.message = append([]byte(nil), message...)
	return broker.err
}

func (broker *fakeBroker) Consume(context.Context, string, func([]byte)) {}

func TestCreateMovieEvent(t *testing.T) {
	broker := &fakeBroker{}
	handler := newEventServer(broker, "kafka:9092")
	request := httptest.NewRequest(http.MethodPost, "/api/events/movie", strings.NewReader(`{"movie_id": 7, "action": "viewed"}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", response.Code, response.Body.String())
	}
	if broker.topic != "movie-events" {
		t.Fatalf("expected movie-events topic, got %q", broker.topic)
	}
	var envelope eventEnvelope
	if err := json.Unmarshal(broker.message, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Type != "movie" || envelope.Payload["action"] != "viewed" {
		t.Fatalf("unexpected envelope: %#v", envelope)
	}
}

func TestPublishFailureReturnsServiceUnavailable(t *testing.T) {
	broker := &fakeBroker{err: errors.New("Kafka unavailable")}
	handler := newEventServer(broker, "kafka:9092")
	request := httptest.NewRequest(http.MethodPost, "/api/events/user", strings.NewReader(`{"user_id": 1}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", response.Code)
	}
}

func TestEmptyPayloadIsRejected(t *testing.T) {
	broker := &fakeBroker{}
	handler := newEventServer(broker, "kafka:9092")
	request := httptest.NewRequest(http.MethodPost, "/api/events/payment", strings.NewReader(`{}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", response.Code)
	}
}

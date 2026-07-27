package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func testUpstream(t *testing.T, name string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(name + ":" + request.URL.RequestURI()))
	}))
}

func parsedURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	value, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func newTestGateway(t *testing.T, gradual bool, percent int) (*Gateway, func()) {
	t.Helper()
	monolith := testUpstream(t, "monolith")
	movies := testUpstream(t, "movies")
	events := testUpstream(t, "events")
	gateway := newGateway(Config{
		MonolithURL:            parsedURL(t, monolith.URL),
		MoviesServiceURL:       parsedURL(t, movies.URL),
		EventsServiceURL:       parsedURL(t, events.URL),
		GradualMigration:       gradual,
		MoviesMigrationPercent: percent,
	})
	return gateway, func() {
		monolith.Close()
		movies.Close()
		events.Close()
	}
}

func responseBody(t *testing.T, handler http.Handler, path string) string {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", response.Code, response.Body.String())
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestMoviesMigrationDisabledUsesMonolith(t *testing.T) {
	gateway, cleanup := newTestGateway(t, false, 100)
	defer cleanup()

	body := responseBody(t, gateway, "/api/movies?id=42")
	if !strings.HasPrefix(body, "monolith:") {
		t.Fatalf("expected monolith, got %q", body)
	}
}

func TestMoviesMigrationAtOneHundredUsesMoviesService(t *testing.T) {
	gateway, cleanup := newTestGateway(t, true, 100)
	defer cleanup()

	body := responseBody(t, gateway, "/api/movies")
	if !strings.HasPrefix(body, "movies:") {
		t.Fatalf("expected movies service, got %q", body)
	}
}

func TestDomainRouting(t *testing.T) {
	gateway, cleanup := newTestGateway(t, true, 100)
	defer cleanup()

	if body := responseBody(t, gateway, "/api/users"); !strings.HasPrefix(body, "monolith:") {
		t.Fatalf("expected users to go to monolith, got %q", body)
	}
	if body := responseBody(t, gateway, "/api/events/user"); !strings.HasPrefix(body, "events:") {
		t.Fatalf("expected events service, got %q", body)
	}
}

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                   string
	MonolithURL            *url.URL
	MoviesServiceURL       *url.URL
	EventsServiceURL       *url.URL
	GradualMigration       bool
	MoviesMigrationPercent int
}

type Gateway struct {
	config        Config
	monolithProxy http.Handler
	moviesProxy   http.Handler
	eventsProxy   http.Handler
	randomPercent func() int
}

func loadConfig() (Config, error) {
	monolithURL, err := requiredURL("MONOLITH_URL")
	if err != nil {
		return Config{}, err
	}
	moviesURL, err := requiredURL("MOVIES_SERVICE_URL")
	if err != nil {
		return Config{}, err
	}
	eventsURL, err := requiredURL("EVENTS_SERVICE_URL")
	if err != nil {
		return Config{}, err
	}

	port := envOrDefault("PORT", "8000")
	gradual, err := strconv.ParseBool(envOrDefault("GRADUAL_MIGRATION", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("GRADUAL_MIGRATION must be true or false: %w", err)
	}
	percent, err := strconv.Atoi(envOrDefault("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil || percent < 0 || percent > 100 {
		return Config{}, errors.New("MOVIES_MIGRATION_PERCENT must be an integer from 0 to 100")
	}

	return Config{
		Port:                   port,
		MonolithURL:            monolithURL,
		MoviesServiceURL:       moviesURL,
		EventsServiceURL:       eventsURL,
		GradualMigration:       gradual,
		MoviesMigrationPercent: percent,
	}, nil
}

func requiredURL(name string) (*url.URL, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil, fmt.Errorf("%s is required", name)
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("%s must be a valid absolute URL", name)
	}
	return parsed, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func newGateway(config Config) *Gateway {
	source := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &Gateway{
		config:        config,
		monolithProxy: newReverseProxy(config.MonolithURL, "monolith"),
		moviesProxy:   newReverseProxy(config.MoviesServiceURL, "movies-service"),
		eventsProxy:   newReverseProxy(config.EventsServiceURL, "events-service"),
		randomPercent: func() int { return source.Intn(100) },
	}
}

func newReverseProxy(target *url.URL, upstreamName string) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(request *http.Request) {
		originalDirector(request)
		request.Host = target.Host
		request.Header.Set("X-CinemaAbyss-Upstream", upstreamName)
	}
	proxy.ModifyResponse = func(response *http.Response) error {
		response.Header.Set("X-CinemaAbyss-Upstream", upstreamName)
		return nil
	}
	proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, err error) {
		log.Printf("proxy error upstream=%s method=%s path=%s error=%v", upstreamName, request.Method, request.URL.Path, err)
		writeJSON(writer, http.StatusBadGateway, map[string]string{
			"error":    "upstream service is unavailable",
			"upstream": upstreamName,
		})
	}
	return proxy
}

func (gateway *Gateway) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/" {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		writeJSON(writer, http.StatusOK, map[string]any{
			"status":  true,
			"service": "cinemaabyss",
			"message": "CinemaAbyss API is running",
		})
		return
	}

	if request.URL.Path == "/health" {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{
			"status":                   true,
			"service":                  "proxy-service",
			"gradual_migration":        gateway.config.GradualMigration,
			"movies_migration_percent": gateway.config.MoviesMigrationPercent,
		})
		return
	}

	switch {
	case request.URL.Path == "/api/movies/health":
		gateway.moviesProxy.ServeHTTP(writer, request)
	case strings.HasPrefix(request.URL.Path, "/api/movies"):
		gateway.moviesUpstream().ServeHTTP(writer, request)
	case strings.HasPrefix(request.URL.Path, "/api/events"):
		gateway.eventsProxy.ServeHTTP(writer, request)
	case strings.HasPrefix(request.URL.Path, "/api/"):
		gateway.monolithProxy.ServeHTTP(writer, request)
	default:
		writeJSON(writer, http.StatusNotFound, map[string]string{"error": "route not found"})
	}
}

func (gateway *Gateway) moviesUpstream() http.Handler {
	if !gateway.config.GradualMigration || gateway.config.MoviesMigrationPercent == 0 {
		return gateway.monolithProxy
	}
	if gateway.config.MoviesMigrationPercent == 100 {
		return gateway.moviesProxy
	}
	if gateway.randomPercent() < gateway.config.MoviesMigrationPercent {
		return gateway.moviesProxy
	}
	return gateway.monolithProxy
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		log.Printf("failed to encode response: %v", err)
	}
}

func main() {
	config, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	server := &http.Server{
		Addr:              ":" + config.Port,
		Handler:           newGateway(config),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	log.Printf(
		"proxy-service listening on :%s, gradual_migration=%t, movies_migration_percent=%d",
		config.Port,
		config.GradualMigration,
		config.MoviesMigrationPercent,
	)
	log.Fatal(server.ListenAndServe())
}

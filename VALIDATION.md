# Validation report

Generated: 2026-07-27

## Passed

- `src/microservices/proxy`: `go test ./...`
- `src/microservices/events`: `go test ./...`
- `gofmt -d` for proxy and events: no diff
- YAML parsing for Docker Compose, OpenAPI, static Kubernetes manifests,
  Helm values/Chart, and GitHub Actions workflows: no errors
- Postman collection JSON: valid JSON
- JavaScript runner syntax: checked with Node.js

## Environment limitations

Docker, Helm, kubectl, and Minikube are not installed in the execution
environment. Therefore the full integration run, GHCR publication, cluster
deployment, Kafka UI verification, and requested runtime screenshots were not
fabricated. Reproduction commands are in `docs/verification.md`.

The original monolith and movies-service require `github.com/lib/pq`. Their Go
tests could not download that dependency because outbound network access is
disabled in the execution environment. Their source code was not changed except
for deployment configuration.

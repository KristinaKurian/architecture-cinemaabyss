# Проверка решения

## Выполнено в текущем окружении

```text
src/microservices/proxy: go test ./... — PASS
src/microservices/events: go test ./... — PASS
```

Также выполнены `gofmt`, проверка YAML и поиск незаполненных маркеров в рабочих манифестах.

## Полная интеграционная проверка

```bash
docker compose up -d --build
cd tests/postman
npm ci
npm run test:local
```

Проверка Strangler Fig:

```bash
# Только монолит
MOVIES_MIGRATION_PERCENT=0 docker compose up -d --build proxy-service
curl -i http://localhost:8000/api/movies

# Только movies-service
MOVIES_MIGRATION_PERCENT=100 docker compose up -d --build proxy-service
curl -i http://localhost:8000/api/movies
```

В ответе proxy есть заголовок `X-CinemaAbyss-Upstream`.

Проверка Kafka:

```bash
curl -X POST http://localhost:8082/api/events/movie   -H 'Content-Type: application/json'   -d '{"movie_id":1,"title":"The Movie","action":"viewed","user_id":1}'

docker compose logs -f events-service
```

Kafka UI: `http://localhost:8090`.

## Скриншоты

Реальные скриншоты формируются после запуска Docker/Kubernetes и сохраняются в `docs/evidence/`:

- `postman-tests.png`;
- `kafka-topics.png`;
- `kubernetes-movies.png`;
- `kubernetes-events-log.png`;
- `helm-pods.png`.

Искусственные скриншоты в архив не добавлялись.

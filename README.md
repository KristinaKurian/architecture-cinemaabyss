# CinemaAbyss: миграция монолита к микросервисам

Учебный проект демонстрирует постепенное выделение доменов из монолита по паттерну Strangler Fig, событийное взаимодействие через Kafka, развёртывание в Kubernetes и упаковку в Helm chart.

## Реализовано

- C4-диаграмма To-Be: [`docs/c4-container-to-be.puml`](docs/c4-container-to-be.puml);
- API Gateway / proxy-service на Go с процентным переключением movie-трафика;
- events-service на Go с HTTP producer API и Kafka consumer-циклами;
- Docker Compose для PostgreSQL, Kafka, сервисов и Kafka UI;
- Kubernetes Deployment/Service для proxy и events, ConfigMap и Ingress;
- Helm-шаблоны;
- GitHub Actions для API-тестов и публикации четырёх образов в GHCR;
- Newman/Postman-коллекция.

## Сервисы

| Компонент | Порт | Назначение |
|---|---:|---|
| Monolith | 8080 | Пользователи, платежи, подписки и legacy movie API |
| Movies Service | 8081 | Домен каталога фильмов |
| Events Service | 8082 | Публикация и обработка событий Kafka |
| Proxy Service | 8000 | Единая точка входа и Strangler Fig |
| Kafka UI | 8090 | Просмотр брокера и топиков |

## Локальный запуск

```bash
docker compose up -d --build
```

```bash
curl http://localhost:8000/health
curl http://localhost:8000/api/movies
curl http://localhost:8082/api/events/health
```

Остановка:

```bash
docker compose down -v
```

## Strangler Fig

- `GRADUAL_MIGRATION=false` — movie-запросы идут в монолит;
- `GRADUAL_MIGRATION=true`, `MOVIES_MIGRATION_PERCENT=0` — только монолит;
- `GRADUAL_MIGRATION=true`, `MOVIES_MIGRATION_PERCENT=100` — только movies-service;
- промежуточное значение задаёт долю трафика в movies-service.

Выбранный upstream возвращается в заголовке `X-CinemaAbyss-Upstream`.

## Event API

```bash
curl -X POST http://localhost:8082/api/events/user \
  -H 'Content-Type: application/json' \
  -d '{"user_id":1,"username":"user1","action":"logged_in"}'
```

Маршруты:

- `POST /api/events/movie` → `movie-events`;
- `POST /api/events/user` → `user-events`;
- `POST /api/events/payment` → `payment-events`.

## Тесты

```bash
(cd src/microservices/proxy && go test ./...)
(cd src/microservices/events && go test ./...)

cd tests/postman
npm ci
npm run test:local
```

Подробности: [`docs/verification.md`](docs/verification.md).

## Kubernetes

Образы настроены на:

```text
ghcr.io/kristinakurian/architecture-cinemaabyss/<service>:latest
```

Для приватных GHCR-пакетов замените пустой pull-secret по инструкции в [`src/kubernetes/README.md`](src/kubernetes/README.md).

```bash
kubectl apply -f src/kubernetes/namespace.yaml
kubectl apply -f src/kubernetes/configmap.yaml
kubectl apply -f src/kubernetes/secret.yaml
kubectl apply -f src/kubernetes/dockerconfigsecret.yaml
kubectl apply -f src/kubernetes/postgres-init-configmap.yaml
kubectl apply -f src/kubernetes/postgres.yaml
kubectl apply -f src/kubernetes/kafka/kafka.yaml
kubectl apply -f src/kubernetes/monolith.yaml
kubectl apply -f src/kubernetes/movies-service.yaml
kubectl apply -f src/kubernetes/events-service.yaml
kubectl apply -f src/kubernetes/proxy-service.yaml
kubectl apply -f src/kubernetes/ingress.yaml
```

## Helm

```bash
helm lint src/kubernetes/helm
helm upgrade --install cinemaabyss src/kubernetes/helm \
  --namespace cinemaabyss \
  --create-namespace
```

# Proxy Service

API Gateway для постепенной миграции домена фильмов по паттерну Strangler Fig.

- `/api/movies*` — монолит или movies-service согласно `GRADUAL_MIGRATION` и `MOVIES_MIGRATION_PERCENT`.
- `/api/events*` — events-service.
- остальные `/api/*` — монолит.
- `/health` — состояние proxy-service.

При `GRADUAL_MIGRATION=false` запросы фильмов идут в монолит. При включённой миграции значение `0` означает только монолит, `100` — только movies-service, промежуточные значения задают процент трафика в новый сервис.

# CinemaAbyss Helm Chart

```bash
helm lint src/kubernetes/helm
helm upgrade --install cinemaabyss src/kubernetes/helm   --namespace cinemaabyss   --create-namespace
```

Основные параметры находятся в `values.yaml`: репозитории образов, ресурсы, persistence, Ingress и процент миграции movie-трафика.

Для приватных GHCR-образов замените `imagePullSecrets.dockerconfigjson` на base64-кодированное содержимое `~/.docker/config.json`.

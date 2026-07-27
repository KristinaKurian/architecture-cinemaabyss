# Kubernetes deployment

Перед развёртыванием убедитесь, что GHCR-образы опубликованы по путям, указанным в Deployment-манифестах.

`dockerconfigsecret.yaml` содержит безопасную пустую конфигурацию Docker. Для приватных пакетов выполните вход в GHCR и замените секрет:

```bash
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USER" --password-stdin
kubectl create secret generic dockerconfigjson   --from-file=.dockerconfigjson="$HOME/.docker/config.json"   --type=kubernetes.io/dockerconfigjson   --namespace cinemaabyss   --dry-run=client -o yaml > src/kubernetes/dockerconfigsecret.yaml
```

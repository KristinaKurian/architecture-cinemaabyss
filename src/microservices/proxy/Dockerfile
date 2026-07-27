FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o proxy-service .

FROM alpine:3.21
RUN apk --no-cache add ca-certificates && addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=builder /app/proxy-service ./proxy-service
USER app
EXPOSE 8000
ENTRYPOINT ["./proxy-service"]

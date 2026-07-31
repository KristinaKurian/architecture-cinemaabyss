# Events Service

MVP event-сервис с HTTP producer API и встроенными Kafka consumer-циклами.

Маршруты:

- `GET /api/events/health`
- `POST /api/events/movie` → `movie-events`
- `POST /api/events/user` → `user-events`
- `POST /api/events/payment` → `payment-events`

Сервис использует минимальный Kafka protocol client из стандартной библиотеки Go: публикует сообщения без сжатия в partition `0`, читает их и записывает обработанные события в лог.

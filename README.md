```
cd producer
go mod init github.com/NaNd/kafka-two-services/producer
go get github.com/segmentio/kafka-go
go get github.com/gin-gonic/gin
```
```
cd ../consumer
go mod init github.com/NaNd/kafka-two-services/consumer
go get github.com/segmentio/kafka-go
```

👇

```
docker compose up -d
```

👇

```
go run ./cmd/producer
go run ./cmd/consumer
```

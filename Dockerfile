FROM --platform=linux/amd64 golang:1.27-alpine AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /hub ./cmd/hub/main.go

FROM --platform=linux/amd64 alpine:latest
COPY --from=builder /hub /hub
RUN chmod +x /hub
EXPOSE 8080
ENTRYPOINT ["/hub"]

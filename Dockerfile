# =============================================================================
# Stage 1: Build Go-сборщик
# =============================================================================
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /build

# Копируем только go.mod и go.sum для кэширования зависимостей
COPY go.mod go.sum ./
RUN go mod download

# Копируем исходный код
COPY . .

# Сборка collector (без cgo — collector не использует Rust-валидатор)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /build/collector ./cmd/collector/

# =============================================================================
# Stage 2: Минимальный образ
# =============================================================================
FROM gcr.io/distroless/base-debian12:latest

WORKDIR /app

COPY --from=builder /build/collector .

EXPOSE 8080

ENTRYPOINT ["/app/collector"]

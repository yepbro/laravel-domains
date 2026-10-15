# Multi-stage образ Go-сервисов Domain Crawler.
#
#   dev         — toolchain + air + delve + golangci-lint + migrate (hot reload)
#   build       — статическая сборка всех cmd/*
#   runtime     — минимальный образ (distroless), для проверки боевой сборки
#   mockserver  — alpine + бинарник mockserver (нужен wget для healthcheck)
#
# Версии фиксируются через ARG (значения по умолчанию из deploy/docker/.env).

# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26.9
ARG ALPINE_VERSION=3.24.2

# --------------------------------------------------------------------------
FROM golang:${GO_VERSION}-alpine AS base
ENV GOFLAGS=-mod=readonly \
    CGO_ENABLED=0
WORKDIR /src
# Сначала только манифесты модулей — слой с зависимостями кэшируется,
# пока не меняются go.mod/go.sum (ТЗ 6.2). Используется встроенный кэш образа
# (/go/pkg/mod); рантайм-тома /gomod и /gocache подключаются в dev-стейдже.
COPY go.mod go.sum ./
RUN go mod download

# --------------------------------------------------------------------------
FROM base AS dev
ARG AIR_VERSION=v1.67.4
ARG DELVE_VERSION=v1.27.2
ARG GOLANGCI_LINT_VERSION=v2.14.0
ARG MIGRATE_VERSION=v4.20.1
RUN apk add --no-cache build-base git ca-certificates tzdata \
 && go install github.com/air-verse/air@${AIR_VERSION} \
 && go install github.com/go-delve/delve/cmd/dlv@${DELVE_VERSION} \
 && go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION} \
 && go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@${MIGRATE_VERSION}
# Рантайм-кэши в именованных томах (compose); 1777 — чтобы том был доступен
# любому UID разработчика (user задаётся в compose).
RUN mkdir -p /gomod /gocache && chmod 1777 /gomod /gocache
ENV PATH="/go/bin:${PATH}" \
    GOMODCACHE=/gomod \
    GOCACHE=/gocache
# Пользователь задаётся на уровне compose (user: DEV_UID:DEV_GID),
# чтобы bind-mount'ы принадлежали разработчику (ТЗ 7.4).

# --------------------------------------------------------------------------
FROM base AS build
COPY . .
# Статические бинарники всех сервисов (ТЗ 6.1: CGO_ENABLED=0, -trimpath, -ldflags).
RUN set -eux; \
    for cmd in ingest scheduler dns-worker http-worker render-worker analyzer monitor crawlerctl mockserver testbedgen; do \
        CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/$cmd ./cmd/$cmd; \
    done

# --------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
WORKDIR /app
COPY --from=build /out/ /app/
USER nonroot
# Точка входа задаётся на уровне compose: ["/app/<сервис>"]

# --------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION} AS mockserver
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S -g 10001 mock \
 && adduser -S -H -u 10001 -G mock mock
COPY --from=build /out/mockserver /usr/local/bin/mockserver
USER mock
EXPOSE 80 443 8080
ENTRYPOINT ["/usr/local/bin/mockserver"]

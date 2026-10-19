# Multi-stage образ Go-сервисов Domain Crawler.
#
#   dev         — toolchain + air + delve + golangci-lint + migrate (hot reload)
#   css         — alpine + standalone-CLI tailwindcss v4: бандл лендинга
#   build       — статическая сборка всех cmd/* (CSS из стадии css подменяет
#                 placeholder до go build — go:embed встраивает собранный файл)
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
# go-mutesting — форк avito-tech одноимённого инструмента zimmski
# (апстрим мёртв и несовместим с современными тулчейнами); версия пинится
# в .env (GO_MUTESTING_VERSION), make mutate — docs/mutation-testing.md.
ARG GO_MUTESTING_VERSION=v0.0.0-20251226130216-48d0401f00fb
RUN apk add --no-cache build-base git ca-certificates tzdata \
 && go install github.com/air-verse/air@${AIR_VERSION} \
 && go install github.com/go-delve/delve/cmd/dlv@${DELVE_VERSION} \
 && go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION} \
 && go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@${MIGRATE_VERSION} \
 && go install github.com/avito-tech/go-mutesting/cmd/go-mutesting@${GO_MUTESTING_VERSION}
# Рантайм-кэши в именованных томах (compose); 1777 — чтобы том был доступен
# любому UID разработчика (user задаётся в compose).
RUN mkdir -p /gomod /gocache && chmod 1777 /gomod /gocache
ENV PATH="/go/bin:${PATH}" \
    GOMODCACHE=/gomod \
    GOCACHE=/gocache
# Пользователь задаётся на уровне compose (user: DEV_UID:DEV_GID),
# чтобы bind-mount'ы принадлежали разработчику (ТЗ 7.4).

# --------------------------------------------------------------------------
# Стадия css — бандл Tailwind лендинга (T-122/T-124): standalone-CLI
# tailwindcss v4 собирает internal/web/static/app.css из исходника
# app.src.css (классы разметки сканируются @source-ом шаблона). Node не
# появляется ни в сборке проекта, ни в рантайме; собранный бандл в git
# не хранится (там placeholder) — стадия подменяет его в build до go
# build, поэтому go:embed встраивает настоящий CSS.
FROM alpine:${ALPINE_VERSION} AS css
ARG TAILWINDCSS_VERSION=v4.3.3
# Автоматический ARG платформы: у CLI отдельные musl-сборки под x64/arm64.
ARG TARGETARCH
# CLI собран Bun под musl, но динамически линкуется к C++-рантайму —
# libstdc++/libgcc это закрывают (сотни килобайт, живут только в стадии
# css, в рантайм-образ не попадают).
RUN apk add --no-cache libstdc++ libgcc
RUN set -eux; \
    case "${TARGETARCH}" in \
        amd64) arch=x64 ;; \
        arm64) arch=arm64 ;; \
        *) echo "css: неподдерживаемая архитектура ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    wget -q -O /usr/local/bin/tailwindcss \
        "https://github.com/tailwindlabs/tailwindcss/releases/download/${TAILWINDCSS_VERSION}/tailwindcss-linux-${arch}-musl" \
    && chmod +x /usr/local/bin/tailwindcss
WORKDIR /src/internal/web/static
# Только исходник CSS и @source-шаблон: правки Go-кода не инвалидируют слой.
COPY internal/web/static/app.src.css ./
COPY internal/web/templates/landing.html ../templates/
RUN /usr/local/bin/tailwindcss -i app.src.css -o /out/app.css --minify

# --------------------------------------------------------------------------
FROM base AS build
# Версия/ревизия для crawler_build_info (T-111): .git исключён из контекста
# сборки (.dockerignore), поэтому значения вычисляет вызывающая сторона
# (make build-runtime) и передаёт build-аргами; прямая сборка docker build
# без арок получает unknown.
ARG BUILD_VERSION=unknown
ARG BUILD_REVISION=unknown
COPY . .
# Бандл Tailwind из стадии css заменяет placeholder (см. шапку): go:embed
# static в internal/web заберёт собранный CSS, а не заглушку из git.
COPY --from=css /out/app.css internal/web/static/app.css
# Статические бинарники всех сервисов (ТЗ 6.1: CGO_ENABLED=0, -trimpath, -ldflags).
# buildMode=prod честно помечает боевые сборки в crawler_build_info
# (dev без -X остаётся dev-stub); buildVersion/buildRevision — git-тег/хеш
# сборки (T-111).
RUN set -eux; \
    for cmd in ingest scheduler dns-worker http-worker render-worker analyzer monitor web dashboard crawlerctl mockserver testbedgen; do \
        CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X crawler/internal/metrics.buildMode=prod -X crawler/internal/metrics.buildVersion=${BUILD_VERSION} -X crawler/internal/metrics.buildRevision=${BUILD_REVISION}" -o /out/$cmd ./cmd/$cmd; \
    done

# --------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
WORKDIR /app
COPY --from=build /out/ /app/
USER nonroot
# Точка входа задаётся на уровне compose: ["/app/<сервис>"]

# --------------------------------------------------------------------------
# Сборка mockserver — отдельная стадия: исходник ограничен stdlib, поэтому
# правки остального модуля (включая тесты) не инвалидируют слой, а make
# test-integration пересобирает образ быстро (--build на up тестбеда).
# buildMode не инъекцируется: mockserver не линкует internal/metrics.
FROM base AS mockserver-build
COPY go.mod go.sum ./
COPY cmd/mockserver/ cmd/mockserver/
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/mockserver ./cmd/mockserver

# --------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION} AS mockserver
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S -g 10001 mock \
 && adduser -S -H -u 10001 -G mock mock
COPY --from=mockserver-build /out/mockserver /usr/local/bin/mockserver
USER mock
EXPOSE 80 443 8080
ENTRYPOINT ["/usr/local/bin/mockserver"]

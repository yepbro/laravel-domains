# Multi-stage image of the Domain Crawler Go services.
#
#   dev         — toolchain + air + delve + golangci-lint + migrate (hot reload)
#   build       — a static build of all cmd/*
#   runtime     — the minimal image (distroless), to verify the production build
#   mockserver  — alpine + the mockserver binary (wget is needed for the healthcheck)
#
# Versions are pinned via ARG (the defaults come from deploy/docker/.env).

# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26.9
ARG ALPINE_VERSION=3.24.2

# --------------------------------------------------------------------------
FROM golang:${GO_VERSION}-alpine AS base
ENV GOFLAGS=-mod=readonly \
    CGO_ENABLED=0
WORKDIR /src
# First the module manifests only — the dependency layer stays cached
# as long as go.mod/go.sum do not change (spec 6.2). The built-in image
# cache is used (/go/pkg/mod); the /gomod and /gocache runtime volumes
# are mounted in the dev stage.
COPY go.mod go.sum ./
RUN go mod download

# --------------------------------------------------------------------------
FROM base AS dev
ARG AIR_VERSION=v1.67.4
ARG DELVE_VERSION=v1.27.2
ARG GOLANGCI_LINT_VERSION=v2.14.0
ARG MIGRATE_VERSION=v4.20.1
# go-mutesting — the avito-tech fork of zimmski's tool of the same name
# (the upstream is dead and incompatible with modern toolchains); the version
# is pinned in .env (GO_MUTESTING_VERSION), make mutate — docs/mutation-testing.md.
ARG GO_MUTESTING_VERSION=v0.0.0-20251226130216-48d0401f00fb
RUN apk add --no-cache build-base git ca-certificates tzdata \
 && go install github.com/air-verse/air@${AIR_VERSION} \
 && go install github.com/go-delve/delve/cmd/dlv@${DELVE_VERSION} \
 && go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION} \
 && go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@${MIGRATE_VERSION} \
 && go install github.com/avito-tech/go-mutesting/cmd/go-mutesting@${GO_MUTESTING_VERSION}
# The runtime caches live in named volumes (compose); 1777 — so that the
# volume is writable by any developer UID (the user is set in compose).
RUN mkdir -p /gomod /gocache && chmod 1777 /gomod /gocache
ENV PATH="/go/bin:${PATH}" \
    GOMODCACHE=/gomod \
    GOCACHE=/gocache
# The user is set at the compose level (user: DEV_UID:DEV_GID),
# so that the bind-mounts belong to the developer (spec 7.4).

# --------------------------------------------------------------------------
FROM base AS build
# The version/revision for crawler_build_info (T-111): .git is excluded
# from the build context (.dockerignore), so the caller computes the values
# (make build-runtime) and passes them as build args; a direct docker build
# without the args gets unknown.
ARG BUILD_VERSION=unknown
ARG BUILD_REVISION=unknown
COPY . .
# Static binaries of all the services (spec 6.1: CGO_ENABLED=0, -trimpath, -ldflags).
# buildMode=prod honestly marks the production builds in crawler_build_info
# (dev without -X stays a dev-stub); buildVersion/buildRevision — the git
# tag/hash of the build (T-111).
RUN set -eux; \
    for cmd in ingest scheduler dns-worker http-worker render-worker analyzer monitor web dashboard crawlerctl mockserver testbedgen; do \
        CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X crawler/internal/metrics.buildMode=prod -X crawler/internal/metrics.buildVersion=${BUILD_VERSION} -X crawler/internal/metrics.buildRevision=${BUILD_REVISION}" -o /out/$cmd ./cmd/$cmd; \
    done

# --------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
WORKDIR /app
COPY --from=build /out/ /app/
USER nonroot
# The entrypoint is set at the compose level: ["/app/<service>"]

# --------------------------------------------------------------------------
# The mockserver build — a separate stage: its source is limited to the
# stdlib, so edits of the rest of the module (including the tests) do not
# invalidate the layer, and make test-integration rebuilds the image
# quickly (--build on the testbed up).
# buildMode is not injected: mockserver does not link internal/metrics.
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

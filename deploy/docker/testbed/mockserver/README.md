# mockserver

A Go application for the testbed's "nasty" scenarios (spec 4.4): tarpit,
slow responses, huge bodies, a zip bomb, a dropped connection, infinite
and cyclic redirects, broken TLS certificates, cookies.

Source code: [`cmd/mockserver`](../../../cmd/mockserver) (the project's
shared Go module, so the repository's linter and tests can be used).

The image is built from `deploy/docker/Dockerfile.go` (target
`mockserver`); the container listens on `:80` (HTTP), `:443` (TLS via SNI)
and `:8080` (healthcheck).

The testbed scenarios (`site: {template: mock, mock_path: …}` in
[`../scenarios.yaml`](../scenarios.yaml)) are addressed by virtual hosts:
a `/` request with the scenario's Host (for example, `tarpit.crawl.test`)
is routed to its handler (`/tarpit`) — the HTTP worker requests only the
main page (spec 5.5, §9), so the nasty response arrives at `/`. The TLS
scenarios (`tls-self-signed` / `tls-expired` / `tls-wrong-san`) are served,
per SNI, certificates with the matching defect; the debug names
`*.bad-tls.crawl.test` are kept.

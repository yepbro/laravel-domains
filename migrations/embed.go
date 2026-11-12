// Package migrations — SQL migrations of the PostgreSQL schema (the
// golang-migrate format, the <version><name>.up.sql / .down.sql pairs).
// The files are embedded into the service binaries via go:embed, so a
// service does not need an external migrations directory at startup.
//
// A new migration: make migrate-new NAME=description — a pair of files is
// created in this directory and automatically lands in the next build.
package migrations

import "embed"

// FS — the embedded SQL migration files (only *.sql of this directory).
//
// It is used as the source/iofs source by the golang-migrate library
// (internal/migrate) and by the set integrity unit tests.
//
//go:embed *.sql
var FS embed.FS

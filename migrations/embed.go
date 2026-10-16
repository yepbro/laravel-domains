// Package migrations — SQL-миграции схемы PostgreSQL (формат golang-migrate,
// пары <версия><имя>.up.sql / .down.sql). Файлы встраиваются в бинарники
// сервисов через go:embed, поэтому при старте сервису не нужен внешний
// каталог миграций.
//
// Новая миграция: make migrate-new NAME=описание — пара файлов создаётся
// в этом каталоге и автоматически попадает в следующую сборку.
package migrations

import "embed"

// FS — встроенные SQL-файлы миграций (только *.sql этого каталога).
//
// Используется источником source/iofs библиотеки golang-migrate
// (internal/migrate) и юнит-тестами целостности набора.
//
//go:embed *.sql
var FS embed.FS

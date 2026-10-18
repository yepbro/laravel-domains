#!/bin/sh
# Роль приложения + права на БД (ТЗ 4.1: init-скрипты создают БД, роль, расширения).
# Выполняется postgres-entrypoint от имени POSTGRES_ADMIN_USER при первом старте.
#
# Имя роли и пароль передаются psql-переменными и квотируются самим psql
# (:'var' — SQL-литерал, :"var" — идентификатор, format %L/%I — в генерируемом
# операторе): спецсимволы пароля (', \, $) не ломают SQL и не создают пароль,
# отличный от ожидаемого приложением. CREATE ROLE не имеет IF NOT EXISTS,
# поэтому оператор генерируется условно и исполняется \gexec (0 строк —
# роль существует — ничего не исполняется); безусловный ALTER идемпотентно
# доводит пароль.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v app_user="$APP_DB_USER" -v app_password="$APP_DB_PASSWORD" \
    -v db_name="$POSTGRES_DB" <<-EOSQL
    SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'app_user', :'app_password')
    WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'app_user')
    \gexec
    ALTER ROLE :"app_user" WITH LOGIN PASSWORD :'app_password';
    GRANT ALL PRIVILEGES ON DATABASE :"db_name" TO :"app_user";
    ALTER DATABASE :"db_name" OWNER TO :"app_user";
EOSQL

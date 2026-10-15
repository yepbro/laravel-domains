#!/bin/sh
# Роль приложения + права на БД (ТЗ 4.1: init-скрипты создают БД, роль, расширения).
# Выполняется postgres-entrypoint от имени POSTGRES_ADMIN_USER при первом старте.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${APP_DB_USER}') THEN
            CREATE ROLE ${APP_DB_USER} LOGIN PASSWORD '${APP_DB_PASSWORD}';
        ELSE
            ALTER ROLE ${APP_DB_USER} WITH LOGIN PASSWORD '${APP_DB_PASSWORD}';
        END IF;
    END
    \$\$;
    GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_DB} TO ${APP_DB_USER};
    ALTER DATABASE ${POSTGRES_DB} OWNER TO ${APP_DB_USER};
EOSQL

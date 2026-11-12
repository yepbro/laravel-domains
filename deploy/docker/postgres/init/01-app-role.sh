#!/bin/sh
# The application role + DB grants (spec 4.1: the init scripts create the DB,
# the role, the extensions). Run by the postgres entrypoint as
# POSTGRES_ADMIN_USER on the first start.
#
# The role name and the password are passed as psql variables and quoted by
# psql itself (:'var' — an SQL literal, :"var" — an identifier, format %L/%I —
# in the generated statement): password special characters (', \, $) do not
# break the SQL and do not produce a password different from the one the
# application expects. CREATE ROLE has no IF NOT EXISTS, so the statement is
# generated conditionally and executed via \gexec (0 rows — the role exists —
# nothing is executed); the unconditional ALTER brings the password up to
# date idempotently.
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

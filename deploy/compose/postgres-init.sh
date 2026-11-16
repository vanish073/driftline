#!/bin/bash
# Runs once, when the PostgreSQL volume is created. The two NOLOGIN roles of DESIGN §12 (V1 creates them only if they
# are missing), and the application's login, which has only driftline_app's privileges. Migrations run as the
# superuser, which plays driftline_migrator here, as in the integration tests (D29).
set -euo pipefail
psql -v ON_ERROR_STOP=1 -v app_password="$DRIFTLINE_DB_PASSWORD" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'EOSQL'
CREATE ROLE driftline_migrator NOLOGIN;
CREATE ROLE driftline_app NOLOGIN;
CREATE ROLE driftline LOGIN PASSWORD :'app_password' IN ROLE driftline_app;
EOSQL

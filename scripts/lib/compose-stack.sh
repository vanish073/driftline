# Shared by scripts/e2e.sh and scripts/chaos.sh (sourced, never run): build, generate the secrets of one run, start the
# Compose stack, wait for it, and always save its logs and tear it down. Expects DEV and COMPOSE to be set.
# shellcheck source=gen-tls.sh
. "$(dirname "${BASH_SOURCE[0]}")/gen-tls.sh"

# Builds the Boot jar and the sandbox (plus any extra Gradle tasks), then stops Gradle to free memory (DESIGN §15.1).
stack_build() {
    ./gradlew :app:bootJar :sandbox:installDist "$@" --quiet
    ./gradlew --stop --quiet || true
}

# A fresh merchant pool (random webhook secrets and tokens, the dispatcher's service token) and random database
# passwords, secrets key and personal-data keys (D77) under $DEV (git-ignored; nothing secret is ever committed).
# Secrets are files, one per setting, never environment variables (Shift 11, D97): each service mounts only its own
# directory under $DEV/secrets read-only (the app's at /run/secrets/driftline, where the file name is the setting).
# $DEV/secrets is private to the user on the host; the files in it are readable inside the containers, which run as
# other users. $DEV/env holds only the extra, non-secret settings given here.
#   stack_secrets MERCHANTS [EXTRA_ENV_LINE...]
stack_secrets() {
    local merchants=$1
    shift
    mkdir -p "$DEV"
    sandbox/build/install/sandbox/bin/sandbox bootstrap --out "$DEV/bootstrap.json" --merchants "$merchants" \
        --partitions 12 --days-back "${BOOTSTRAP_DAYS_BACK:-3}"
    local s="$DEV/secrets"
    rm -rf "$s"
    (umask 077 && mkdir -p "$s")
    local dirs=(app migrate verify postgres grafana prometheus)
    for d in "${dirs[@]}"; do
        mkdir -p "$s/$d"
        chmod 755 "$s/$d"
    done
    put() {
        printf '%s' "$2" > "$1"
        chmod 444 "$1"
    }
    # The service token (observations:write) is generated into the bootstrap file, which registers its hash.
    local service_token db_password admin_password
    service_token=$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["service_token"])' \
        "$DEV/bootstrap.json")
    db_password=$(openssl rand -hex 16)
    admin_password=$(openssl rand -hex 16)
    put "$s/app/spring.datasource.password" "$db_password"
    put "$s/app/driftline.secrets.keys.dev" "$(openssl rand -base64 32)"
    put "$s/app/driftline.pii.keys.dev" "$(openssl rand -base64 32)"
    put "$s/app/driftline.pii.index-key" "$(openssl rand -base64 32)"
    put "$s/app/driftline.dispatcher.service-token" "$service_token"
    put "$s/migrate/spring.datasource.password" "$admin_password"
    put "$s/verify/spring.datasource.password" "$db_password"
    put "$s/postgres/postgres-password" "$admin_password"
    put "$s/postgres/app-password" "$db_password"
    put "$s/grafana/admin-password" "$(openssl rand -hex 16)"
    # D98: Prometheus scrapes the management port with its own token, and asks for a password on its UI and API
    local metrics_token prometheus_password
    metrics_token=$(openssl rand -hex 24)
    prometheus_password=$(openssl rand -hex 16)
    put "$s/app/driftline.metrics.token" "$metrics_token"
    put "$s/prometheus/metrics-token" "$metrics_token"
    put "$s/grafana/prometheus-password" "$prometheus_password"
    put "$s/prometheus/web.yml" "basic_auth_users:
  prometheus: $(python3 -W ignore -I -c 'import crypt, sys
print(crypt.crypt(sys.argv[1], crypt.mksalt(crypt.METHOD_BLOWFISH, rounds=1 << 10)))' "$prometheus_password")
"
    # D100: TLS inside the stack. One CA per run, a certificate per service; PostgreSQL and Kafka refuse plaintext.
    tls_stack "$s" driftline,matcher,dispatcher,localhost,127.0.0.1 sandbox,localhost,127.0.0.1
    put "$s/prometheus/ca.crt" "$(cat "$s/ca.crt")"   # Prometheus checks the management port's certificate
    {
        for line in "$@"; do
            echo "$line"
        done
    } > "$DEV/env"
}

# The EXIT trap: saves the Compose logs to $DEV/compose.log, scans the stack for leaked personal data (D77; it fails the
# run if any is found, or if the vault is empty while the run otherwise passed) and removes the stack, volumes included.
stack_teardown() {
    local status=$?
    "${COMPOSE[@]}" logs --no-color --timestamps > "$DEV/compose.log" 2>&1 || true
    local scan=0
    scripts/pii-scan.sh "$DEV/compose.log" "${COMPOSE[@]}" || scan=$?
    if [ "$status" -eq 0 ] && [ "$scan" -ne 0 ]; then
        status=$scan
    fi
    "${COMPOSE[@]}" down --volumes --remove-orphans > /dev/null 2>&1 || true
    exit "$status"
}

# For tools on the host (D100): the API and the sandbox speak HTTPS, signed by the run's CA. curl takes the CA file;
# Java tools take the truststore through JAVA_OPTS.
stack_tls_java() {
    echo "-Djavax.net.ssl.trustStore=$DEV/secrets/truststore.p12 -Djavax.net.ssl.trustStorePassword=changeit" \
        "-Djavax.net.ssl.trustStoreType=PKCS12"
}

# Starts the stack and waits for the sandbox, which itself waits for a healthy Driftline.
stack_up() {
    "${COMPOSE[@]}" up --detach --build
    echo "waiting for the stack (sandbox up implies a healthy Driftline)..."
    for _ in $(seq 1 150); do
        if curl -fsS --cacert "$DEV/secrets/ca.crt" https://127.0.0.1:8085/health > /dev/null 2>&1; then
            break
        fi
        if "${COMPOSE[@]}" ps --all --format '{{.Service}} {{.State}} {{.ExitCode}}' \
                | grep -E '^(migrate|kafka-init) exited [1-9]'; then
            echo "a one-shot service failed" >&2
            exit 1
        fi
        sleep 2
    done
    curl -fsS --cacert "$DEV/secrets/ca.crt" https://127.0.0.1:8085/health > /dev/null
    "${COMPOSE[@]}" ps
}

# The header of a results file: what ran, where, on which commit.
stack_header() {
    echo "# $1, $(date -u +%Y-%m-%dT%H:%M:%SZ), commit $(git rev-parse --short HEAD)$(git diff --quiet || echo '+uncommitted')"
    echo "# host: $(uname -sr), $(nproc) cores, $(free -g | awk '/Mem:/ {print $2}') GB RAM; $2"
}

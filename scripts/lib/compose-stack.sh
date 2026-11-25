# Shared by scripts/e2e.sh and scripts/chaos.sh (sourced, never run): build, generate the secrets of one run, start the
# Compose stack, wait for it, and always save its logs and tear it down. Expects DEV and COMPOSE to be set.

# Builds the Boot jar and the sandbox (plus any extra Gradle tasks), then stops Gradle to free memory (DESIGN §15.1).
stack_build() {
    ./gradlew :app:bootJar :sandbox:installDist "$@" --quiet
    ./gradlew --stop --quiet || true
}

# A fresh merchant pool (random webhook secrets and tokens, the dispatcher's service token) and random database
# passwords, secrets key and personal-data keys (D77) under $DEV (git-ignored; nothing secret is ever committed). Extra env lines may follow.
#   stack_secrets MERCHANTS [EXTRA_ENV_LINE...]
stack_secrets() {
    local merchants=$1
    shift
    mkdir -p "$DEV"
    sandbox/build/install/sandbox/bin/sandbox bootstrap --out "$DEV/bootstrap.json" --merchants "$merchants" \
        --partitions 12 --days-back "${BOOTSTRAP_DAYS_BACK:-3}"
    umask 077
    # The service token (observations:write) is generated into the bootstrap file, which registers its hash.
    local service_token
    service_token=$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["service_token"])' \
        "$DEV/bootstrap.json")
    {
        echo "POSTGRES_PASSWORD=$(openssl rand -hex 16)"
        echo "DRIFTLINE_DB_PASSWORD=$(openssl rand -hex 16)"
        echo "DRIFTLINE_SECRETS_KEY=$(openssl rand -base64 32)"
        echo "DRIFTLINE_PII_KEY=$(openssl rand -base64 32)"
        echo "DRIFTLINE_PII_INDEX_KEY=$(openssl rand -base64 32)"
        echo "GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 16)"
        echo "DRIFTLINE_SERVICE_TOKEN=$service_token"
        for line in "$@"; do
            echo "$line"
        done
    } > "$DEV/env"
    umask 022
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

# Starts the stack and waits for the sandbox, which itself waits for a healthy Driftline.
stack_up() {
    "${COMPOSE[@]}" up --detach --build
    echo "waiting for the stack (sandbox up implies a healthy Driftline)..."
    for _ in $(seq 1 150); do
        if curl -fsS http://127.0.0.1:8085/health > /dev/null 2>&1; then
            break
        fi
        if "${COMPOSE[@]}" ps --all --format '{{.Service}} {{.State}} {{.ExitCode}}' \
                | grep -E '^(migrate|kafka-init) exited [1-9]'; then
            echo "a one-shot service failed" >&2
            exit 1
        fi
        sleep 2
    done
    curl -fsS http://127.0.0.1:8085/health > /dev/null
    "${COMPOSE[@]}" ps
}

# The header of a results file: what ran, where, on which commit.
stack_header() {
    echo "# $1, $(date -u +%Y-%m-%dT%H:%M:%SZ), commit $(git rev-parse --short HEAD)$(git diff --quiet || echo '+uncommitted')"
    echo "# host: $(uname -sr), $(nproc) cores, $(free -g | awk '/Mem:/ {print $2}') GB RAM; $2"
}

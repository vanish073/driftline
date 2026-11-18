#!/usr/bin/env bash
# The kind smoke test (DESIGN §15.3, D80), run by CI (nightly, on demand, and on pull requests that change the
# manifests); it needs Docker, kind and kubectl, and about 5 GB of memory, so it does not run on the 4 GB laptop.
#
#   scripts/kind-smoke.sh [results-file]        KEEP_CLUSTER=1 leaves the cluster up afterwards
#
# 1. Builds the Boot jar, the sandbox and the packaged e2e suite with Gradle, then both images.
# 2. Generates fresh secrets and a merchant pool under deploy/k8s/overlays/kind/.dev/ (git-ignored).
# 3. Creates a fresh kind cluster (deploy/k8s/overlays/kind/cluster.yaml) and loads the images into it.
# 4. Applies the kind overlay stage by stage and waits for each: infra (PostgreSQL, Kafka, the topic, Secrets,
#    NetworkPolicies), migrate (the Flyway Job), app (every role and the sandbox).
# 5. Checks the NetworkPolicies from an unlabelled pod: the API is reachable, PostgreSQL and Kafka are not.
# 6. Runs the e2e suite against the cluster: one scenario per break type, each detected through the ops API, handled
#    by the resolver and the dispatcher, plus the manual resolution and the ingest security checks.
# 7. Runs the verify Job (`driftline verify`) and requires it to complete.
# 8. Always saves every pod's log to build/kind/, checks them for raw customer contacts, and deletes the cluster.
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS=${1:-}
CLUSTER=driftline-smoke
NS=driftline
OVERLAY=deploy/k8s/overlays/kind
DEV=$OVERLAY/.dev
OUT=build/kind
BUSYBOX=busybox:1.37.0@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec
K=(kubectl --context "kind-$CLUSTER" -n "$NS")
mkdir -p "$OUT"

teardown() {
    local status=$?
    if kind get clusters 2> /dev/null | grep -qx "$CLUSTER"; then
        {
            "${K[@]}" get all,networkpolicy,job -o wide || true
            for pod in $("${K[@]}" get pods -o name 2> /dev/null); do
                echo "===== $pod"
                "${K[@]}" logs --all-containers --prefix "$pod" 2>&1 || true
                "${K[@]}" logs --all-containers --prefix --previous "$pod" 2> /dev/null || true
            done
        } > "$OUT/kind.log" 2>&1
        # D77: the sandbox's fake contacts must not appear in any pod's log
        if grep -aEq 'buyer[0-9]{6}@(okaxis|example\.com)|(^|[^0-9A-Za-z_])6000[0-9]{6}([^0-9A-Za-z_]|$)' "$OUT/kind.log"; then
            echo "kind-smoke: FAIL: a raw customer contact appears in a pod's log" >&2
            [ "$status" -eq 0 ] && status=1
        else
            echo "kind-smoke: no raw customer contact in any pod's log"
        fi
        if [ "${KEEP_CLUSTER:-0}" != 1 ]; then
            kind delete cluster --name "$CLUSTER" > /dev/null 2>&1 || true
        fi
    fi
    exit "$status"
}

smoke() {
    echo "kind-smoke: building"
    ./gradlew :app:bootJar :sandbox:installDist :sandbox:e2eDist --quiet
    ./gradlew --stop --quiet || true
    docker build -q -f deploy/docker/app.Dockerfile -t driftline-app:dev . > /dev/null
    docker build -q -f deploy/docker/sandbox.Dockerfile -t driftline-sandbox:dev . > /dev/null

    echo "kind-smoke: generating secrets under $DEV"
    rm -rf "$DEV"
    mkdir -p "$DEV"
    sandbox/build/install/sandbox/bin/sandbox bootstrap --out "$DEV/bootstrap.json" --merchants 12 \
        --partitions 12 --days-back 3
    local service_token db_password
    service_token=$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["service_token"])' \
        "$DEV/bootstrap.json")
    db_password=$(openssl rand -hex 16)
    (
        umask 077
        {
            echo "SPRING_DATASOURCE_PASSWORD=$db_password"
            echo "DRIFTLINE_SECRETS_KEYS_K1=$(openssl rand -base64 32)"
            echo "DRIFTLINE_PII_KEYS_P1=$(openssl rand -base64 32)"
            echo "DRIFTLINE_PII_INDEX_KEY=$(openssl rand -base64 32)"
            echo "DRIFTLINE_DISPATCHER_SERVICE_TOKEN=$service_token"
        } > "$DEV/driftline-secrets.env"
        local postgres_password
        postgres_password=$(openssl rand -hex 16)
        printf 'SPRING_DATASOURCE_USERNAME=postgres\nSPRING_DATASOURCE_PASSWORD=%s\n' "$postgres_password" \
            > "$DEV/driftline-migrator.env"
        printf 'POSTGRES_PASSWORD=%s\nDRIFTLINE_DB_PASSWORD=%s\n' "$postgres_password" "$db_password" \
            > "$DEV/driftline-infra.env"
    )

    echo "kind-smoke: creating cluster $CLUSTER"
    kind delete cluster --name "$CLUSTER" > /dev/null 2>&1 || true
    kind create cluster --name "$CLUSTER" --config "$OVERLAY/cluster.yaml" --wait 180s
    kind load docker-image driftline-app:dev driftline-sandbox:dev --name "$CLUSTER"

    kubectl kustomize --load-restrictor LoadRestrictionsNone "$OVERLAY" > "$OUT/rendered.yaml"
    apply_stage() {
        kubectl --context "kind-$CLUSTER" apply -f "$OUT/rendered.yaml" -l "driftline.io/stage=$1"
    }
    echo "kind-smoke: stage infra"
    apply_stage infra
    "${K[@]}" rollout status statefulset/postgres --timeout=5m
    "${K[@]}" rollout status statefulset/kafka --timeout=5m
    "${K[@]}" wait --for=condition=complete job/kafka-init --timeout=5m
    echo "kind-smoke: stage migrate"
    apply_stage migrate
    "${K[@]}" wait --for=condition=complete job/migrate --timeout=5m
    echo "kind-smoke: stage app"
    apply_stage app
    for d in ingest matcher settlement-worker resolver dispatcher sandbox; do
        "${K[@]}" rollout status "deployment/$d" --timeout=8m
    done
    for _ in $(seq 1 60); do
        if curl -fsS http://127.0.0.1:18085/health > /dev/null 2>&1; then
            break
        fi
        sleep 2
    done
    curl -fsS http://127.0.0.1:18085/health > /dev/null

    echo "kind-smoke: NetworkPolicy check from an unlabelled pod"
    local probe
    probe=$("${K[@]}" run netpol-probe --image="$BUSYBOX" --restart=Never --rm -i --quiet --command -- sh -c '
        nc -z -w 5 ingest 8080 && echo "api: open" || echo "api: closed"
        nc -z -w 5 postgres 5432 && echo "postgres: open" || echo "postgres: closed"
        nc -z -w 5 kafka 9092 && echo "kafka: open" || echo "kafka: closed"' 2>&1 || true)
    echo "$probe"
    if ! { grep -q 'api: open' <<< "$probe" && grep -q 'postgres: closed' <<< "$probe" \
            && grep -q 'kafka: closed' <<< "$probe"; }; then
        echo "kind-smoke: FAIL: the NetworkPolicies do not behave as written" >&2
        return 1
    fi

    echo "kind-smoke: e2e suite"
    local e2e=sandbox/build/e2e cp
    cp="$e2e/classes:$(find "$e2e/lib" -name '*.jar' | sort | tr '\n' ':')"
    BOOTSTRAP_FILE="$DEV/bootstrap.json" DRIFTLINE_URL=http://127.0.0.1:18080 SANDBOX_URL=http://127.0.0.1:18085 \
        java -Xmx192m -jar "$e2e"/launcher/junit-platform-console-standalone-*.jar execute \
        --class-path "$cp" --select-package io.github.vanish073.driftline.e2e --details=tree --disable-banner \
        --disable-ansi-colors

    echo "kind-smoke: verify"
    apply_stage verify
    # verify re-checks for up to 5 minutes; the Job ends as succeeded (CLEAN) or failed (violations)
    local outcome=""
    for _ in $(seq 1 108); do
        outcome=$("${K[@]}" get job/verify -o jsonpath='{.status.succeeded}/{.status.failed}')
        if [ "$outcome" = "1/" ] || [ "${outcome#*/}" != "" ]; then
            break
        fi
        sleep 5
    done
    "${K[@]}" logs job/verify | grep '^verify: ' || true
    if [ "$outcome" != "1/" ]; then
        echo "kind-smoke: FAIL: driftline verify did not succeed (succeeded/failed: '$outcome')" >&2
        return 1
    fi
    echo "kind-smoke: PASS"
}

trap teardown EXIT
if [ -n "$RESULTS" ]; then
    mkdir -p "$(dirname "$RESULTS")"
    {
        echo "# scripts/kind-smoke.sh, $(date -u +%Y-%m-%dT%H:%M:%SZ), commit $(git rev-parse --short HEAD)$(git diff --quiet || echo '+uncommitted')"
        echo "# host: $(uname -sr), $(nproc) cores, $(free -g | awk '/Mem:/ {print $2}') GB RAM; kind $(kind version | awk '{print $2}')"
        smoke
    } 2>&1 | tee "$RESULTS"
    exit "${PIPESTATUS[0]}"
else
    smoke
fi

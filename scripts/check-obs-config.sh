#!/usr/bin/env bash
# Static checks of the observability config (DESIGN §13, D78), run by CI's `obs-config` job; needs Docker only.
#   1. promtool: the Prometheus config and alert rules are valid, and every alert's unit test passes;
#   2. the OpenTelemetry collector validates its config;
#   3. the Grafana dashboard is exactly what scripts/gen-grafana-dashboard.py generates.
# The metric names the board and the rules query are checked against a real scrape in the app's ObservabilityTest.
set -euo pipefail
cd "$(dirname "$0")/.."

PROMETHEUS=prom/prometheus:v3.15.1@sha256:fa3302279710be2f21a8301ec17a8b4f2301fd829bd113839b3b6b02b6f191a2
COLLECTOR=otel/opentelemetry-collector:0.164.0@sha256:28b6d41ac51bff6b41ba5e8fe503569ffb9d6a1ea90f7a78419e86f34e44f750

# The scrape's token file (D98) is a stack's secret; promtool insists it exists, so a dummy stands in for it here.
dummy=$(mktemp -d)
trap 'rm -rf "$dummy"' EXIT
printf 'dummy' > "$dummy/metrics-token"
chmod 644 "$dummy/metrics-token"
docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/etc/prometheus:ro" \
    -v "$dummy/metrics-token:/run/secrets/prometheus/metrics-token:ro" -w /etc/prometheus "$PROMETHEUS" \
    check config prometheus.yml
docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/etc/prometheus:ro" -w /etc/prometheus "$PROMETHEUS" \
    test rules alerts.test.yml
docker run --rm -v "$PWD/deploy/otel/collector.yaml:/etc/otelcol/config.yaml:ro" "$COLLECTOR" \
    validate --config=/etc/otelcol/config.yaml
echo "collector config: valid"

generated=$(mktemp)
trap 'rm -f "$generated"; rm -rf "$dummy"' EXIT
python3 scripts/gen-grafana-dashboard.py "$generated"
if ! cmp -s "$generated" deploy/grafana/dashboards/driftline.json; then
    echo "check-obs-config: deploy/grafana/dashboards/driftline.json differs from what the generator writes;" \
        "run python3 scripts/gen-grafana-dashboard.py" >&2
    exit 1
fi
echo "grafana dashboard: matches its generator"

#!/usr/bin/env bash
# Static checks of the Kubernetes manifests (DESIGN §14, §15.3, D80), run by CI's `k8s-manifests` job; needs kubectl
# (with kustomize) and Docker.
#   1. the base and the kind overlay render, and kubeconform validates every resource against Kubernetes 1.37's
#      strict schemas (fetched first, with retries);
#   2. every Driftline and sandbox pod runs as non-root with a read-only root filesystem, no privilege escalation, no
#      capabilities, the RuntimeDefault seccomp profile, no service-account token and a memory limit; every role's
#      Deployment has startup, liveness and readiness probes;
#   3. a default-deny ingress policy exists, and the base commits no Secret.
# The kind overlay's Secrets are generated from files in .dev/; when they are missing, dummies are written for the
# render and removed again.
set -euo pipefail
cd "$(dirname "$0")/.."

KUBECONFORM=ghcr.io/yannh/kubeconform:v0.8.0@sha256:faffaf43f95aa6425306e1ab8d6fcad72acb9049158f38e574c085ea1ec0f64e
OVERLAY=deploy/k8s/overlays/kind
work=$(mktemp -d)
created_dev=0
cleanup() {
    rm -rf "$work"
    if [ "$created_dev" = 1 ]; then
        rm -rf "$OVERLAY/.dev"
    fi
}
trap cleanup EXIT
if [ ! -d "$OVERLAY/.dev" ]; then
    created_dev=1
    mkdir -p "$OVERLAY/.dev"
    mkdir -p "$OVERLAY/.dev/secrets/app" "$OVERLAY/.dev/secrets/migrate" "$OVERLAY/.dev/secrets/postgres"
    for f in spring.datasource.password driftline.secrets.keys.k1 driftline.pii.keys.p1 driftline.pii.index-key \
            driftline.dispatcher.service-token; do
        printf 'dummy' > "$OVERLAY/.dev/secrets/app/$f"
    done
    printf 'dummy' > "$OVERLAY/.dev/secrets/migrate/spring.datasource.username"
    printf 'dummy' > "$OVERLAY/.dev/secrets/migrate/spring.datasource.password"
    printf 'dummy' > "$OVERLAY/.dev/secrets/postgres/postgres-password"
    printf 'dummy' > "$OVERLAY/.dev/secrets/postgres/app-password"
    printf 'dummy' > "$OVERLAY/.dev/secrets/app/driftline.kafka.password"
    mkdir -p "$OVERLAY/.dev/secrets/tls-app" "$OVERLAY/.dev/secrets/tls-client" "$OVERLAY/.dev/secrets/tls-sandbox" \
        "$OVERLAY/.dev/secrets/kafka"
    for f in postgres/server.crt postgres/server.key tls-app/ca.crt tls-app/server.crt tls-app/server.key \
            tls-app/truststore.p12 tls-client/ca.crt tls-client/truststore.p12 tls-sandbox/server.p12 \
            tls-sandbox/keystore-password tls-sandbox/truststore.p12 kafka/ca.crt kafka/kafka.p12 \
            kafka/keystore-password kafka/key-password kafka/jaas.conf kafka/client.properties; do
        printf 'dummy' > "$OVERLAY/.dev/secrets/$f"
    done
    echo '{}' > "$OVERLAY/.dev/bootstrap.json"
fi
kubectl kustomize deploy/k8s/base > "$work/base.yaml"
kubectl kustomize --load-restrictor LoadRestrictionsNone "$OVERLAY" > "$work/kind.yaml"
# kubeconform's schemas for exactly the kinds rendered, fetched with retries (a dropped connection must not fail the
# check), then validated offline. File names follow kubeconform's {kind}{-group}-{version} convention.
mkdir -p "$work/schemas"
python3 -I - "$work/base.yaml" "$work/kind.yaml" > "$work/schema-names" <<'PY'
import sys
import yaml
names = set()
for path in sys.argv[1:]:
    for d in yaml.safe_load_all(open(path, encoding="utf-8")):
        if d:
            group, _, version = d["apiVersion"].rpartition("/")
            suffix = f"-{group.split('.')[0]}" if group else ""
            names.add(f"{d['kind'].lower()}{suffix}-{version}")
print("\n".join(sorted(names)))
PY
while read -r name; do
    curl -fsS --retry 8 --retry-all-errors --retry-delay 2 -o "$work/schemas/$name.json" \
        "https://raw.githubusercontent.com/yannh/kubernetes-json-schema/master/v1.37.0-standalone-strict/$name.json"
done < "$work/schema-names"
for f in base kind; do
    docker run --rm -v "$work:/m:ro" "$KUBECONFORM" -strict -summary \
        -schema-location '/m/schemas/{{.ResourceKind}}{{.KindSuffix}}.json' "/m/$f.yaml"
done
python3 -I - "$work/base.yaml" "$work/kind.yaml" <<'PY'
import sys
import yaml

problems = []
def check(name, ok, what):
    if not ok:
        problems.append(f"{name}: {what}")

for path in sys.argv[1:]:
    docs = [d for d in yaml.safe_load_all(open(path, encoding="utf-8")) if d]
    tree = "base" if path.endswith("base.yaml") else "kind"
    if tree == "base":
        check("base", not any(d["kind"] == "Secret" for d in docs), "commits a Secret")
    check(tree, any(d["kind"] == "NetworkPolicy" and d["spec"].get("podSelector") == {}
                    and d["spec"].get("policyTypes") == ["Ingress"] and not d["spec"].get("ingress") for d in docs),
          "has no default-deny ingress policy")
    for d in docs:
        if d["kind"] not in ("Deployment", "Job", "StatefulSet"):
            continue
        app = d["metadata"].get("labels", {}).get("app.kubernetes.io/name")
        if app not in ("driftline", "sandbox"):
            continue   # the kind overlay's dev-only PostgreSQL, Kafka and topic job run their images' own way
        name = f"{tree} {d['kind']} {d['metadata']['name']}"
        pod = d["spec"]["template"]["spec"]
        psc = pod.get("securityContext", {})
        check(name, psc.get("runAsNonRoot") is True and psc.get("runAsUser", 0) > 0, "not non-root with a numeric user")
        check(name, psc.get("seccompProfile", {}).get("type") == "RuntimeDefault", "no RuntimeDefault seccomp")
        check(name, pod.get("automountServiceAccountToken") is False, "mounts a service-account token")
        for c in pod["containers"]:
            sc = c.get("securityContext", {})
            check(name, sc.get("readOnlyRootFilesystem") is True, "root filesystem not read-only")
            check(name, sc.get("allowPrivilegeEscalation") is False, "privilege escalation allowed")
            check(name, sc.get("capabilities", {}).get("drop") == ["ALL"], "capabilities not dropped")
            check(name, "memory" in c.get("resources", {}).get("limits", {}), "no memory limit")
            check(name, any(m["mountPath"] == "/tmp" for m in c.get("volumeMounts", [])), "no writable /tmp")
            if d["kind"] == "Deployment" and app == "driftline":
                for probe in ("startupProbe", "livenessProbe", "readinessProbe"):
                    check(name, probe in c, f"no {probe}")
if problems:
    print("check-k8s: FAIL:\n  " + "\n  ".join(problems), file=sys.stderr)
    sys.exit(1)
print("check-k8s: every Driftline and sandbox pod is hardened, probed and limited; default deny is in place")
PY

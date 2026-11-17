#!/usr/bin/env bash
# Static checks of the Kubernetes manifests (DESIGN §14, §15.3, D80), run by CI's `k8s-manifests` job; needs kubectl
# (with kustomize) and Docker.
#   1. the base and the kind overlay render, and kubeconform validates every resource against Kubernetes 1.37;
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
    printf 'SPRING_DATASOURCE_PASSWORD=dummy\nDRIFTLINE_SECRETS_KEYS_K1=dummy\nDRIFTLINE_PII_KEYS_P1=dummy\n%s\n%s\n' \
        'DRIFTLINE_PII_INDEX_KEY=dummy' 'DRIFTLINE_DISPATCHER_SERVICE_TOKEN=dummy' > "$OVERLAY/.dev/driftline-secrets.env"
    printf 'SPRING_DATASOURCE_USERNAME=postgres\nSPRING_DATASOURCE_PASSWORD=dummy\n' > "$OVERLAY/.dev/driftline-migrator.env"
    printf 'POSTGRES_PASSWORD=dummy\nDRIFTLINE_DB_PASSWORD=dummy\n' > "$OVERLAY/.dev/driftline-infra.env"
    echo '{}' > "$OVERLAY/.dev/bootstrap.json"
fi
kubectl kustomize deploy/k8s/base > "$work/base.yaml"
kubectl kustomize --load-restrictor LoadRestrictionsNone "$OVERLAY" > "$work/kind.yaml"
for f in base kind; do
    docker run --rm -v "$work:/m:ro" "$KUBECONFORM" -strict -summary -kubernetes-version 1.37.0 "/m/$f.yaml"
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

#!/usr/bin/env bash
# Trivy image scan (DESIGN §14, §16, D79), run nightly by CI; needs Docker.
#
#   scripts/trivy-scan.sh
#
# Builds the Boot jar and the sandbox with Gradle, builds both images, and fails on any high or critical vulnerability
# that has a fixed version, in the base image's packages or in the jars inside (one with no fix yet is reported by
# Trivy's database but cannot be acted on, so it does not fail the run).
set -euo pipefail
cd "$(dirname "$0")/.."

TRIVY=aquasec/trivy:0.76.1@sha256:790fd66d1697395d8693d1acaa1ea702406a20f109b6fc7bac8db623eca6c06c
./gradlew -q :app:bootJar :sandbox:installDist
docker build -q -f deploy/docker/app.Dockerfile -t driftline-app:scan . > /dev/null
docker build -q -f deploy/docker/sandbox.Dockerfile -t driftline-sandbox:scan . > /dev/null
# the vulnerability database is cached in a Docker volume, so nothing root-owned lands in the working tree
status=0
for image in driftline-app:scan driftline-sandbox:scan; do
    echo "== $image"
    docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v driftline-trivy-cache:/root/.cache/trivy \
        "$TRIVY" image --quiet --timeout 20m --scanners vuln --severity HIGH,CRITICAL --ignore-unfixed \
        --exit-code 1 --format table "$image" || status=1
done
if [ "$status" -ne 0 ]; then
    echo "trivy-scan: FAIL: a high or critical vulnerability with a fix is in an image" >&2
    exit 1
fi
echo "trivy-scan: no fixable high or critical vulnerability in either image"

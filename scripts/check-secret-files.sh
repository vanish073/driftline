#!/usr/bin/env bash
# No tracked file is a secret by its name (Shift 11, D92): env files, private keys, key stores, certificates, SSH keys,
# password databases and cloud credential files. gitleaks (CI `security`) looks for secrets inside files; this looks at
# what the files are, so a key store or a binary key file it cannot read is caught too. An example file that holds no
# secret is allowed by name: `.env.example`, `*.example.*`.
#
#   scripts/check-secret-files.sh     exits 1 and lists the files if any tracked file matches
set -euo pipefail
cd "$(dirname "$0")/.."

pattern='(^|/)\.env($|\.)|(^|/)\.dev/|\.(pem|key|crt|cer|der|csr|p8|p12|pfx|jks|keystore|truststore|kdbx|ovpn|asc|gpg)$|(^|/)id_(rsa|dsa|ecdsa|ed25519)(\.pub)?$|(^|/)\.(netrc|pgpass|npmrc|pypirc|htpasswd)$|(^|/)(credentials|service-account|client_secret)[^/]*\.json$|(^|/)\.aws/|(^|/)\.ssh/'
allowed='(^|/)\.env\.example$|\.example\.[a-z]+$|\.example$'

found=$(git ls-files -z | tr '\0' '\n' | grep -E -i "$pattern" | grep -E -i -v "$allowed" || true)
if [ -n "$found" ]; then
    echo "check-secret-files: these tracked files look like secrets; remove them from git (git rm --cached) and rotate:" >&2
    printf '  %s\n' $found >&2
    exit 1
fi
echo "check-secret-files: no tracked file looks like a secret ($(git ls-files | wc -l) files checked)"

# Driftline threat model

A short STRIDE review (DESIGN §14, Shift 9). Each mitigation names the test or CI job that proves it. The last section
lists what is **not** mitigated.

## What is protected

| Asset | Where it lives |
|---|---|
| The ledger and the audit trail (money, and who changed it by hand) | PostgreSQL: `ledger_*`, `audit_log` |
| Refund decisions and the outbox (an action Driftline takes in the world) | PostgreSQL: `breaks`, `outbox` |
| Customers' contact details (UPI VPA, phone, email) | PostgreSQL `payment_customers`: ciphertext and a blind index (D77) |
| Webhook secrets, API tokens, encryption keys | secrets: AES-GCM ciphertext in `merchant_webhook_secrets`; tokens: SHA-256 only; keys: environment or Kubernetes Secret, never the database |
| The event log | Kafka topic `driftline.events.v1` |

## Trust boundaries

1. **Internet → ingest API** (port 8080): the payment gateway's webhooks (HMAC-signed), merchants' order events and
   settlement files (bearer tokens), ops staff (ops tokens), the dispatcher's observations (service token).
2. **Driftline → the outside world**: the dispatcher calls the gateway, the merchant and the ops sink.
3. **Inside the deployment**: Driftline processes, PostgreSQL, Kafka, the management port 9091 (health and metrics,
   never exposed, D21), and in the `obs` profile the collector, Jaeger, Prometheus and Grafana.
4. **The supply chain**: dependencies, base images, GitHub Actions, the OpenTelemetry agent.

## STRIDE

| Threat | Example | Mitigation | Proof |
|---|---|---|---|
| **S**poofing | A forged or replayed gateway webhook | HMAC-SHA256 over timestamp and raw body, verified before parsing, constant-time comparison, 300 s replay window; an unknown merchant gets the same 401 as a bad signature; two active secrets during a rotation (D76) | `WebhookSignatureTest`, `IngestApiTest` (tampered body, wrong secret, stale or future timestamp, unknown merchant), `SecretRotationIT`, the e2e suite's 401 checks |
| **S**poofing | A merchant acting as another merchant | Opaque tokens stored as SHA-256, looked up on every request (revocation is immediate); scopes as `SCOPE_*` authorities; the token's merchant must match `{merchantId}` | `IngestApiTest`/`OpsApiTest` IDOR tests (merchant A on merchant B is 403), missing-scope 403 |
| **T**ampering | Editing the ledger or the audit trail after the fact | The app role has no UPDATE or DELETE on them; append-only triggers stop even the owner; the audit trail is a SHA-256 hash chain that `verify-audit` walks from row 1 (D75) | `LedgerDatabaseGuardsIT`, `AuditTrailIT` (an edit with triggers disabled is found), `VerifyIT`; `driftline verify` in the chaos run |
| **T**ampering | Re-sending an event key with a different body | Dedupe on the key and payload hash; a reused key with another body is quarantined and escalated (IDEMPOTENCY_CONFLICT) | `MatcherTest`, `DetectionTest`, the simulator's invariants |
| **T**ampering | A second, different settlement file for a date | Files are stored by SHA-256; a conflicting file is refused and raised as FILE_CONFLICT | `IngestTablesIT`, `SettlementWorkerIT`, the settlement corpus |
| **T**ampering | Formula injection through the CSV export | A cell starting with `= + - @`, tab or CR gets a leading `'` and is quoted | `OpsApiTest` |
| **R**epudiation | "I never adjusted that ledger / resolved that break / rotated that secret" | Every manual action appends an audit row naming the token, with a required note, in the same transaction as the change (D74, D76) | `ManualAdjustmentTest`, `OpsApiTest`, `WebhookSecretRotationIT` |
| **I**nformation disclosure | Personal data in the database, the event log or logs | Contacts never enter events, Kafka, the core or the simulator; AES-256-GCM per field with `kid` and row-bound associated data; HMAC blind index per merchant; masked in every log line and in the ops API (D77) | `PiiCipherTest`, `PiiMaskingTest`, `CustomerVaultIT` (ciphertext only, all app output clean), `scripts/pii-scan.sh` in every e2e and chaos run (logs, Kafka, every table but the vault) |
| **I**nformation disclosure | Secrets or keys leaking | Webhook secrets encrypted with a key outside the database; configuration `toString` hides keys; gitleaks over the whole git history in CI; generated secrets for local runs live in a git-ignored directory | `SecretCipherTest`, CI `security` (gitleaks; `.gitleaksignore` lists each accepted historic finding and why) |
| **I**nformation disclosure | Learning which breaks or merchants exist | Another merchant's break is 404, not 403; an unknown merchant's webhook looks like a bad signature; error messages name fields, never values | `OpsApiTest`, `IngestApiTest` |
| **D**enial of service | Huge or deeply nested bodies | Webhooks and order events capped at 64 KiB (413 before parsing), settlement files at 16 MiB; strict JSON: no arrays, depth at most 3, duplicate keys refused | `IngestApiTest` (413), `CodecTest` (arrays, depth, duplicate keys, trailing content) |
| **D**enial of service | A settlement file built to make subset-sum explode | The solver's budget counts operations, never time, and an over-budget line is escalated, not solved (D7) | `SubsetSumProperties`, `BatchMatchingTest`, the settlement corpus |
| **E**levation of privilege | A merchant token adjusting the ledger or rotating secrets | `ledger:adjust` and `secrets:write` exist only on ops tokens; every endpoint checks its scope with `@PreAuthorize` | `OpsApiTest` |
| **E**levation of privilege | A compromised Driftline process altering the schema | Migrations run as `driftline_migrator` in a one-shot role; services connect as the DML role `driftline_app` with column-level grants | `MigrateRoleIT` |
| **E**levation of privilege | Breaking out of a container | Images run as a non-root user; the core has no Spring and no I/O (ArchUnit) | the Dockerfiles; `*ArchitectureTest` |

## Supply chain

| Control | Proof |
|---|---|
| Every resolved dependency pinned in committed Gradle lockfiles, which must match the build files | CI `security`: `scripts/check-lockfiles.sh` |
| No locked dependency with a high, critical or unscored advisory | CI `security`: `scripts/osv-scan.sh` (OSV-Scanner) |
| No pull request adds a dependency with a high or critical advisory | CI `dependency-review` |
| No fixable high or critical vulnerability in the app or sandbox image | nightly `trivy`: `scripts/trivy-scan.sh` |
| Updates proposed weekly, at least 14 days after release | `.github/dependabot.yml` |
| Actions pinned by commit SHA, images by digest, the OpenTelemetry agent by SHA-256 | the workflows, the Compose file, `app.Dockerfile` |

## Residual risks (not mitigated)

- **No TLS inside the deployment.** Kafka and PostgreSQL traffic in Compose is plaintext on a private network, and Kafka
  has no authentication. TLS at the ingress and mTLS between services are out of scope for v1.
- **No rate limiting.** Size limits bound each request, but nothing bounds the request rate; it belongs at the ingress
  or API gateway in front of Driftline.
- **Tokens do not expire.** They can be revoked (`revoked_at`, effective at once), but only through the database; there
  is no token API.
- **Keys come from the environment.** No KMS or HSM; whoever can read the process environment can decrypt the stored
  secrets and contacts.
- **Stored contacts are kept forever.** No retention period or erasure on request (D77).
- **The management port is unauthenticated.** It is reachable only inside the deployment network (D21), as are the `obs`
  profile's UIs, which Compose binds to 127.0.0.1; Grafana allows anonymous viewing there.
- **The dev bootstrap file** creates merchants and tokens at start-up when `driftline.bootstrap-file` is set; it is for
  local and test stacks, not production.
- Known functional weaknesses are listed in DESIGN D73 and the decision notes that supersede parts of it.

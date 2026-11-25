# Driftline threat model

A short STRIDE review (DESIGN §14, Shift 9; hardened in Shift 11, D92–D100). Each mitigation names the test or CI job
that proves it. The last section lists what is **not** mitigated. [`results/security-shift11.md`](results/security-shift11.md)
walks through the usual attacks on a payment API one by one.

## What is protected

| Asset | Where it lives |
|---|---|
| The ledger and the audit trail (money, and who changed it by hand) | PostgreSQL: `ledger_*`, `audit_log` |
| Refund decisions and the outbox (an action Driftline takes in the world) | PostgreSQL: `breaks`, `outbox` |
| Customers' contact details (UPI VPA, phone, email) | PostgreSQL `payment_customers`: ciphertext and a blind index (D77) |
| Webhook secrets, API tokens, encryption keys | secrets: AES-GCM ciphertext in `merchant_webhook_secrets`; tokens: SHA-256 only; keys: environment or Kubernetes Secret, never the database |
| The event log | Kafka topic `driftline.events.v1` |

## Trust boundaries

1. **Internet → ingest API** (port 8080, HTTPS): the payment gateway's webhooks (HMAC-signed), merchants' order events
   and settlement files (bearer tokens), ops staff (ops tokens), the dispatcher's observations (service token).
2. **Driftline → the outside world**: the dispatcher calls the gateway, the merchant and the ops sink, over HTTPS.
3. **Inside the deployment**, every link over TLS (D100): Driftline processes, PostgreSQL (TLS only, certificate
   checked), Kafka (TLS with logins), the management port 9091 (HTTPS; health open for probes, metrics behind a token,
   never exposed, D21, D98), and in the dev-only `obs` profile the collector, Jaeger, Prometheus and Grafana.
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
| **S**poofing | Guessing a token or a webhook signature | Tokens are 256-bit random values; 20 failed authentications from one address in a minute lock it out for 5 minutes, before any lookup (D96) | `RateLimiterTest`, `RateLimitApiTest` |
| **S**poofing | A token that leaked long ago | Every token expires (90 days for ops and service tokens, 365 for merchants'); revocation through the API takes effect at once (D95) | `TokenApiTest`, `TokenLifecycleIT` |
| **S**poofing | Someone on the network impersonating the database, Kafka or Driftline | TLS on every link with the certificate and host name checked; Kafka logins; a production start refuses a plaintext link (D100) | `TransportGuardTest`, `KafkaSecurityTest`, the Compose e2e and chaos runs and the kind smoke test (all with TLS) |
| **T**ampering | Reading or altering traffic inside the deployment | TLS on PostgreSQL (`hostssl` only), Kafka (`SASL_SSL`), the API, the management port, the dispatcher's calls and the trace export (D100) | as above |
| **T**ampering | An absurd amount that overflows the matcher's arithmetic and halts a partition | Amounts above ₹1,000 crore (payments) or ₹1 lakh crore (settlement fields) are refused wherever they enter (D93) | `AmountLimitsTest`, `AttackSurfaceTest`, the settlement corpus |
| **T**ampering | Replaying a manual action ("redeem it 100 times") | Every manual POST needs an `Idempotency-Key`; a repeat gets the first answer and changes nothing; a unique index stops concurrent repeats (D94) | `ManualAdjustmentTest`, `ManualResolutionTest`, `OpsApiTest`, `WebhookSecretRotationIT`, the store contract |
| **E**levation of privilege | A token creating a stronger one ("admin at signup") | No signup; a token grants only scopes it holds; merchant tokens get merchant scopes only; unknown fields such as `role` are refused (D95) | `TokenApiTest` |
| **I**nformation disclosure | A browser on another site reading the API, or markup in a key | No CORS grant; `nosniff`, `frame-ancestors 'none'`, `default-src 'none'`, `no-store`, `no-referrer`; outside keys limited to `[A-Za-z0-9._:-]` (D92) | `AttackSurfaceTest`, `IdsTest` |
| **I**nformation disclosure | Keys and passwords in environment variables, or secrets committed to git | Secrets are mounted files; a production start refuses them in its environment (D97); CI fails if a tracked file is named like a secret | `SecretSourceGuardTest`, CI `security` (`scripts/check-secret-files.sh`) |
| **I**nformation disclosure | Contacts kept longer than needed | Deleted after the retention period (180 days) and on request, with tombstones (D99) | `CustomerErasureIT`, `OpsApiTest` |
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
| Version updates are deliberate after v1.0 (D91): Dependabot opens no version PRs, because the results name the versions they were measured with; its alerts, dependency-review, OSV-Scanner and Trivy still report vulnerabilities | `.github/dependabot.yml` |
| No tracked file is a secret by its name (env files, keys, key stores) | CI `security`: `scripts/check-secret-files.sh` |
| Actions pinned by commit SHA, images by digest, the OpenTelemetry agent by SHA-256 | the workflows, the Compose file, `app.Dockerfile` |

## Residual risks (not mitigated)

Shift 11 closed the residual risks v1.0 listed: TLS and Kafka authentication inside the deployment (D100), rate
limiting (D96), token expiry and a token API (D95), an unauthenticated management port and Grafana's anonymous viewer
(D98), contacts kept forever (D99), the dev bootstrap file in production (D97). What remains:

- **No KMS or HSM.** Keys are files mounted from the deployment's secret store, never environment variables (D97), but
  whoever can read those files or the process's memory can decrypt the stored secrets and contacts.
- **Services authenticate each other with tokens and passwords, not client certificates.** TLS checks the server's
  certificate on every link (D100); the client proves itself with a token or a password inside it. Kafka checks logins
  but has no ACLs: any client with a login may read or write any topic, and only Driftline and the admin have one.
- **Rate limits are per process, in memory.** With N instances a client gets up to N times the rate, and behind a proxy
  the per-address lockout sees the proxy's address (D96); a deployment behind one relies on the proxy for addresses.
- **The dev-only `obs` profile's own links are plaintext**: collector to Jaeger, Grafana to Prometheus and Jaeger, and
  the browser to Grafana and Prometheus, on the Compose network and 127.0.0.1, each behind a login (D98). Driftline's
  links to them (the metrics scrape, the trace export) use TLS. The Kubernetes deployment has no `obs` stack.
- **Health endpoints are open** on the management port, for the orchestrator's probes; they show only UP or DOWN.
- **Erasure reaches the database, not its backups**, and the first contact seen for a payment still wins (D77, D99).
- Known functional weaknesses are listed in DESIGN D73 and the decision notes that supersede parts of it.

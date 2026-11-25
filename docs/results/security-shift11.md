# Security, attack by attack (Shift 11)

The usual ways a payment backend is attacked, each tried against Driftline. Every answer names the test or CI job that
proves it; the design notes are DESIGN D92–D100. What is still not covered is the last section of
[`docs/THREAT_MODEL.md`](../THREAT_MODEL.md).

| What someone tries | What happens | Proof |
|---|---|---|
| Opens `/admin`, `/actuator/env`, `/.env`, `/.git/config`, `/swagger-ui.html`, or a path with `..` | There is no such page. Without a token: 401 (or 400 from the firewall for `..` and encoded slashes). With an ops token holding every scope: 404. Actuator lives only on the management port, never published, and its metrics need Prometheus's own token | `AttackSurfaceTest`, `ApplicationSmokeIT` |
| Changes the merchant or break id in the URL | A merchant token works only on its own merchant: another merchant's path is 403, another merchant's break 404, so its existence does not leak | `IngestApiTest` (IDOR), `OpsApiTest` |
| Puts an API key in the browser: the URL, a cookie, another header | Only `Authorization: Bearer` authenticates; a token in the query string, a cookie, `X-Api-Key` or another scheme is 401. No CORS grant, so another site's page cannot call the API with a visitor's browser; answers are `no-store`, `nosniff`, not frameable | `AttackSurfaceTest` |
| Edits the price | A payment's amount arrives inside the gateway's HMAC-signed webhook; a changed body fails the signature (401). A merchant's order amount is its own claim: one that differs from the gateway's is a break, not a payment | `IngestApiTest`, the simulator's invariants |
| Orders −1 items, or a zero, fractional or absurd amount | Amounts must be positive whole paise, at most ₹1,000 crore; anything else is 400. Before Shift 11 an amount near the 64-bit limit could overflow the matcher's arithmetic and halt reconciliation (D93) | `AttackSurfaceTest`, `AmountLimitsTest` |
| Fakes a payment | Webhooks need an HMAC-SHA256 signature with the merchant's secret over the timestamp and body; an unknown merchant looks exactly like a bad signature; a timestamp older than 5 minutes is refused | `IngestApiTest`, the e2e suite |
| Redeems it 100 times | A replayed webhook is deduplicated downstream and changes nothing. Every manual action needs an `Idempotency-Key`: 100 repeats post one ledger correction and get the first answer back (D94) | `ManualAdjustmentTest`, `OpsApiTest`, the e2e suite |
| Makes themselves admin at signup | There is no signup. Tokens come only from a token with `tokens:write`, which can grant only scopes it holds; a merchant token cannot use the API; fields like `"role":"admin"` are refused (D95) | `TokenApiTest` |
| Guesses the login code | Tokens are 256-bit random values. After 20 failed attempts in a minute an address is locked out for 5 minutes, before any lookup (D96) | `RateLimiterTest`, `RateLimitApiTest` |
| Prompt-injects the AI | Driftline has no AI model: nothing reads text as instructions. Free text (notes, narrations) is stored and shown as data | — |
| Puts a script in a username | Identifiers allow only `[A-Za-z0-9._-]`, outside keys `[A-Za-z0-9._:-]`; script, SQL and header-splitting text is refused and never echoed. Answers are JSON with `nosniff` and `Content-Security-Policy: default-src 'none'`; the CSV export defuses spreadsheet formulas | `AttackSurfaceTest`, `IdsTest`, `OpsApiTest` |
| Uploads a non-photo in the photo part | The only upload is the settlement file, strictly parsed: a PNG, JPEG, ZIP, executable, HTML page, formula or empty body is 400 and nothing is stored; at most 16 MiB | `AttackSurfaceTest`, `SettlementFileParserTest` |
| Finds `.env` in git | Nothing secret is committed: `.env*`, keys, key stores and certificates are ignored, gitleaks scans the whole history, and CI fails if a tracked file is named like a secret. Secrets are files mounted at run time, never environment variables in production (D97) | CI `security` (gitleaks, `check-secret-files.sh`), `SecretSourceGuardTest` |
| Queries the database | Every query binds its values. The database is reachable only inside the deployment, over TLS only, with the certificate checked; the application's login has no DDL and cannot edit the ledger or the audit trail (D100) | `LedgerDatabaseGuardsIT`, `MigrateRoleIT`, `TransportGuardTest` |
| Sniffs the network inside the deployment | PostgreSQL, Kafka (with logins), the API, the management port, the dispatcher's calls and the trace export all use TLS (D100) | the Compose e2e and chaos runs and the kind smoke test, with TLS |

# DESIGN §14 security checklist, end of Shift 9

Each row of DESIGN §14 with what proves it. Every test and job named here exists and passed on the Shift 9 code;
the CI run that confirms it is listed at the end.

| §14 item | Proof (tests) | Proof (scripts and CI jobs) |
|---|---|---|
| Signed webhooks with a replay window | `WebhookSignatureTest`; `IngestApiTest.aTamperedBodyAWrongSecretAndAMissingSignatureAre401AndNothingIsPublished`, `.aStaleOrFutureTimestampIsRejected`; `SecretRotationIT` (rotated secret, D76) | e2e `ComposeE2eTest.aStaleTimestampIsRejected` (CI `e2e-compose`, `kind-smoke`) |
| Token scopes per merchant | `IngestApiTest.aMerchantATokenOnMerchantBIs403TheIdorTest`, `.aTokenWithoutTheScopeIs403`; `OpsApiTest.anotherMerchantsBreakIs404NotForbiddenSoItsExistenceDoesNotLeak` | CI `build` |
| Encrypted personal fields | `PiiCipherTest` (round trip, key rotation, row binding, blind index); `CustomerVaultIT.aContactIsStoredEncryptedFoundByVpaShownMaskedAndNeverLogged` (the columns hold only ciphertext) | CI `build`, `integration` |
| Masked logs | `PiiMaskingTest.theRealLogbackConfigurationMasksMessagesAndStackTraces`; `CustomerVaultIT` (all captured app output) | `scripts/pii-scan.sh` after every e2e and chaos run (logs, the Kafka topic, every table but the vault); the pod-log check in `scripts/kind-smoke.sh` (CI `e2e-compose`, `chaos-compose`, `kind-smoke`) |
| Append-only audit trail | `AuditChainTest`; `AuditTrailIT` (UPDATE and DELETE refused, an edit with triggers off is found by `verify-audit`); `VerifyIT`; `LedgerDatabaseGuardsIT` | `driftline verify` "audit chain intact" (CI `chaos-compose`, `kind-smoke`) |
| Dependency scanning in CI | — | CI `security`: `scripts/check-lockfiles.sh`, `scripts/osv-scan.sh` (fails on CVSS ≥ 7.0 or unscored); CI `dependency-review` (pull requests, high or critical); nightly `trivy` (`scripts/trivy-scan.sh`); `.github/dependabot.yml` |
| Extras: CSV export escaping | `OpsApiTest.csvFieldsAreQuotedOnlyWhenTheyMustBe` | CI `build` |
| Extras: DB least privilege | `MigrateRoleIT` (migrations as the owner, services as the DML role); `LedgerDatabaseGuardsIT`; `WebhookSecretRotationIT` (column-level grant: a secret can be superseded, never rewritten) | CI `integration` |
| Extras: non-root, read-only-root containers | — | `scripts/check-k8s.sh` (every Driftline and sandbox pod; CI `k8s-manifests`); Compose runs the same images read-only as uid 10001 (CI `e2e-compose`, `chaos-compose`); `scripts/kind-smoke.sh` (CI `kind-smoke`) |
| Extras: NetworkPolicy | — | `scripts/check-k8s.sh` (a default deny exists); `scripts/kind-smoke.sh` probes from an unlabelled pod: the API is reachable, PostgreSQL and Kafka are not (CI `kind-smoke`) |
| Extras: request size limits | `IngestApiTest.aBodyOverTheLimitIs413BeforeAnyParsing`; `CodecTest.strictJsonRefusesDepthBeyondThreeArraysAndDuplicateKeys` | CI `build` |
| Extras: secret scanning | — | CI `security`: gitleaks over the whole git history (`.gitleaksignore` lists each accepted finding and why) |

The threats these answer, and what is not mitigated, are in `docs/THREAT_MODEL.md`.

## CI confirmation

Filled in at the end of Shift 9 from the pull request's run on its final commit.

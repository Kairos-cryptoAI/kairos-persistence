# Durable Text processing fences

`NewsProcessingRepository` reserves canonical event hashes before Text can call
an extractor/provider. Concurrent batches return only newly inserted hashes.
Claims commit before the caller proceeds; no provider or publication call holds
a database transaction. Completion locks and validates the exact scope, event
and attempt identity, and only confirms after all signals were durably accepted.

An interrupted claim remains `CLAIMED` permanently. It is not automatically
expired, refunded, deleted, reassigned or retried. This deliberately trades
possible missing news for avoiding repeated paid/unknown-outcome operations.
It is not exactly-once delivery at an external provider. Existing source-spend
reservations remain the separate authority for historical and reserved costs.

Migration `028_news_processing.sql` is opt-in **controlled-runtime only**.
Runtime, SIM and research-campaign migration manifests are unchanged. An
existing controlled-runtime deployment must remain stopped/refuse startup until
an independently backed-up, reviewed schema transition is accepted. Importing
the repository never connects, grants privileges or migrates a database.
The scoped runtime role needs SELECT, INSERT and UPDATE on this table, not
ownership, DELETE, schema modification or an operator role. The database trigger
prevents deletion, identity reassignment and reopening completed claims.

Text's stable processing-scope hash must bind its reviewed deployment, service
and feed/account configuration. A fresh random scope is not a recovery option:
it would create a new namespace rather than preserving prior no-resend evidence.
Only event identities and timestamps are stored; no news body, credential or
provider response is placed in this table. Retention never deletes uncertainty
merely to permit another call.

The offline suite covers input/identity checks and unconfirmed outcomes. The
explicit disposable PostgreSQL test covers competing claimants, process
reconstruction, same-owner completion, foreign-owner rejection and immutable
interrupted claims. These are engineering proofs, not provider quality, venue
qualification, trading readiness or a completed live deployment.

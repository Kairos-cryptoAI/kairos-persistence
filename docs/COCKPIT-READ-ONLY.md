# Read-only Decision Cockpit API

The persistence package now contains a GET-only snapshot API at
`/api/v1/cockpit/snapshot`. It projects closed bars, persisted market
indicators, venue-quality receipts, risk decisions, and execution lifecycle
events from the runtime audit journal. Missing evidence stays empty or
`UNAVAILABLE`; the endpoint does not fabricate market state or TCA.

## Security boundary

The API is not an authentication provider and is not a public website. Put it
behind a private authenticated reverse proxy/VPN. The proxy must authenticate
the user, remove every client-supplied `X-Kairos-*` header, and then inject the
three signed headers `X-Kairos-Authenticated-User`, `X-Kairos-Auth-Timestamp`,
and `X-Kairos-Auth-Signature`. The HMAC-SHA256 signature is bound to the
principal, fixed GET method, fixed route, and a 30-second timestamp window.
Keep the shared signing key only in a mounted secret file available to the
proxy and API; never expose it to the browser. There is no CORS configuration,
no mutation route, no query filtering, and no anonymous fallback.

Configure `KAIROS_COCKPIT_PROXY_SIGNING_KEY_FILE` and
`KAIROS_COCKPIT_DATABASE_URL_FILE` with absolute paths to protected secret
files. On POSIX, files with group or world permissions are rejected. The
database URL must belong to a dedicated runtime reader role, not an application
writer credential. The API sets PostgreSQL sessions read-only, checks the
exact runtime migration profile, and fails startup unless the current role is
non-privileged, has no role memberships, can read the audit table, and has no
table mutation or schema/database creation privileges.

## Operational status and non-goals

The API always projects `DRY_RUN`, `REJECT_ALL`, degraded runtime, and all
readiness flags false. This is observation only, not evidence of PAPER, alpha,
or LIVE qualification. No database role has been provisioned and no Compose
service or external ingress is wired by this package change. Do not point it
at the primary runtime database until a fresh recovery receipt is accepted and
a dedicated least-privilege account is provisioned through the approved
operations process. Do not start it with production secrets or expose its
port as part of this code change.

Run locally only against a disposable runtime-profile database with mounted
secret files and a private test ingress. The package entry point is
`kairos-cockpit-api`; its default bind address is loopback.

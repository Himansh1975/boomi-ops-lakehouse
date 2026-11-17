# Boomi Platform API: what the ingester relies on

Checked against developer.boomi.com on 2026-11-17. Re-check before trusting in live mode.

| Topic | What the docs say | Source |
|---|---|---|
| Base URL | `https://api.boomi.com/api/rest/v1/{accountId}` (Europe: `api.platform.gb.boomi.com`) | [Auth](https://developer.boomi.com/docs/APIs/PlatformAPI/Introduction/Platform_API_and_Partner_API_authentication) |
| Auth | HTTP Basic. With an API token the user is `BOOMI_TOKEN.<username>` and the password is the token | same |
| JSON | Send `Content-Type: application/json`, `Accept: application/json` (default is XML) | same |
| Query | `POST /ExecutionRecord/query` with `{"QueryFilter": {"expression": ...}}`. `BETWEEN` takes 2 arguments | [ExecutionRecord](https://developer.boomi.com/docs/api/platformapi/ExecutionRecord) |
| Paging | Max 100 results per response. `queryToken` appears only when there are more. `POST /<Object>/queryMore` with **only the token as a text/plain body**. Tokens expire, so always use the newest one | [Query paging](https://developer.boomi.com/docs/APIs/PlatformAPI/Introduction/Query_paging) |
| Errors | 503 = server busy **or rate limit exceeded**; 504 = runtime unreachable. Both are retryable with growing delays. No 429 is documented | [API errors](https://developer.boomi.com/docs/APIs/PlatformAPI/Introduction/API_errors) |
| Status values | ABORTED, ABORTED_RECOVERING, COMPLETE, COMPLETE_WARN, DISCARDED, ENQUEUED, ERROR, INPROCESS, STARTED | ExecutionRecord schema |
| executionType | exec_manual, exec_sched, exec_listener, exec_listener_bridge, exec_remote, retry_manual, retry_sched, fiber, ... | same |
| Number quirk | The schema says `executionDuration` is int64, but the query example returns `["Long", 3832]`. The same goes for `inboundDocumentSize` / `outboundDocumentSize`. **Parse both forms.** | same |
| `executionTime` | Start time of the run. `recordedDate` = end time | same |
| Logs | `POST /ProcessLog {"executionId": ..., "logLevel": "ALL"}` returns `LogDownload {url, statusCode: 202}`; then GET the url (a zip). 504 means the runtime is offline. Logs are purged after 30 days | [ProcessLog](https://developer.boomi.com/docs/api/platformapi/ProcessLog) |
| Deployments | `POST /DeployedPackage/query`. Fields: deploymentId, componentId, componentName, componentType, componentVersion, packageVersion, version, deployedDate, deployedBy, environmentId, active | [DeployedPackage](https://developer.boomi.com/docs/api/platformapi/DeployedPackage) |
| Audit log | `POST /AuditLog/query`, filterable on date, type, action, userId, ... Kept 12 months | [AuditLog](https://developer.boomi.com/docs/api/platformapi/AuditLog) |

## Seen on a live trial account (2026-11-17)
- `executionDuration` really is `["Long", 5735]`, not a plain number.
- A running job shows `status: "STARTED"` (not INPROCESS). It flips to `COMPLETE` about a minute later, which is why the 2h lookback matters.
- Building components via `POST /Component` (XML), then `POST /PackagedComponent`, `POST /DeployedPackage`, `POST /ProcessSchedules/{id}` and `POST /ExecutionRequest`, all work on a trial account. See `scripts/setup_demo.py`.

## Not confirmed yet
- The numeric rate limit for a trial account. One doc example mentions "five requests per second", so the client defaults to at most 4 requests per second.
- Whether queries can sort by `executionTime` and page at the same time without gaps. The ingester doesn't depend on order; it MERGEs on `executionId`.

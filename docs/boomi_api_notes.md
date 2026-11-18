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

## Lineage: ComponentReference and ComponentMetadata (live trial, 2026-11-18)
Fixtures from these calls are in `tests/fixtures/lineage/` (the username is replaced with `demo.user@example.com`).

- `ComponentMetadata/query` with `currentVersion=true` and `deleted=false` returned all 14 components in one page (no `queryToken`). On this account `currentVersion` and `deleted` came back as JSON booleans. The docs show strings too, so still parse both.
- Connections have `subType` (`http`, `disk-sdk`); processes have an empty `subType`.
- **Downstream** (`parentComponentId` + `parentVersion`) returns **both the connection and the operation** of every connector shape, all `DEPENDENT`. Example: Payroll Export v1 → Partner APIs (httpbin) + GET payroll ack.
- **Where used** (`componentId`) works as documented: Partner APIs (httpbin) ← Benefits API Push, Payroll Export, Vendor Invoice Poller.
- A query with no matches returns **HTTP 200** with `{"result": [], "numberOfResults": 0}`.
- `GET /ComponentReference/{id}` is a where-used lookup (it lists parents, not children). It returns **HTTP 400 `"No results found with given input"`** both for a component with no parents **and** for an ID that doesn't exist. The two cases can't be told apart, so the ingester uses only the query endpoint.
- **Old versions have no references.** HR Employee Sync is at v3; querying `parentVersion` 1 or 2 returns an empty result. Lineage history therefore exists only if we snapshot it ourselves on every crawl (SCD2 in `silver_component_edges`).
- **API vs XML:** parsing `connectionId` / `operationId` on `connectoraction` shapes in the process XML gives exactly the same 16 edges as the API (0 missing either way), after skipping the root `<bns:Component componentId=...>` self-reference.
- `createdBy` / `modifiedBy` hold the account user's email in both the metadata and the XML. Redact them before anything is published (viewer, GitHub Pages, fixtures).

### Maps, profiles and sub-processes (added with `scripts/setup_lineage_demo.py`, 2026-11-18)
- A process's downstream query lists its **map** and its **sub-process** (Process Call), but **not** the map's profiles or the sub-process's connections. One level only, as documented, so the crawler must also query maps and sub-processes.
- A map's downstream query returns its `fromProfile` and `toProfile`.
- Sub-processes come back as `DEPENDENT` (packaged with the parent), the same as connections. No `INDEPENDENT` reference has shown up yet.
- An unused profile has an empty where-used result, which is how orphans can be found.
- API vs XML across all 21 components: **28 links from each, 0 differences**. The XML check counts every GUID attribute except the component's own ID (`connectionId`, `operationId`, `mapId`, `processId`, `fromProfile`, `toProfile`).
- The parent with map + sub-process ran COMPLETE on the first try (1 document in, 11 out).

## Not confirmed yet
- When `INDEPENDENT` references appear (maybe for process routes or shared web server listeners).
- The numeric rate limit for a trial account. One doc example mentions "five requests per second", so the client defaults to at most 4 requests per second.
- Whether queries can sort by `executionTime` and page at the same time without gaps. The ingester doesn't depend on order; it MERGEs on `executionId`.

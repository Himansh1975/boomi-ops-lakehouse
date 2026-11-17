# Setup: Boomi trial, local runtime, API token, Databricks

About 1 to 2 hours in total. Replay mode needs none of this. It's only for running against a real account.

Checked on 2026-11-17. Steps marked *(unverified)* come from older docs or from memory, so menu names may differ a little. Follow what the screen says.

## 1. Boomi trial account (10 min)

1. Sign up at <https://boomi.com/form/trial/>. The trial is **30 days**, so start it when you can spend the next few weeks on this.
2. Use a **personal** email, not your work one. *(Boomi doesn't say whether a work email is required. If the form rejects a Gmail address, that's why.)*
3. Activate the account from the email link and log in.

Trial limits Boomi lists: 3 standard connectors, 3 test connectors, limited deployment and cloud options. Six demo processes fit if they reuse the same few connections (HTTP Client, Disk/SFTP).

## 2. Local runtime in Docker (15 min)

The demo processes need a runtime. Use Boomi's official image `boomi/atom` ([Docker Hub](https://hub.docker.com/r/boomi/atom)).

1. In Boomi: **Integration → Manage → Runtime Management → New → Runtime** (basic runtime), and **generate an installer token**. *(Menu path unverified: older docs call it "Atom Management".)* The token expires within 30 min to 24 h, so do step 3 right away.
2. Create `runtime/.env` containing one line:
   ```
   INSTALL_TOKEN=<the token>
   ```
3. Start it:
   ```bash
   docker compose -f runtime/compose.yml up -d
   docker logs -f sim-runtime-01
   ```
4. Within a few minutes the runtime `sim-runtime-01` appears in Runtime Management (probably under *Unattached*). Attach it to an environment, for example "Test".

`runtime/.env` and `runtime/data/` are git-ignored.

## 3. Platform API token (5 min)

1. **Settings → Account Information and Setup → Platform API Tokens → Add token.** (This path is from [Boomi's API auth docs](https://developer.boomi.com/docs/APIs/PlatformAPI/Introduction/Platform_API_and_Partner_API_authentication).) Copy the token now, because it isn't shown again.
2. Find your **account ID** in Settings → Account Information. It looks like `yourname-AB1CD2`.
3. Copy `.env.example` to `.env` in the repo root and fill in:
   ```
   BOOMI_ACCOUNT_ID=<account id>
   BOOMI_USERNAME=<your Boomi login email>
   BOOMI_API_TOKEN=<token>
   ```
4. Test it (read-only, two API calls):
   ```bash
   docker compose run --rm cli "python -m boomi_ops.run check"
   ```

| Result | Meaning |
|---|---|
| `ExecutionRecord: 0 in the last 168h` + `OK` | Token works. There are no runs yet, which is expected before step 4 |
| HTTP 401 | Wrong token, or the username isn't the exact login email |
| HTTP 403 | Your role lacks the **API** privilege, or the trial has no Platform API access. Check Settings → User Management → your role. If it still fails, ask Boomi support whether trials include the Platform API |

## 4. A first demo process (30 min)

Start with process 1 from the plan, the happy path, so `check` has something to show:

1. Build: **Start (No Data)** → **HTTP Client** GET to a public test API (for example `https://jsonplaceholder.typicode.com/users`) → **Disk** connector write → **Stop**.
2. Deploy it to the environment where `sim-runtime-01` is attached, and schedule it every 15 minutes.
3. After about 30 minutes, run `check` again. You should see `ExecutionRecord: 2` or more, with status `COMPLETE`. Also look at what the `duration field looks like:` line prints: `["Long", n]` or a plain number. Either form is already handled.

Add the failing processes (401, bad data, timeout, upstream down, bad deploy) after this works. Use only public test endpoints and fake data.

## 5. Databricks Free Edition (10 min)

1. Sign up at <https://www.databricks.com/signup/free-edition> (email, Google or Microsoft login).
2. Know the limits ([docs](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)): serverless only, one 2X-Small SQL warehouse, max 5 concurrent job tasks, daily compute quota, and **outbound internet only to a limited set of trusted domains**.
3. Because of that last limit, assume Databricks **cannot call the Boomi API**. The plan becomes: ingestion runs on a schedule in GitHub Actions (or on your laptop), uploads the landing files to Databricks, and Databricks runs bronze → silver → gold. That's week 3, and the code doesn't need to change for it.

## Week-0 checklist
- [ ] Trial active (note the end date: start + 30 days)
- [ ] `sim-runtime-01` online in Runtime Management
- [ ] `check` prints `OK`
- [ ] Demo process 1 runs every 15 min and shows up in `check`
- [ ] Databricks Free Edition workspace opens

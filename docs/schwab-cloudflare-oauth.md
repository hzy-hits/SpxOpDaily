# Schwab OAuth through Cloudflare Tunnel

## Decision

Use a dedicated public callback hostname and keep all Schwab credentials and
tokens on Oracle:

```text
Schwab
  -> https://schwab-auth.zh3nyu.com/oauth/callback
  -> Cloudflare Tunnel
  -> 127.0.0.1:8183 callback listener

collectors/verifier
  -> 127.0.0.1:8184 local market-data gateway
  -> one refresh-capable schwab-py Client
  -> Schwab market-data API
```

The callback listener and data gateway run in one process. This matters because
`schwab-py` does not support multiple clients refreshing the same token file.
Only the callback port is routed by Cloudflare; the data gateway remains
localhost-only and only proxies the quote and option-chain endpoints.

## 1. Schwab application

Register this callback URL exactly in the Schwab developer portal:

```text
https://schwab-auth.zh3nyu.com/oauth/callback
```

Case, path, scheme, and trailing slash must match the local configuration.

Changing an approved application's callback may return it to Schwab review.
Treat portal status as a deployment gate: do not run the new OAuth flow or make
Schwab the primary provider until the application is again shown as ready for
use. Keep IBKR and the existing provider path active during that interval.

## 2. Oracle environment

Put these values in the ignored `/home/ubuntu/spx-spark/.env`. Do not paste the
App Secret, callback response, or token into chat or commit them to Git.
`scripts/set-schwab-env.sh KEY` can read each value from standard input without
echoing it, which is the preferred way to transfer the Client ID and Secret.

```dotenv
SCHWAB_APP_KEY=replace-locally
SCHWAB_APP_SECRET=replace-locally
SCHWAB_CALLBACK_URL=https://schwab-auth.zh3nyu.com/oauth/callback
SCHWAB_TOKEN_FILE=/srv/data/spx-spark/runtime/schwab-token.json
SCHWAB_OAUTH_STATE_FILE=/srv/data/spx-spark/runtime/schwab-oauth-state.json
SCHWAB_OAUTH_STATE_TTL_SECONDS=900

SCHWAB_OAUTH_BIND_HOST=127.0.0.1
SCHWAB_OAUTH_BIND_PORT=8183
SCHWAB_GATEWAY_BIND_HOST=127.0.0.1
SCHWAB_GATEWAY_BIND_PORT=8184
SCHWAB_GATEWAY_URL=http://127.0.0.1:8184
SCHWAB_STREAMING_MODE=off

# The gateway owns refresh. Do not override it with a static access token.
SCHWAB_ACCESS_TOKEN=
```

Keep `.env` and the runtime files private:

```bash
chmod 600 /home/ubuntu/spx-spark/.env
mkdir -p /srv/data/spx-spark/runtime
chmod 700 /srv/data/spx-spark/runtime
```

## 3. Cloudflare Tunnel

Add the callback hostname before the final `http_status:404` rule in
`/home/ubuntu/.cloudflared/config.yml`:

```yaml
ingress:
  - hostname: code.zh3nyu.com
    service: http://127.0.0.1:8443
  - hostname: hub.zh3nyu.com
    service: http://127.0.0.1:18124
  - hostname: schwab-auth.zh3nyu.com
    path: ^/(oauth/callback|healthz)$
    service: http://127.0.0.1:8183
  - service: http_status:404
```

Validate both the file and the route match before reloading `cloudflared`:

```bash
cloudflared --config /home/ubuntu/.cloudflared/config.yml tunnel ingress validate
cloudflared --config /home/ubuntu/.cloudflared/config.yml \
  tunnel ingress rule https://schwab-auth.zh3nyu.com/oauth/callback
```

Create the DNS route for `schwab-auth.zh3nyu.com` to the existing tunnel:

```bash
cloudflared tunnel route dns <TUNNEL_NAME_OR_UUID> schwab-auth.zh3nyu.com
```

Cloudflare evaluates ingress rules from top to bottom, so the hostname/path
rule must appear before the final catch-all. The path expression prevents any
other endpoint on that hostname from reaching the callback listener.

Create a zone-level WAF custom rule with this exact expression:

```text
http.host eq "schwab-auth.zh3nyu.com"
and http.request.uri.path eq "/oauth/callback"
```

Use the `Skip` action only for the security feature that caused the existing
browser challenge, and disable security-event logging for this skip rule when
the plan supports it. Cloudflare cannot path-skip the Free-plan Bot Fight Mode;
if that is the source of the challenge, verify the real browser redirect before
changing zone-wide protection. Do not add a cache rule for the callback and do
not export query strings to access-log drains. Keep the rest of the zone
protection unchanged.

Add a separate rate-limiting rule for the same hostname/path, for example 10
requests per minute per source IP. Do not select the rate-limiting phase in the
Skip rule. The origin callback listener is deliberately single-threaded, and
the systemd unit also caps tasks, file descriptors, and memory.

Do not route port `8184` through Cloudflare. Do not change the host's public
port `443`; the existing Tunnel reaches the loopback service without opening a
new firewall port.

Cloudflare references:

- [Tunnel ingress configuration](https://developers.cloudflare.com/tunnel/advanced/local-management/configuration-file/)
- [Tunnel DNS routing](https://developers.cloudflare.com/tunnel/routing/)
- [WAF Skip rules](https://developers.cloudflare.com/waf/custom-rules/skip/)

## 4. Install and start

After the updated repository is present on Oracle:

```bash
cd /home/ubuntu/spx-spark
uv sync --frozen
scripts/install-schwab-oauth-service.sh
```

Local checks:

```bash
curl --fail http://127.0.0.1:8183/healthz
curl --fail http://127.0.0.1:8184/livez
curl --silent --show-error http://127.0.0.1:8184/healthz
scripts/run-schwab-oauth.sh status
```

The callback health endpoint deliberately reveals no token state. The local
gateway `/livez` endpoint reports process liveness. Before the first
authorization, `/healthz` intentionally returns `503` with `ready=false`.

Only after the origin service and local checks pass, activate the edited Tunnel
configuration. The current unit has no `ExecReload`, so use a controlled
restart:

```bash
sudo systemctl restart cloudflared
sudo systemctl is-active cloudflared

cloudflared --config /home/ubuntu/.cloudflared/config.yml \
  tunnel ingress rule https://code.zh3nyu.com/
cloudflared --config /home/ubuntu/.cloudflared/config.yml \
  tunnel ingress rule https://hub.zh3nyu.com/
cloudflared --config /home/ubuntu/.cloudflared/config.yml \
  tunnel ingress rule https://schwab-auth.zh3nyu.com/oauth/callback
```

The first two URLs must still match ports `8443` and `18124`; the callback must
match `8183`. A public request to `/oauth/callback` without state should reach
the service and return a deliberate `400`, not Cloudflare `403`, Tunnel `502`,
or the catch-all `404`.

## 5. Authorize

Generate the login URL over SSH:

```bash
cd /home/ubuntu/spx-spark
scripts/run-schwab-oauth.sh authorize
```

Open the printed URL in the local browser and approve access. Schwab redirects
the browser through Cloudflare to Oracle. The server validates the server-side
state, consumes it once, exchanges the authorization code, writes the token
atomically with mode `0600`, and reloads the gateway client.

Verify without exposing the token:

```bash
scripts/run-schwab-oauth.sh status
curl --fail http://127.0.0.1:8184/healthz
scripts/run-schwab-verifier.sh --skip-chains
scripts/run-schwab-verifier.sh
```

## Offline acceptance while the app is pending

The deterministic acceptance suite does not require a Schwab token:

```bash
scripts/run-schwab-offline-acceptance.sh
```

It verifies 500-symbol quote batching, global request pacing, bounded transient
retries, immediate 401 reauthorization latching, sparse quote/option-chain
normalization, SPX/SPXW identity, provider fallback, JSONL landing, SQLite
research linkage, ZSTD Parquet compaction, schema evolution, deduplication, and
DuckDB reads. It deliberately does not claim live entitlement, payload-field,
or production-rate-limit validation; those remain the two verifier commands
above after the portal reports `Ready for Use`.

The single gateway owner applies the outbound request policy. Defaults are 120
requests per minute, three retries, exponential backoff from 0.5 to 8 seconds,
and a 30-second maximum retry wait. A longer provider `Retry-After` is returned
to the caller without retrying early. Override the `SCHWAB_HTTP_*` values only
when Schwab's active limits require it.

Provider preference is independently configurable with
`MARKET_DATA_PROVIDER_PRIORITY`. Live Schwab acceptance is complete, so the
runtime default begins with `schwab,ibkr`. Quality and freshness still outrank
preference, so a stale or missing Schwab quote falls back to a fresh IBKR
quote. Provider symbols, collection lists, and numeric request policy live in
`config/runtime.yaml` with descriptions.

## Security and failure behavior

- `/oauth/start` does not exist. Only the SSH command can create authorization
  state and print a login URL.
- Pending state expires after 15 minutes and is single-use.
- The callback server suppresses request-target logs because the query contains
  an authorization code and state.
- Token writes use a lock, temporary file, `fsync`, atomic replacement, and
  mode `0600`.
- A process-lifetime owner lock prevents a manual token helper and the gateway
  from refreshing or replacing the same token concurrently.
- The public callback listener cannot proxy Schwab data or account endpoints.
- The local gateway only allows `/marketdata/v1/quotes` and
  `/marketdata/v1/chains`; trading and account paths are rejected.
- If the token is absent or invalid, the gateway returns `503` and collectors
  degrade instead of retrying credentials in multiple processes.
- Periodic Schwab reauthorization is still required when the refresh token can
  no longer be refreshed. The same callback service handles that without a
  redeploy.

## Rollback

```bash
systemctl --user disable --now spx-spark-schwab-oauth.service
```

Then remove the dedicated Cloudflare ingress/DNS route and clear
`SCHWAB_GATEWAY_URL`. This does not touch IBKR or delete the Schwab token.

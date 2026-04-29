# Forex Rate Proxy

A local HTTP proxy for exchange rates. Internal services call this service instead of calling
One-Frame directly. It centralizes caching, resilience, and request shaping.

## Requirements (From the Brief)

1. Return an exchange rate for two supported currencies.
2. Returned rate must never be older than **5 minutes**.
3. Support **at least 10,000 successful requests/day** with an API token capped at 1,000 upstream
   calls/day.

## Assumptions

- **Single instance**: cache is in-memory and not shared across instances.
- **Full-pair cache updates**: no per-pair caching strategy; every miss/refresh fetches all pairs in a single call.
- **Single upstream**: no secondary provider fallback.
- **`maxStaleOnError` defaults to 5m**: stale serving beyond that requires explicit config increase.

## Key Decisions and Trade-offs

### In-memory cache over Redis

For single-instance local proxying, in-memory avoids extra infra, network latency, and cache-tier failure modes. The cache is abstracted via `CacheAlgebra[F]` for future Redis migration.

### Typed domain errors over exceptions

Service calls return `Either` with explicit cases:

- `OneFrameQuotaExceeded`
- `OneFrameUnreachable`
- `OneFrameLookupFailed`

This keeps failure handling explicit and compiler-checked.

### Safe currency parsing

`Currency.fromString` returns `Either` instead of throwing runtime `MatchError`.
Invalid user input maps to deterministic `400 Bad Request`.

### Same-currency shortcut

`GET /rates?from=USD&to=USD` returns `1.0` immediately without cache/upstream access.

## API

```
GET /rates?from={CURRENCY}&to={CURRENCY}
```

Supported currencies: `AUD CAD CHF EUR GBP JPY NZD SGD USD`

### Success (200)

```json
{
  "from": "USD",
  "to": "EUR",
  "price": 0.8432,
  "timestamp": "2026-04-28T05:53:56.522Z"
}
```

### Error responses

| Status | When | Body |
|--------|------|------|
| 400 | Missing or unrecognized currency | `{"error": "Unsupported currency: XYZ"}` |
| 400 | Missing query parameter | `{"error": "Both 'from' and 'to' query parameters are required"}` |
| 429 | Per-IP rate limit exceeded | `{"error": "..."}` + `Retry-After: <seconds>` header |
| 502 | One-Frame unreachable or quota exceeded | `{"error": "One-Frame is unreachable: ..."}` |
| 500 | Unexpected internal error | `{"error": "..."}` |

### Health endpoints

```
GET /health/live    # returns 200 OK when process is up
GET /health/ready   # returns 200 OK when cache is ready to serve traffic; else 503 SERVICE UNAVAILABLE
```

## Operational and Observability Features

### Request correlation (`X-Request-ID`)

Each request is tagged with `X-Request-ID`:
- If incoming header exists, it is propagated.
- If missing, a UUID is generated and returned in response headers.
This makes request tracing easier across gateway/service logs.

### Rate limiting

Per-IP request limiting is enforced (`rate-limiter.max-requests-per-minute`, default `100`):
- Over-limit requests return `429 Too Many Requests`.
- Response includes `Retry-After` header.

### Structured logging

`OneFrameLive` emits structured JSON log events for key behaviors:
- cache path decisions (`rates_lookup`, `coalesced_wait`)
- upstream lifecycle (`upstream_fetch_start`, `upstream_fetch_ok`, `upstream_fetch_error`, `upstream_retry`)
- fallback/degradation (`stale_fallback`, `quota_alert`)
This supports easier querying/alerting in centralized log systems.

### Retry + circuit breaker

For upstream connectivity failures:
- requests are retried (bounded by `one-frame.max-retries`) with exponential backoff
- repeated failures open a circuit breaker (`circuit-breaker.max-failures`)
- breaker transitions back via reset timeout (`circuit-breaker.reset-timeout`)

Only `OneFrameUnreachable` errors trigger retries and count toward the failure threshold. Quota errors (`OneFrameQuotaExceeded`) pass through immediately and do not open the circuit.

### Daily quota tracking

Each successful upstream call increments an in-process counter that resets at UTC midnight. Warning thresholds:

| Calls today | Log level | Event |
|-------------|-----------|-------|
| ≥ 800 | `WARN` | `quota_alert` with `level: "warning"` |
| ≥ 950 | `ERROR` | `quota_alert` with `level: "critical"` |

The counter is in-memory and resets on restart. If the service restarts mid-day with 900 calls already made, the counter will not reflect prior usage. Raise `max-stale-on-error` as an operational mitigation if continuity across restarts is required.

## Constraints and Resulting Design

### One-Frame returns all pairs in one call

One-Frame accepts multiple `pair` query params, e.g.

```
GET /rates?pair=USDEUR&pair=USDJPY&pair=GBPAUD&...
```

Fetching all 72 pairs costs the same request budget as fetching one pair, so each upstream call
refreshes the full cache.

### Hard 5-minute freshness ceiling

Serving data older than 5 minutes violates the requirement. If a cached value is >= 5 minutes
old, it is not served; request blocks until fresh data is fetched.

### One-Frame always returns HTTP 200

Failures are encoded in body payloads, e.g.:

```json
{"error": "Quota reached for token ..."}
```

`OneFrameHttpClient` inspects response body shape (`array` success vs `object.error` failure).
It also maps One-Frame's `time_stamp` field to internal timestamp handling.

## Cache Strategy (SWR)

The cache uses stale-while-revalidate with two TTL boundaries:

```
Age of cached rate           Behavior
---------------------------- ----------------------------------------------
0-4m (fresh)                 Serve immediately, no upstream call.
4-5m (stale-valid)           Serve immediately, refresh in background.
>=5m (expired)               Block until synchronous fresh fetch completes.
Cache miss (cold)            Block until synchronous fresh fetch completes.
```

This keeps most reads as in-memory lookups while preserving strict freshness at 5 minutes.

### Why no fixed background poller?

A periodic poller would consume quota even with zero traffic (e.g. 360/day at 4-minute cadence).
SWR refreshes only when there is demand.

### Concurrent cold-start handling

Concurrent requests during cold/expired state are coalesced via `Deferred`:

1. First request starts upstream fetch.
2. Other requests wait on the same gate.
3. All waiters resume from one completed fetch.

This prevents thundering-herd duplicate upstream calls.

## Compliance Check

### Requirement 1: valid rates for supported currencies

```bash
curl 'http://localhost:8081/rates?from=USD&to=EUR'
curl 'http://localhost:8081/rates?from=BTC&to=USD'  # HTTP 400
curl 'http://localhost:8081/rates?from=USD'         # HTTP 400
```

### Requirement 2: max 5-minute age

Hard TTL enforces expiry at exactly 5 minutes. Expired entries are never served.

### Requirement 3: 10k/day within 1k upstream/day

Worst-case under steady traffic:

```
60 min/hr / 4 min soft-TTL = 15 upstream calls/hr
15 * 24 hr = 360 upstream calls/day
```

This stays under 1,000/day, leaving headroom for cold starts/retries.

## Configuration

| Key | Default | Env var override | Description |
|-----|---------|------------------|-------------|
| `http.host` | `0.0.0.0` | — | Bind address |
| `http.port` | `8081` | — | Proxy listen port |
| `http.timeout` | `40 seconds` | — | Per-request server timeout |
| `one-frame.base-uri` | `http://localhost:8080` | `ONE_FRAME_BASE_URI` | One-Frame base URL |
| `one-frame.auth-token` | *(empty)* | `ONE_FRAME_TOKEN` | One-Frame auth token |
| `one-frame.timeout` | `10 seconds` | — | Upstream request timeout |
| `one-frame.max-retries` | `3` | — | Retry attempts for unreachable upstream |
| `circuit-breaker.max-failures` | `5` | — | Failures before opening circuit |
| `circuit-breaker.reset-timeout` | `60 seconds` | — | Half-open probe wait |
| `cache.ttl` | `5 minutes` | — | Hard freshness ceiling |
| `cache.soft-ttl` | `4 minutes` | — | SWR revalidation threshold |
| `cache.max-stale-on-error` | `5 minutes` | — | Stale serving window during outages |
| `rate-limiter.max-requests-per-minute` | `100` | — | Per-IP request cap |

## Local Run

### Prerequisite
Use Java 17:
```bash export JAVA_HOME=/opt/homebrew/opt/openjdk@17```

### Start One-Frame (8080)
```bash docker run -p 8080:8080 paidyinc/one-frame```

### Start proxy (8081)
```bash ONE_FRAME_TOKEN=10dc303535874aeccc86a8251e6992f5 sbt run```

### Quick check
```bash curl 'http://localhost:8081/rates?from=USD&to=EUR'```
```bash curl 'http://localhost:8081/rates?from=USD&to=USD'```

## Tests
```bash sbt test```

The suite runs entirely in-memory (no network, no Docker). 66 tests across 7 specs:

| Spec | What it covers |
|------|----------------|
| `CurrencySpec` | `fromString` parsing, case sensitivity, `allPairs` size and correctness |
| `RatesCacheSpec` | get/put, SWR freshness boundaries, overwrite behaviour |
| `OneFrameClientSpec` | JSON decoding, quota error detection, auth header, pair encoding, malformed base URI |
| `RatesProgramSpec` | Same-currency short-circuit, error mapping from service to program layer |
| `CircuitBreakerSpec` | Closed/Open/HalfOpen state transitions, quota errors not counted as failures, probe on reset |
| `OneFrameLiveSpec` | Retry succeeds after transient failures, quota errors not retried, WARN/ERROR quota alerts at 800/950 |
| `RatesRoutesIntegrationSpec` | Full stack — 15 end-to-end scenarios including coalescing, SWR, graceful degradation, and rate limiting |

## Extensions

### Add a currency
1. Add a `case object` in `Currency`.
2. Add it to `Currency.values`.
`fromString`, `allPairs`, and cache coverage update naturally from the enumeration.

### Swap cache backend (e.g. Redis)
Implement `CacheAlgebra[F]` in `forex/services/rates/cache/` (e.g. with [redis4cats](https://redis4cats.profunktor.dev/)) and wire it into `OneFrameLive.makeWithCache`.

### Add another upstream provider
Implement `OneFrameClientAlgebra[F]`; `OneFrameLive` already depends on this abstraction, so provider switching is mostly composition/wiring.
# Forex Rate Proxy

A local HTTP proxy for currency exchange rates. Internal services call this instead of calling
One-Frame directly — it handles caching, rate limiting, error recovery, and all the edge cases
so callers don't have to.

---

## The problem

One-Frame is the upstream provider of exchange rates. It has one hard constraint that makes
it awkward to use directly:

> **1,000 requests per day per API token.**

If your services make 10,000 requests per day (the stated requirement), hitting One-Frame on
every request burns the quota in 6 minutes. This proxy solves that by sitting in front of
One-Frame and serving most requests from an in-memory cache.

---

## Requirements (from the brief)

1. Return an exchange rate when given two supported currency codes.
2. The rate returned must never be older than **5 minutes**.
3. Support **at least 10,000 successful client requests per day** using a single API token
   (which is capped at 1,000 upstream calls per day).

---

## Constraints and what they forced

### One-Frame returns all pairs in a single request

The API accepts multiple `pair` query parameters:
```
GET /rates?pair=USDEUR&pair=USDJPY&pair=GBPAUD&...
```

This means there is no extra cost to fetching all 72 pairs (nPr = 9! / 7!) at once versus fetching one.
Every upstream call in this service fetches all 72 pairs and fills the entire cache.
There is no per-pair granularity.

### The 5-minute freshness ceiling

Serving stale data is a correctness bug. Serving data that is 4:59 old is fine; 5:01 is not.
The implementation enforces this with a hard check: if a cached entry is ≥ 5 minutes old, it
is never served — the request blocks until a fresh batch has been fetched from One-Frame.

### One-Frame always returns HTTP 200

Even on errors. Quota exhaustion looks like:
```json
{"error": "Quota reached for token ..."}
```
The client has to inspect the body to distinguish success from failure. This is handled in
`OneFrameHttpClient` — successful responses are JSON arrays; error responses are JSON objects
with an `error` field.

The One-Frame response also uses `time_stamp` (snake_case with underscore), which is worth
calling out explicitly because it differs from the camelCase convention everywhere else.

---

## Assumptions

**Single instance.** The service runs as a single process. The cache is in-memory and is not
shared across instances. This is appropriate for a "local proxy" and keeps the deployment
simple. If horizontal scaling were needed, the `CacheAlgebra` interface makes a Redis migration
straightforward.

**All 9 currencies are always cached together.** There is no logic to cache individual pairs
separately or to prioritise popular pairs. Every cache miss fetches all 72 pairs. This is
deliberate — the marginal cost of fetching 71 extra pairs is zero, and the simplicity is worth it.

**One-Frame is the only upstream source.** No fallback provider is implemented. If One-Frame
is down and the cache is exhausted, the service returns 502.

**`maxStaleOnError` defaults to 5 minutes, matching the SLA.** If One-Frame is unreachable and
the cache is older than 5 minutes, the service returns 502. If your system can tolerate slightly
stale rates over an error response during an outage, raise this to e.g. 10 minutes — the service
will then serve cached data up to that age before falling back to 502.

---

## How the cache works

The cache uses a **Stale-While-Revalidate (SWR)** policy with two TTL boundaries:

```
Age of the cached rate       What happens
──────────────────────────── ─────────────────────────────────────────────────
0 – 4 min   (fresh)          Served immediately. No upstream work.
4 – 5 min   (stale-valid)    Served immediately. Background refresh fires concurrently.
≥ 5 min     (expired)        Request waits. Fetch completes first. Then respond.
Not in cache (cold)          Request waits. Fetch completes first. Then respond.
```

The key insight: for the overwhelming majority of requests (anything in the 0–4 min window)
the response latency is just a map lookup. The 4–5 min window means clients are never blocked
by a revalidation — they get a slightly stale value while the cache updates behind them.
Only a cold start or a true expiry blocks the caller.

### Why not a background polling job?

An earlier version ran a fiber every 4 minutes to keep the cache warm. It was removed.
A polling job burns upstream quota even when the service has zero traffic — 360 calls per day
regardless. The SWR approach only refreshes when someone actually asks for a rate. Zero traffic,
zero upstream calls.

### Concurrent cold starts (the thundering herd)

When the cache is empty and 100 requests arrive simultaneously, without protection all 100
would race to call One-Frame. The implementation uses a `Deferred` gate (a one-shot promise):

1. The first request creates the gate and starts the upstream fetch.
2. Every other concurrent request finds the gate and waits.
3. When the first request finishes, all waiters unblock at once with the result already in cache.

Result: exactly **one** upstream call per burst, regardless of concurrency.

---

## Key decisions

### In-memory cache, not Redis

Adding Redis means adding infrastructure, network latency on every cache read, and a new
failure mode (the cache itself can become unavailable). For a single-instance local proxy,
in-memory is correct. The `CacheAlgebra[F]` trait is the only thing a Redis implementation
would need to satisfy.

### Typed errors, not exceptions

Every function that can fail returns an `Either` — a value that is either a typed error or
a result. The error type is an exhaustive set of cases:

- `OneFrameQuotaExceeded` — the daily limit is hit
- `OneFrameUnreachable` — network failure, timeout, or unexpected HTTP status from One-Frame
- `OneFrameLookupFailed` — One-Frame returned an error message we didn't recognise

The compiler enforces that every call site handles all cases. If a new error is added later,
every unhandled case becomes a compile error — not a runtime crash.

### `fromString` returns `Either`, not a throw

The original scaffold had `Currency.fromString` as an unsafe partial pattern match. An unknown
currency code would throw a `MatchError` at runtime. The replacement finds the currency by
looking it up in the `values` list, and returns `Left("Unsupported currency: XYZ")` if not found.
The HTTP layer turns that into a `400 Bad Request` with a JSON error body.

### Same-currency short-circuit

`GET /rates?from=USD&to=USD` is a valid request. USD always exchanges to USD at 1.0.
The program layer intercepts this case and returns immediately without touching the cache
or the upstream API.

---

## API

```
GET /rates?from={CURRENCY}&to={CURRENCY}
```

Supported currencies: `AUD CAD CHF EUR GBP JPY NZD SGD USD`

**Success (200)**
```json
{
  "from": "USD",
  "to": "EUR",
  "price": 0.8432,
  "timestamp": "2026-04-28T05:53:56.522Z"
}
```

**Error responses**

| Status | When | Body |
|--------|------|------|
| 400 | Missing or unrecognised currency | `{"error": "Unsupported currency: XYZ"}` |
| 400 | Missing query parameter | `{"error": "Both 'from' and 'to' query parameters are required"}` |
| 429 | Per-IP rate limit exceeded | `{"error": "..."}` + `Retry-After: <seconds>` header |
| 502 | One-Frame unreachable or quota exceeded | `{"error": "One-Frame is unreachable: ..."}` |
| 500 | Unexpected internal error | `{"error": "..."}` |

---

## Compliance

### Requirement 1 — return a rate for two supported currencies

```bash
curl 'http://localhost:8081/rates?from=USD&to=EUR'
# {"from":"USD","to":"EUR","price":0.8432,"timestamp":"2026-04-28T05:53:56.522Z"}
```

All 72 pairs work. Invalid input returns a descriptive error:

```bash
curl 'http://localhost:8081/rates?from=BTC&to=USD'
# HTTP 400 — {"error":"Unsupported currency: BTC"}

curl 'http://localhost:8081/rates?from=USD'
# HTTP 400 — {"error":"Both 'from' and 'to' query parameters are required"}
```

### Requirement 2 — rate not older than 5 minutes

Hard TTL is enforced at the cache layer. `CacheEntry.isExpired` returns `true` at exactly
5 minutes. An expired entry is never served — the request fetches synchronously. The test
suite includes a case that seeds a 5m30s-old entry and asserts the fresh price is returned.

### Requirement 3 — ≥10,000 client requests/day within 1,000 upstream calls

```
60 min/hr ÷ 4 min soft-TTL = 15 upstream calls/hr
15 × 24 hr                 = 360 upstream calls/day   (limit: 1,000)

Client requests served from cache between refreshes: unlimited
```

The 360 figure is the worst case under sustained load. Under zero or low traffic, the SWR
policy means fewer upstream calls. The 640-call headroom also absorbs cache misses on cold
starts and any retries.

---

## Configuration

| Key | Default | Env var override | Description |
|-----|---------|-----------------|-------------|
| `http.host` | `0.0.0.0` | — | Bind address |
| `http.port` | `8081` | — | Proxy listen port |
| `http.timeout` | `40 seconds` | — | Per-request server timeout |
| `one-frame.base-uri` | `http://localhost:8080` | `ONE_FRAME_BASE_URI` | One-Frame base URL |
| `one-frame.auth-token` | *(empty)* | `ONE_FRAME_TOKEN` | One-Frame auth token |
| `one-frame.timeout` | `10 seconds` | — | Upstream request timeout; prevents infinite wait on hung connections |
| `one-frame.max-retries` | `3` | — | Maximum retry attempts for unreachable upstream calls |
| `circuit-breaker.max-failures` | `5` | — | Consecutive unreachable failures before opening the circuit |
| `circuit-breaker.reset-timeout` | `60 seconds` | — | Wait time before half-open probe after circuit opens |
| `cache.ttl` | `5 minutes` | — | Hard freshness ceiling |
| `cache.soft-ttl` | `4 minutes` | — | SWR revalidation boundary |
| `cache.max-stale-on-error` | `5 minutes` | — | Serve expired cache on One-Frame outage; raise to e.g. 10 min to tolerate stale over 502 |
| `rate-limiter.max-requests-per-minute` | `100` | — | Per-IP request cap. Requests above the limit return `429 Too Many Requests` |

---

## Prerequisites

Use Java 17 for sbt commands in this project.

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
```

On Linux, set `JAVA_HOME` to your Java 17 installation path instead.

---

## Running locally

**1. Start One-Frame (port 8080)**
```bash
docker run -p 8080:8080 paidyinc/one-frame
```

**2. Start the proxy (port 8081)**
```bash
ONE_FRAME_TOKEN=10dc303535874aeccc86a8251e6992f5 sbt run
```

**3. Try it**
```bash
curl 'http://localhost:8081/rates?from=USD&to=EUR'
curl 'http://localhost:8081/rates?from=USD&to=USD'   # returns 1.0 instantly, no upstream call
curl 'http://localhost:8081/rates?from=XYZ&to=USD'   # 400 with error message
```

---

## Running the tests

```bash
sbt test
```

No Docker or network access required. The test suite runs entirely in-memory using mock
One-Frame backends built inline. 53 tests across 5 specs:

| Spec | What it covers |
|------|---------------|
| `CurrencySpec` | `fromString` parsing, case sensitivity, `allPairs` size and correctness |
| `RatesCacheSpec` | get/put, SWR freshness boundaries, overwrite behaviour |
| `OneFrameClientSpec` | JSON decoding, quota error detection, auth header, pair encoding, malformed base URI |
| `RatesProgramSpec` | Same-currency short-circuit, error mapping from service to program layer |
| `RatesRoutesIntegrationSpec` | Full stack — 15 end-to-end scenarios including coalescing, SWR, graceful degradation, and rate limiting |

---

## Extending

### Add a currency

1. Add a `case object` to `Currency`
2. Add it to `Currency.values`

`fromString`, `allPairs`, and the cache all update automatically.

### Swap the cache for Redis

Not needed for a single instance — in-memory is faster and has no external failure mode.
Redis becomes relevant when running multiple instances that need to share rate state, or
when cache warmth across restarts matters.

Implement `CacheAlgebra[F]` in `forex/services/rates/cache/` using a Redis client
(e.g. [redis4cats](https://redis4cats.profunktor.dev/)). Pass the new instance to
`OneFrameLive.makeWithCache`. Nothing else in the stack changes.

### Add a second upstream provider

Implement `OneFrameClientAlgebra[F]` for the new provider. The `OneFrameLive` interpreter
accepts any implementation of that algebra — switching or wrapping providers is a wiring
change, not a logic change.

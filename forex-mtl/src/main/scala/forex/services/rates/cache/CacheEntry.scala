package forex.services.rates.cache

import forex.domain.Rate

import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.FiniteDuration

/** An immutable snapshot of a fetched rate together with the wall-clock time it was
  * retrieved from One-Frame.
  *
  * Three staleness levels drive the Stale-While-Revalidate (SWR) policy:
  *
  *   isFresh           – age < softTtl  → serve immediately, no background work
  *   needsRevalidation – softTtl ≤ age < ttl → serve immediately AND refresh in background
  *   isExpired         – age ≥ ttl      → must fetch synchronously before responding
  */
final case class CacheEntry(rate: Rate, fetchedAt: OffsetDateTime) {

  private def ageMillis(now: OffsetDateTime): Long =
    ChronoUnit.MILLIS.between(fetchedAt, now)

  def isFresh(now: OffsetDateTime, softTtl: FiniteDuration): Boolean =
    ageMillis(now) < softTtl.toMillis

  def isExpired(now: OffsetDateTime, ttl: FiniteDuration): Boolean =
    ageMillis(now) >= ttl.toMillis

  /** True during the softTtl–ttl window: serve the cached value and kick off a background refresh. */
  def needsRevalidation(now: OffsetDateTime, softTtl: FiniteDuration, ttl: FiniteDuration): Boolean =
    !isFresh(now, softTtl) && !isExpired(now, ttl)
}

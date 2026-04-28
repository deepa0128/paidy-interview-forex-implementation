package forex.services.rates.cache

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.functor._
import forex.domain.Rate

import java.time.OffsetDateTime

trait CacheAlgebra[F[_]] {
  def get(pair: Rate.Pair): F[Option[CacheEntry]]
  def putBatch(rates: List[Rate], fetchedAt: OffsetDateTime): F[Unit]
  def allKeys: F[Set[Rate.Pair]]
}

/** TODO(scale): Plug in Redis when running multiple app instances.
  *   1- Add a `RedisRatesCache` implementing this `CacheAlgebra`.
  *   2- Store each pair under a stable key (for example `rates:USD:EUR`).
  *   3- Save `{rate, fetchedAt}` and enforce TTL with Redis expiry.
  *   4- Replace `InMemoryRatesCache.create` wiring in `Module`/interpreter assembly.
  *   5- Keep `putBatch` semantics atomic via `MULTI/EXEC` (or Lua) to avoid partial writes.
  */
class InMemoryRatesCache[F[_]: Sync] private (state: Ref[F, Map[Rate.Pair, CacheEntry]]) extends CacheAlgebra[F] {

  override def get(pair: Rate.Pair): F[Option[CacheEntry]] =
    state.get.map(_.get(pair))

  override def putBatch(rates: List[Rate], fetchedAt: OffsetDateTime): F[Unit] =
    state.update { existing =>
      rates.foldLeft(existing) { (acc, rate) =>
        acc.updated(rate.pair, CacheEntry(rate, fetchedAt))
      }
    }

  override def allKeys: F[Set[Rate.Pair]] =
    state.get.map(_.keySet)
}

object InMemoryRatesCache {

  def create[F[_]: Sync]: F[InMemoryRatesCache[F]] =
    Ref.of[F, Map[Rate.Pair, CacheEntry]](Map.empty).map(new InMemoryRatesCache[F](_))
}

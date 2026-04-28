package forex.services.rates.interpreters

import cats.data.NonEmptyList
import cats.effect.concurrent.{ Deferred, Ref }
import cats.effect.{ Concurrent, Resource, Timer }
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import forex.clients.oneframe.{ OneFrameClientAlgebra, OneFrameHttpClient }
import forex.config.ApplicationConfig
import forex.domain.Rate
import forex.services.rates.Algebra
import forex.services.rates.cache.{ CacheAlgebra, InMemoryRatesCache }
import forex.services.rates.errors.{ Error => ServiceError }
import org.http4s.client.Client
import org.slf4j.LoggerFactory

import java.time.OffsetDateTime

class OneFrameLive[F[_]: Concurrent: Timer] private (
    oneFrameClient: OneFrameClientAlgebra[F],
    cache: CacheAlgebra[F],
    fetchGate: Ref[F, Option[Deferred[F, Either[ServiceError, Unit]]]],
    config: ApplicationConfig
) extends Algebra[F] {

  private val logger          = LoggerFactory.getLogger(getClass)
  private val softTtl         = config.cache.softTtl
  private val ttl             = config.cache.ttl
  private val maxStaleOnError = config.cache.maxStaleOnError

  override def get(pair: Rate.Pair): F[Either[ServiceError, Rate]] =
    nowUtc.flatMap { now =>
      cache.get(pair).flatMap { cached =>
        cached match {
          case Some(entry) if entry.isFresh(now, softTtl) =>
            Concurrent[F].delay(logger.debug(s"Cache HIT for $pair")) >>
              entry.rate.asRight[ServiceError].pure[F]

          case Some(entry) if entry.needsRevalidation(now, softTtl, ttl) =>
            Concurrent[F].delay(logger.debug(s"Cache STALE for $pair, revalidating in background")) >>
              Concurrent[F]
                .start(fetchAllPairsAndPopulateCache)
                .as(entry.rate.asRight[ServiceError])

          case _ =>
            Concurrent[F].delay(logger.debug(s"Cache MISS for $pair, fetching synchronously")) >>
              fetchWithCoalescing.flatMap {
                case Right(_) =>
                  cache.get(pair).map {
                    case Some(entry) => entry.rate.asRight
                    case None =>
                      ServiceError.OneFrameLookupFailed(s"One-Frame did not return a rate for $pair").asLeft
                  }

                case Left(err) =>
                  cached match {
                    case Some(entry) if !entry.isExpired(now, maxStaleOnError) =>
                      Concurrent[F]
                        .delay(
                          logger.warn(s"One-Frame unavailable; serving stale cache for $pair")
                        )
                        .as(entry.rate.asRight[ServiceError])
                    case _ =>
                      err.asLeft[Rate].pure[F]
                  }
              }
        }
      }
    }

  private val fetchWithCoalescing: F[Either[ServiceError, Unit]] =
    Deferred[F, Either[ServiceError, Unit]].flatMap { newGate =>
      fetchGate
        .modify {
          case None               => (Some(newGate), Left(newGate))
          case Some(existingGate) => (Some(existingGate), Right(existingGate))
        }
        .flatMap {
          case Left(ourGate) =>
            fetchAllPairsAndPopulateCache
              .flatTap(result => ourGate.complete(result))
              .flatTap(_ => fetchGate.set(None))
              .handleErrorWith { err =>
                val svcError: ServiceError = ServiceError.OneFrameUnreachable(err)
                ourGate.complete(svcError.asLeft) >>
                  fetchGate.set(None) >>
                  svcError.asLeft[Unit].pure[F]
              }

          case Right(theirGate) =>
            Concurrent[F].delay(logger.debug("Awaiting in-flight One-Frame fetch")) >>
              theirGate.get
        }
    }

  private def fetchAllPairsAndPopulateCache: F[Either[ServiceError, Unit]] =
    NonEmptyList.fromList(Rate.Pair.allPairs) match {
      case None =>
        (ServiceError.OneFrameLookupFailed("No currency pairs defined"): ServiceError).asLeft[Unit].pure[F]

      case Some(pairs) =>
        Concurrent[F].delay(logger.info(s"Fetching ${pairs.size} pairs from One-Frame")) >>
          oneFrameClient.getRates(pairs).flatMap {
            case Right(rates) =>
              nowUtc.flatMap(now => cache.putBatch(rates, now)) >>
                Concurrent[F]
                  .delay(logger.info(s"Cached ${pairs.size} pairs"))
                  .as(().asRight[ServiceError])

            case Left(err) =>
              Concurrent[F]
                .delay(logger.warn(s"One-Frame fetch failed: $err"))
                .as(err.asLeft[Unit])
          }
    }

  private def nowUtc: F[OffsetDateTime] =
    Timer[F].clock.realTime(scala.concurrent.duration.MILLISECONDS).map { millis =>
      OffsetDateTime.ofInstant(
        java.time.Instant.ofEpochMilli(millis),
        java.time.ZoneOffset.UTC
      )
    }
}

object OneFrameLive {

  def resource[F[_]: Concurrent: Timer](
      httpClient: Client[F],
      config: ApplicationConfig
  ): Resource[F, Algebra[F]] =
    Resource.eval(make(OneFrameHttpClient[F](httpClient, config.oneFrame), config))

  def make[F[_]: Concurrent: Timer](
      client: OneFrameClientAlgebra[F],
      config: ApplicationConfig
  ): F[Algebra[F]] =
    for {
      cache <- InMemoryRatesCache.create[F]
      fetchGate <- Ref.of[F, Option[Deferred[F, Either[ServiceError, Unit]]]](None)
    } yield new OneFrameLive[F](client, cache, fetchGate, config)

  def makeWithCache[F[_]: Concurrent: Timer](
      client: OneFrameClientAlgebra[F],
      cache: CacheAlgebra[F],
      config: ApplicationConfig
  ): F[Algebra[F]] =
    Ref
      .of[F, Option[Deferred[F, Either[ServiceError, Unit]]]](None)
      .map(fetchGate => new OneFrameLive[F](client, cache, fetchGate, config))
}

package forex.services.rates.interpreters

import cats.data.NonEmptyList
import cats.effect.ExitCase
import cats.effect.concurrent.{ Deferred, Ref }
import cats.effect.{ Concurrent, Resource, Timer }
import cats.effect.syntax.bracket._
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import forex.clients.oneframe.{ OneFrameClientAlgebra, OneFrameHttpClient }
import forex.config.ApplicationConfig
import forex.domain.{ Currency, Rate }
import forex.logging.LogEvent
import forex.services.rates.{ Algebra, CircuitBreaker }
import forex.services.rates.cache.{ CacheAlgebra, InMemoryRatesCache }
import forex.services.rates.errors.{ Error => ServiceError }
import io.circe.Json
import org.http4s.client.Client
import org.slf4j.LoggerFactory

import java.time.OffsetDateTime
import scala.concurrent.duration.MILLISECONDS

class OneFrameLive[F[_]: Concurrent: Timer] private (
    oneFrameClient: OneFrameClientAlgebra[F],
    cache: CacheAlgebra[F],
    fetchGate: Ref[F, Option[Deferred[F, Either[ServiceError, Unit]]]],
    quotaCounter: Ref[F, (Long, Long)],
    config: ApplicationConfig
) extends Algebra[F] {

  private val logger          = LoggerFactory.getLogger(getClass)
  private val softTtl         = config.cache.softTtl
  private val ttl             = config.cache.ttl
  private val maxStaleOnError = config.cache.maxStaleOnError
  private val maxRetries      = config.oneFrame.maxRetries

  override def isReady: F[Boolean] = cache.allKeys.map(_.nonEmpty)

  override def get(pair: Rate.Pair): F[Either[ServiceError, Rate]] =
    nowUtc.flatMap { now =>
      cache.get(pair).flatMap { cached =>
        cached match {
          case Some(entry) if entry.isFresh(now, softTtl) =>
            Concurrent[F].delay(
              logger.debug(LogEvent("rates_lookup",
                "from"         -> Json.fromString(pair.from.show),
                "to"           -> Json.fromString(pair.to.show),
                "cache_status" -> Json.fromString("HIT")
              ))
            ) >> entry.rate.asRight[ServiceError].pure[F]

          case Some(entry) if entry.needsRevalidation(now, softTtl, ttl) =>
            Concurrent[F].delay(
              logger.debug(LogEvent("rates_lookup",
                "from"         -> Json.fromString(pair.from.show),
                "to"           -> Json.fromString(pair.to.show),
                "cache_status" -> Json.fromString("STALE_REVALIDATING")
              ))
            ) >> Concurrent[F].start(fetchAllPairsAndPopulateCache).as(entry.rate.asRight[ServiceError])

          case _ =>
            Concurrent[F].delay(
              logger.debug(LogEvent("rates_lookup",
                "from"         -> Json.fromString(pair.from.show),
                "to"           -> Json.fromString(pair.to.show),
                "cache_status" -> Json.fromString("MISS")
              ))
            ) >> fetchWithCoalescing.flatMap {
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
                        logger.warn(LogEvent("stale_fallback",
                          "from"  -> Json.fromString(pair.from.show),
                          "to"    -> Json.fromString(pair.to.show),
                          "error" -> Json.fromString(err.toString)
                        ))
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
              .flatTap(ourGate.complete)
              .handleErrorWith { err =>
                val svcError: ServiceError = ServiceError.OneFrameUnreachable(err)
                ourGate.complete(svcError.asLeft).as(svcError.asLeft[Unit])
              }
              .guaranteeCase {
                case ExitCase.Canceled =>
                  ourGate
                    .complete(ServiceError.OneFrameUnreachable(new java.util.concurrent.CancellationException("fetch cancelled")).asLeft)
                    .attempt
                    .void >> fetchGate.set(None)
                case _ =>
                  fetchGate.set(None)
              }

          case Right(existingGate) =>
            Concurrent[F].delay(logger.debug(LogEvent("coalesced_wait"))) >>
              existingGate.get
        }
    }

  private def fetchAllPairsAndPopulateCache: F[Either[ServiceError, Unit]] = {
    def loop(attemptsLeft: Int): F[Either[ServiceError, Unit]] =
      doOneUpstreamCall.flatMap {
        case Right(_) => ().asRight[ServiceError].pure[F]
        case Left(ServiceError.OneFrameUnreachable(_)) if attemptsLeft > 1 =>
          val attempt = maxRetries - attemptsLeft + 2
          val delayMs = math.min(200L * (1L << attempt), 5000L)
          Concurrent[F].delay(
            logger.warn(LogEvent("upstream_retry",
              "attempt"    -> Json.fromInt(maxRetries - attemptsLeft + 2),
              "delay_ms"   -> Json.fromLong(delayMs),
              "retries_left" -> Json.fromInt(attemptsLeft - 1)
            ))
          ) >> Timer[F].sleep(scala.concurrent.duration.Duration(delayMs, MILLISECONDS)) >>
            loop(attemptsLeft - 1)
        case left => left.pure[F]
      }
    loop(maxRetries + 1)
  }

  private def doOneUpstreamCall: F[Either[ServiceError, Unit]] =
    NonEmptyList.fromList(Rate.Pair.allPairs) match {
      case None =>
        (ServiceError.OneFrameLookupFailed("No currency pairs defined"): ServiceError).asLeft[Unit].pure[F]

      case Some(pairs) =>
        Timer[F].clock.realTime(MILLISECONDS).flatMap { startMs =>
          Concurrent[F].delay(
            logger.debug(LogEvent("upstream_fetch_start", "pair_count" -> Json.fromInt(pairs.size)))
          ) >>
            oneFrameClient.getRates(pairs).flatMap { result =>
              Timer[F].clock.realTime(MILLISECONDS).flatMap { endMs =>
                val durationMs = endMs - startMs
                result match {
                  case Right(rates) =>
                    trackQuota >>
                      nowUtc.flatMap(now => cache.putBatch(rates, now)) >>
                      Concurrent[F].delay(
                        logger.info(LogEvent("upstream_fetch_ok",
                          "pair_count"  -> Json.fromInt(rates.size),
                          "duration_ms" -> Json.fromLong(durationMs)
                        ))
                      ).as(().asRight[ServiceError])

                  case Left(err) =>
                    Concurrent[F].delay(
                      logger.warn(LogEvent("upstream_fetch_error",
                        "error"       -> Json.fromString(err.toString),
                        "duration_ms" -> Json.fromLong(durationMs)
                      ))
                    ).as(err.asLeft[Unit])
                }
              }
            }
        }
    }

  private def trackQuota: F[Unit] =
    nowUtc.flatMap { now =>
      val today = now.toLocalDate.toEpochDay
      quotaCounter.modify { case (day, count) =>
        val newCount = if (day == today) count + 1 else 1L
        ((today, newCount), newCount)
      }.flatMap {
        case n if n >= 950 =>
          Concurrent[F].delay(
            logger.error(LogEvent("quota_alert",
              "calls_today" -> Json.fromLong(n),
              "limit"       -> Json.fromInt(1000),
              "level"       -> Json.fromString("critical")
            ))
          )
        case n if n >= 800 =>
          Concurrent[F].delay(
            logger.warn(LogEvent("quota_alert",
              "calls_today" -> Json.fromLong(n),
              "limit"       -> Json.fromInt(1000),
              "level"       -> Json.fromString("warning")
            ))
          )
        case _ => Concurrent[F].unit
      }
    }

  private def nowUtc: F[OffsetDateTime] =
    Timer[F].clock.realTime(MILLISECONDS).map { millis =>
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
    Resource
      .eval(
        for {
          rawClient <- OneFrameHttpClient[F](httpClient, config.oneFrame)
          client    <- CircuitBreaker.wrap(rawClient, config.circuitBreaker.maxFailures, config.circuitBreaker.resetTimeout)
          service   <- make(client, config)
        } yield service
      )
      .flatTap { service =>
        Resource.eval(service.get(Rate.Pair(Currency.USD, Currency.EUR)).void)
      }

  def make[F[_]: Concurrent: Timer](
      client: OneFrameClientAlgebra[F],
      config: ApplicationConfig
  ): F[Algebra[F]] =
    for {
      cache        <- InMemoryRatesCache.create[F]
      fetchGate    <- Ref.of[F, Option[Deferred[F, Either[ServiceError, Unit]]]](None)
      quotaCounter <- Ref.of[F, (Long, Long)]((0L, 0L))
    } yield new OneFrameLive[F](client, cache, fetchGate, quotaCounter, config)

  def makeWithCache[F[_]: Concurrent: Timer](
      client: OneFrameClientAlgebra[F],
      cache: CacheAlgebra[F],
      config: ApplicationConfig
  ): F[Algebra[F]] =
    for {
      fetchGate    <- Ref.of[F, Option[Deferred[F, Either[ServiceError, Unit]]]](None)
      quotaCounter <- Ref.of[F, (Long, Long)]((0L, 0L))
    } yield new OneFrameLive[F](client, cache, fetchGate, quotaCounter, config)
}

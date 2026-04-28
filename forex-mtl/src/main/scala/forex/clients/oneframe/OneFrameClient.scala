package forex.clients.oneframe

import cats.data.NonEmptyList
import cats.effect.Sync
import cats.instances.either._
import cats.instances.list._
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.traverse._
import forex.clients.oneframe.OneFrameProtocol._
import forex.config.OneFrameConfig
import forex.domain.{ Currency, Price, Rate, Timestamp }
import forex.services.rates.errors.{ Error => ServiceError }
import io.circe.parser.decode
import org.http4s.Header
import org.http4s.Headers
import org.http4s.Method
import org.http4s.Request
import org.http4s.Uri
import org.http4s.client.Client
import org.slf4j.LoggerFactory
import org.typelevel.ci.CIString

import java.time.OffsetDateTime

/** Contract for fetching live exchange rates from an upstream provider.
  *
  * Implementations must be safe to call concurrently.
  * A single call supplies all requested pairs in one round-trip.
  */
trait OneFrameClientAlgebra[F[_]] {
  def getRates(pairs: NonEmptyList[Rate.Pair]): F[Either[ServiceError, List[Rate]]]
}

/** Live implementation that talks to the One-Frame HTTP API.
  * One-Frame quirks handled here:
  *   - Always returns HTTP 200; errors are embedded in the JSON body.
  *   - Auth via a raw `token <secret>` header.
  *   - Pair encoding: `pair=USDEUR` (concatenated currencies without separators).
  */
class OneFrameHttpClient[F[_]: Sync] private (
    client: Client[F],
    config: OneFrameConfig,
    baseUri: Uri
) extends OneFrameClientAlgebra[F] {

  private val logger = LoggerFactory.getLogger(getClass)

  override def getRates(pairs: NonEmptyList[Rate.Pair]): F[Either[ServiceError, List[Rate]]] =
    client
      .expect[String](buildRequest(pairs))
      .map(parseResponse)
      .handleErrorWith { err =>
        Sync[F]
          .delay(logger.warn(s"One-Frame HTTP call failed: ${err.getMessage}", err))
          .as(ServiceError.OneFrameUnreachable(err).asLeft)
      }

  private def buildRequest(pairs: NonEmptyList[Rate.Pair]): Request[F] = {
    val pairParams = pairs.toList
      .map(p => s"pair=${Currency.show.show(p.from)}${Currency.show.show(p.to)}")
      .mkString("&")

    val uri = Uri.unsafeFromString(s"${baseUri.renderString}/rates?$pairParams")

    Request[F](
      method = Method.GET,
      uri = uri,
      headers = Headers(Header.Raw(CIString("token"), config.authToken))
    )
  }

  private def parseResponse(body: String): Either[ServiceError, List[Rate]] =
    decode[List[OneFrameRate]](body) match {
      case Right(rates) =>
        rates
          .traverse(toRate)
          .leftMap(msg => (ServiceError.OneFrameLookupFailed(msg): ServiceError))

      case Left(_) =>
        decode[OneFrameErrorResponse](body) match {
          case Right(err) if err.error.contains("Quota reached") =>
            ServiceError.OneFrameQuotaExceeded.asLeft

          case Right(err) =>
            ServiceError.OneFrameLookupFailed(s"One-Frame error: ${err.error}").asLeft

          case Left(_) =>
            ServiceError.OneFrameLookupFailed(s"Unparseable One-Frame response: $body").asLeft
        }
    }

  private def toRate(r: OneFrameRate): Either[String, Rate] =
    for {
      from <- Currency.fromString(r.from)
      to <- Currency.fromString(r.to)
      timestamp <- parseTimestamp(r.timeStamp)
    } yield Rate(Rate.Pair(from, to), Price(r.price), Timestamp(timestamp))

  private def parseTimestamp(raw: String): Either[String, OffsetDateTime] =
    Either
      .catchNonFatal(OffsetDateTime.parse(raw))
      .leftMap(e => s"Invalid timestamp '$raw': ${e.getMessage}")
}

object OneFrameHttpClient {

  def apply[F[_]: Sync](
      client: Client[F],
      config: OneFrameConfig
  ): F[OneFrameClientAlgebra[F]] =
    Sync[F]
      .fromEither(
        Uri
          .fromString(config.baseUri)
          .leftMap(e => new IllegalArgumentException(s"Invalid One-Frame base URI '${config.baseUri}': ${e.message}"))
      )
      .map(new OneFrameHttpClient[F](client, config, _))
}

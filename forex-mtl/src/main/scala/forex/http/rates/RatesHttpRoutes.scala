package forex.http
package rates

import cats.data.Validated.{ Invalid, Valid }
import cats.effect.Sync
import cats.implicits._
import forex.programs.RatesProgram
import forex.programs.rates.errors.{ Error => ProgramError }
import forex.programs.rates.{ Protocol => RatesProgramProtocol }
import io.circe.Json
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import org.slf4j.LoggerFactory

class RatesHttpRoutes[F[_]: Sync](rates: RatesProgram[F]) extends Http4sDsl[F] {

  import Converters._, QueryParams._, Protocol._

  private val logger = LoggerFactory.getLogger(getClass)

  private[http] val prefixPath = "/rates"

  private val httpRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root :? FromQueryParam(maybeFrom) +& ToQueryParam(maybeTo) =>
      (maybeFrom, maybeTo) match {
        case (Some(validFrom), Some(validTo)) =>
          (validFrom, validTo).mapN(RatesProgramProtocol.GetRatesRequest.apply) match {
            case Valid(request) =>
              Sync[F].delay(logger.info(s"Rates lookup: ${request.from}/${request.to}")) >>
                rates.get(request).flatMap {
                  case Right(rate) =>
                    Ok(rate.asGetApiResponse)

                  case Left(ProgramError.InvalidCurrency(input)) =>
                    Sync[F].delay(logger.warn(s"Invalid currency: $input")) >>
                      BadRequest(errorBody(s"Unsupported currency: $input"))

                  case Left(ProgramError.UpstreamUnavailable(msg)) =>
                    Sync[F].delay(logger.warn(s"Upstream unavailable: $msg")) >>
                      BadGateway(errorBody(msg))

                  case Left(ProgramError.SystemError(msg)) =>
                    Sync[F].delay(logger.error(s"Internal error: $msg")) >>
                      InternalServerError(errorBody(msg))
                }

            case Invalid(parseFailures) =>
              val message = parseFailures.map(_.sanitized).toList.distinct.mkString(", ")
              Sync[F].delay(logger.warn(s"Bad request: $message")) >>
                BadRequest(errorBody(message))
          }

        case _ =>
          Sync[F].delay(logger.warn("Missing from/to parameters")) >>
            BadRequest(errorBody("Both 'from' and 'to' query parameters are required"))
      }
  }

  val routes: HttpRoutes[F] = Router(prefixPath -> httpRoutes)

  private def errorBody(message: String): Json =
    Json.obj("error" -> message.asJson)
}

package forex.http
package rates

import forex.domain.Currency.show
import forex.domain.Rate.Pair
import forex.domain._
import io.circe._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder

import java.time.format.DateTimeFormatter

object Protocol {

  implicit val configuration: Configuration = Configuration.default.withSnakeCaseMemberNames

  final case class GetApiRequest(
      from: Currency,
      to: Currency
  )

  final case class GetApiResponse(
      from: Currency,
      to: Currency,
      price: Price,
      timestamp: Timestamp
  )

  implicit val currencyEncoder: Encoder[Currency] =
    Encoder.instance[Currency](show.show _ andThen Json.fromString)

  implicit val priceEncoder: Encoder[Price] =
    Encoder.instance[Price](p => Json.fromBigDecimal(p.value))

  implicit val timestampEncoder: Encoder[Timestamp] =
    Encoder.instance[Timestamp](t => Json.fromString(t.value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)))

  implicit val pairEncoder: Encoder[Pair] =
    deriveConfiguredEncoder[Pair]

  implicit val rateEncoder: Encoder[Rate] =
    deriveConfiguredEncoder[Rate]

  implicit val responseEncoder: Encoder[GetApiResponse] =
    deriveConfiguredEncoder[GetApiResponse]

}

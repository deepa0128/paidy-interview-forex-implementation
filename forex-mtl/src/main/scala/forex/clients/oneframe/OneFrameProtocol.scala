package forex.clients.oneframe

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

/** JSON codecs for the One-Frame API wire format.
  * HTTP 200- A successful call returns a JSON array of rate objects
  * a failed call (e.g. quota exceeded) returns a JSON object with a single "error" field.
  */
object OneFrameProtocol {

  final case class OneFrameRate(
      from: String,
      to: String,
      bid: BigDecimal,
      ask: BigDecimal,
      price: BigDecimal,
      timeStamp: String
  )

  final case class OneFrameErrorResponse(error: String)

  implicit val oneFrameRateDecoder: Decoder[OneFrameRate] =
    Decoder.forProduct6("from", "to", "bid", "ask", "price", "time_stamp")(OneFrameRate.apply)
  implicit val oneFrameErrorDecoder: Decoder[OneFrameErrorResponse] = deriveDecoder
}

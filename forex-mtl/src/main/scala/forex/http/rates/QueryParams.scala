package forex.http.rates

import cats.syntax.either._
import forex.domain.Currency
import org.http4s.{ ParseFailure, QueryParamDecoder }
import org.http4s.dsl.impl.OptionalValidatingQueryParamDecoderMatcher

object QueryParams {

  private[http] implicit val currencyQueryParam: QueryParamDecoder[Currency] =
    QueryParamDecoder[String].emap { s =>
      Currency.fromString(s).leftMap(msg => ParseFailure(msg, msg))
    }

  object FromQueryParam extends OptionalValidatingQueryParamDecoderMatcher[Currency]("from")
  object ToQueryParam extends OptionalValidatingQueryParamDecoderMatcher[Currency]("to")

}

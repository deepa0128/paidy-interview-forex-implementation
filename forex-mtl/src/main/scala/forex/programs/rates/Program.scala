package forex.programs.rates

import cats.Applicative
import cats.data.EitherT
import cats.syntax.applicative._
import cats.syntax.either._
import errors._
import forex.domain._
import forex.services.RatesService

class Program[F[_]: Applicative](
    ratesService: RatesService[F]
) extends Algebra[F] {

  override def get(request: Protocol.GetRatesRequest): F[Either[Error, Rate]] = {
    val pair = Rate.Pair(request.from, request.to)
    if (request.from == request.to)
      Rate(pair, Price(BigDecimal(1)), Timestamp.now).asRight[Error].pure[F]
    else
      EitherT(ratesService.get(pair)).leftMap(toProgramError).value
  }
}

object Program {

  def apply[F[_]: Applicative](
      ratesService: RatesService[F]
  ): Algebra[F] = new Program[F](ratesService)
}

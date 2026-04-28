package forex.programs.rates

import cats.effect.IO
import forex.domain.{ Currency, Price, Rate, Timestamp }
import forex.programs.rates.errors.Error
import forex.services.rates.{ Algebra => ServiceAlgebra }
import forex.services.rates.errors.{ Error => ServiceError }
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RatesProgramSpec extends AnyWordSpec with Matchers with EitherValues {

  private val usd       = Currency.USD
  private val eur       = Currency.EUR
  private val dummyRate = Rate(Rate.Pair(usd, eur), Price(BigDecimal("0.85")), Timestamp.now)

  private def programWith(serviceResult: Either[ServiceError, Rate]): Algebra[IO] = {
    val stubService = new ServiceAlgebra[IO] {
      override def get(pair: Rate.Pair): IO[Either[ServiceError, Rate]] = IO.pure(serviceResult)
      override def isReady: IO[Boolean]                                 = IO.pure(true)
    }
    Program[IO](stubService)
  }

  "Program.get" when {

    "from == to" should {

      "return price 1.0 without calling the service" in {
        var serviceCalled = false
        val service = new ServiceAlgebra[IO] {
          override def get(pair: Rate.Pair): IO[Either[ServiceError, Rate]] = {
            serviceCalled = true
            IO.pure(Right(dummyRate))
          }
          override def isReady: IO[Boolean] = IO.pure(true)
        }
        val program = Program[IO](service)
        val result  = program.get(Protocol.GetRatesRequest(usd, usd)).unsafeRunSync()

        serviceCalled shouldBe false
        result.value.price shouldBe Price(BigDecimal(1))
      }

      "return a rate with matching from and to" in {
        val program = programWith(Right(dummyRate))
        val result  = program.get(Protocol.GetRatesRequest(eur, eur)).unsafeRunSync()

        result.value.pair.from shouldBe eur
        result.value.pair.to shouldBe eur
      }
    }

    "the service returns a rate" should {

      "pass the rate through to the caller" in {
        val program = programWith(Right(dummyRate))
        val result  = program.get(Protocol.GetRatesRequest(usd, eur)).unsafeRunSync()

        result.value shouldBe dummyRate
      }
    }

    "the service returns QuotaExceeded" should {

      "map to UpstreamUnavailable" in {
        val program = programWith(Left(ServiceError.OneFrameQuotaExceeded))
        val result  = program.get(Protocol.GetRatesRequest(usd, eur)).unsafeRunSync()

        result.left.value shouldBe a[Error.UpstreamUnavailable]
      }
    }

    "the service returns OneFrameUnreachable" should {

      "map to UpstreamUnavailable" in {
        val cause   = new RuntimeException("connection refused")
        val program = programWith(Left(ServiceError.OneFrameUnreachable(cause)))
        val result  = program.get(Protocol.GetRatesRequest(usd, eur)).unsafeRunSync()

        result.left.value shouldBe a[Error.UpstreamUnavailable]
      }
    }

    "the service returns OneFrameLookupFailed" should {

      "map to UpstreamUnavailable" in {
        val program = programWith(Left(ServiceError.OneFrameLookupFailed("timeout")))
        val result  = program.get(Protocol.GetRatesRequest(usd, eur)).unsafeRunSync()

        result.left.value shouldBe a[Error.UpstreamUnavailable]
      }
    }
  }
}

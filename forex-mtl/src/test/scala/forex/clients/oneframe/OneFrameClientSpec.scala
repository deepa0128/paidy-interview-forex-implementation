package forex.clients.oneframe

import cats.data.NonEmptyList
import cats.effect.{ IO, Resource }
import forex.config.OneFrameConfig
import forex.domain.{ Currency, Rate }
import forex.services.rates.errors.{ Error => ServiceError }
import org.http4s._
import org.http4s.client.Client
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.ci.CIString
import scala.concurrent.duration._

class OneFrameClientSpec extends AnyWordSpec with Matchers with EitherValues {

  private val config = OneFrameConfig(
    baseUri = "http://one-frame",
    authToken = "test-token",
    timeout = 10.seconds,
    maxRetries = 0
  )

  private val usdEur = Rate.Pair(Currency.USD, Currency.EUR)
  private val usdGbp = Rate.Pair(Currency.USD, Currency.GBP)

  private def clientFrom(body: String, status: Status = Status.Ok): Client[IO] =
    Client.fromHttpApp[IO](HttpApp.pure(Response[IO](status = status).withEntity(body)))

  private def makeClient(httpClient: Client[IO]): OneFrameClientAlgebra[IO] =
    OneFrameHttpClient[IO](httpClient, config).unsafeRunSync()

  private val validBody =
    """[{
      |  "from":"USD","to":"EUR",
      |  "bid":0.8532,"ask":0.8540,"price":0.8536,
      |  "time_stamp":"2024-01-15T10:30:00+00:00"
      |}]""".stripMargin

  "OneFrameHttpClient.getRates" should {

    "return a list of rates for a well-formed response" in {
      val result = makeClient(clientFrom(validBody))
        .getRates(NonEmptyList.one(usdEur))
        .unsafeRunSync()

      val rates = result.value
      rates should have size 1
      rates.head.pair shouldBe usdEur
      rates.head.price.value shouldBe BigDecimal("0.8536")
    }

    "parse the ISO-8601 timestamp" in {
      val rate = makeClient(clientFrom(validBody))
        .getRates(NonEmptyList.one(usdEur))
        .unsafeRunSync()
        .value
        .head

      rate.timestamp.value.getYear shouldBe 2024
      rate.timestamp.value.getDayOfMonth shouldBe 15
    }

    "return QuotaExceeded when One-Frame reports quota exhaustion" in {
      val quotaBody = """{"error":"Quota reached for token abc123"}"""
      val result = makeClient(clientFrom(quotaBody))
        .getRates(NonEmptyList.one(usdEur))
        .unsafeRunSync()

      result.left.value shouldBe ServiceError.OneFrameQuotaExceeded
    }

    "return OneFrameLookupFailed for other One-Frame error messages" in {
      val errorBody = """{"error":"Invalid authentication credentials"}"""
      val result = makeClient(clientFrom(errorBody))
        .getRates(NonEmptyList.one(usdEur))
        .unsafeRunSync()

      result.left.value should matchPattern { case ServiceError.OneFrameLookupFailed(_) => }
    }

    "return OneFrameUnreachable when the HTTP client raises an exception" in {
      val failingClient = Client[IO](_ => Resource.eval(IO.raiseError(new RuntimeException("connection refused"))))
      val result = makeClient(failingClient)
        .getRates(NonEmptyList.one(usdEur))
        .unsafeRunSync()

      result.left.value should matchPattern { case ServiceError.OneFrameUnreachable(_) => }
    }

    "return OneFrameLookupFailed for a non-JSON response" in {
      val result = makeClient(clientFrom("not json"))
        .getRates(NonEmptyList.one(usdEur))
        .unsafeRunSync()

      result.left.value should matchPattern { case ServiceError.OneFrameLookupFailed(_) => }
    }

    "send the auth token in the request header" in {
      var capturedToken: Option[String] = None
      val capturingApp = HttpApp[IO] { req =>
        capturedToken = req.headers.get(CIString("token")).map(_.head.value)
        IO.pure(Response[IO]().withEntity(validBody))
      }

      makeClient(Client.fromHttpApp(capturingApp))
        .getRates(NonEmptyList.one(usdEur))
        .unsafeRunSync()

      capturedToken shouldBe Some("test-token")
    }

    "encode multiple pairs as separate 'pair' query parameters" in {
      var capturedUri: Option[Uri] = None
      val capturingApp = HttpApp[IO] { req =>
        capturedUri = Some(req.uri)
        IO.pure(Response[IO]().withEntity("[]"))
      }

      makeClient(Client.fromHttpApp(capturingApp))
        .getRates(NonEmptyList.of(usdEur, usdGbp))
        .unsafeRunSync()

      val qs = capturedUri.map(_.query.renderString).getOrElse("")
      qs should include("pair=USDEUR")
      qs should include("pair=USDGBP")
    }

    "fail at construction with a clear error for a malformed base URI" in {
      val badConfig = config.copy(baseUri = "not a uri !!!")
      val result    = OneFrameHttpClient[IO](clientFrom(validBody), badConfig).attempt.unsafeRunSync()

      result.left.value shouldBe an[IllegalArgumentException]
      result.left.value.getMessage should include("not a uri !!!")
    }
  }
}

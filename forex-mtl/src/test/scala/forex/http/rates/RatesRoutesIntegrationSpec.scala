package forex.http.rates

import cats.effect.{ ContextShift, IO, Timer }
import cats.syntax.parallel._
import forex.clients.oneframe.OneFrameHttpClient
import forex.config.{ ApplicationConfig, CacheConfig, CircuitBreakerConfig, HttpConfig, OneFrameConfig, RateLimiterConfig }
import forex.domain.{ Currency, Price, Rate, Timestamp }
import forex.http.RateLimiter
import forex.programs.rates.Program
import forex.services.rates.cache.InMemoryRatesCache
import forex.services.rates.interpreters.OneFrameLive
import org.http4s._
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.implicits._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

class RatesRoutesIntegrationSpec extends AnyWordSpec with Matchers {

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  private val testConfig = ApplicationConfig(
    http = HttpConfig("0.0.0.0", 8080, 40.seconds),
    oneFrame = OneFrameConfig("http://one-frame", "test-token", 10.seconds, maxRetries = 0),
    cache = CacheConfig(
      ttl = 5.minutes,
      softTtl = 4.minutes,
      maxStaleOnError = 10.minutes
    ),
    rateLimiter = RateLimiterConfig(maxRequestsPerMinute = 100),
    circuitBreaker = CircuitBreakerConfig(maxFailures = 5, resetTimeout = 60.seconds)
  )

  private def allPairsJsonWithPrice(price: BigDecimal): String = {
    val entries = Rate.Pair.allPairs.map { pair =>
      s"""{"from":"${Currency.show.show(pair.from)}","to":"${Currency.show.show(pair.to)}",""" +
        s""""bid":${price - 0.01},"ask":${price + 0.01},"price":$price,"time_stamp":"2024-06-01T12:00:00+00:00"}"""
    }
    s"[${entries.mkString(",")}]"
  }

  private def allPairsJson: String = allPairsJsonWithPrice(BigDecimal("0.85"))

  private def routesUsing(oneFrameApp: HttpApp[IO]): HttpRoutes[IO] = {
    val httpClient = Client.fromHttpApp[IO](oneFrameApp)
    val oneFrameClient = OneFrameHttpClient[IO](httpClient, testConfig.oneFrame).unsafeRunSync()
    val interpreter = OneFrameLive.make[IO](oneFrameClient, testConfig).unsafeRunSync()
    val program = Program[IO](interpreter)
    new RatesHttpRoutes[IO](program).routes
  }

  private def routesWithSeededCache(
      oneFrameApp: HttpApp[IO],
      seedRates: List[Rate],
      fetchedAt: OffsetDateTime
  ): IO[HttpRoutes[IO]] =
    for {
      cache <- InMemoryRatesCache.create[IO]
      _ <- cache.putBatch(seedRates, fetchedAt)
      httpClient = Client.fromHttpApp[IO](oneFrameApp)
      oneFrameClient <- OneFrameHttpClient[IO](httpClient, testConfig.oneFrame)
      interpreter <- OneFrameLive.makeWithCache[IO](oneFrameClient, cache, testConfig)
    } yield new RatesHttpRoutes[IO](Program[IO](interpreter)).routes

  private def allRatesWith(price: BigDecimal): List[Rate] =
    Rate.Pair.allPairs.map(p => Rate(p, Price(price), Timestamp.now))

  private def requestIO(routes: HttpRoutes[IO], uri: Uri): IO[Response[IO]] =
    routes.run(Request[IO](Method.GET, uri)).getOrElse(Response.notFound)

  "GET /rates" when {

    "both currencies are valid and One-Frame responds successfully" should {

      "return 200 with from, to, price, and timestamp" in {
        val routes   = routesUsing(HttpApp.pure(Response[IO]().withEntity(allPairsJson)))
        val response = requestIO(routes, uri"/rates?from=USD&to=EUR").unsafeRunSync()

        response.status shouldBe Status.Ok

        val body = response.as[io.circe.Json].unsafeRunSync()
        body.hcursor.get[String]("from").toOption shouldBe Some("USD")
        body.hcursor.get[String]("to").toOption shouldBe Some("EUR")
        body.hcursor.get[BigDecimal]("price").toOption shouldBe Some(BigDecimal("0.85"))
      }
    }

    "from == to" should {

      "return 200 with price 1.0 without calling One-Frame" in {
        var oneFrameCalled = false
        val capturingApp = HttpApp[IO] { _ =>
          IO { oneFrameCalled = true } *>
            IO.pure(Response[IO]().withEntity(allPairsJson))
        }

        val response = requestIO(routesUsing(capturingApp), uri"/rates?from=USD&to=USD").unsafeRunSync()

        response.status shouldBe Status.Ok
        oneFrameCalled shouldBe false

        val body = response.as[io.circe.Json].unsafeRunSync()
        body.hcursor.get[BigDecimal]("price").toOption shouldBe Some(BigDecimal(1))
      }
    }

    "an unrecognised currency code is given" should {

      "return 400 mentioning the bad code" in {
        val routes   = routesUsing(HttpApp.pure(Response[IO]().withEntity(allPairsJson)))
        val response = requestIO(routes, uri"/rates?from=XYZ&to=USD").unsafeRunSync()

        response.status shouldBe Status.BadRequest

        val message = response.as[io.circe.Json].unsafeRunSync().hcursor.get[String]("error").getOrElse("")
        message should include("XYZ")
      }
    }

    "the 'from' parameter is missing" should {

      "return 400" in {
        val routes   = routesUsing(HttpApp.pure(Response[IO]().withEntity(allPairsJson)))
        val response = requestIO(routes, uri"/rates?to=EUR").unsafeRunSync()
        response.status shouldBe Status.BadRequest
      }
    }

    "the 'to' parameter is missing" should {

      "return 400" in {
        val routes   = routesUsing(HttpApp.pure(Response[IO]().withEntity(allPairsJson)))
        val response = requestIO(routes, uri"/rates?from=USD").unsafeRunSync()
        response.status shouldBe Status.BadRequest
      }
    }

    "One-Frame reports quota exhaustion" should {

      "return 502 Bad Gateway" in {
        val quotaBody = """{"error":"Quota reached for token test-token"}"""
        val response = requestIO(
          routesUsing(HttpApp.pure(Response[IO]().withEntity(quotaBody))),
          uri"/rates?from=USD&to=EUR"
        ).unsafeRunSync()

        response.status shouldBe Status.BadGateway
      }
    }

    "One-Frame returns an HTTP 500" should {

      "return 502 Bad Gateway" in {
        val errorApp = HttpApp.pure[IO](Response[IO](status = Status.InternalServerError))
        val response = requestIO(routesUsing(errorApp), uri"/rates?from=USD&to=EUR").unsafeRunSync()
        response.status shouldBe Status.BadGateway
      }
    }

    "ten concurrent requests arrive with a cold cache" should {

      "make exactly one upstream call (Deferred coalescing)" in {
        val callCount = new AtomicInteger(0)
        val countingApp = HttpApp[IO] { _ =>
          IO(callCount.incrementAndGet()) *>
            IO.pure(Response[IO]().withEntity(allPairsJson))
        }

        val routes = routesUsing(countingApp)

        List
          .fill(10)(requestIO(routes, uri"/rates?from=USD&to=EUR"))
          .parSequence
          .unsafeRunSync()

        callCount.get() shouldBe 1
      }
    }

    "a second request arrives after the cache is warm" should {

      "serve the cached rate without a second One-Frame call" in {
        val callCount = new AtomicInteger(0)
        val countingApp = HttpApp[IO] { _ =>
          IO(callCount.incrementAndGet()) *>
            IO.pure(Response[IO]().withEntity(allPairsJson))
        }

        val routes = routesUsing(countingApp)

        requestIO(routes, uri"/rates?from=USD&to=EUR").unsafeRunSync()
        requestIO(routes, uri"/rates?from=USD&to=EUR").unsafeRunSync()

        callCount.get() shouldBe 1
      }
    }

    "the cached rate is between softTtl and hardTtl (stale-while-revalidate)" should {

      "serve the stale rate immediately and trigger a background revalidation" in {
        val stalePrice = BigDecimal("0.777")
        val freshPrice = BigDecimal("0.999")
        val callCount  = new AtomicInteger(0)
        val countingApp = HttpApp[IO] { _ =>
          IO(callCount.incrementAndGet()) *>
            IO.pure(Response[IO]().withEntity(allPairsJsonWithPrice(freshPrice)))
        }

        val staleTime = OffsetDateTime.now().minusSeconds(270)

        val (status, body) = (for {
          routes <- routesWithSeededCache(countingApp, allRatesWith(stalePrice), staleTime)
          response <- requestIO(routes, uri"/rates?from=USD&to=EUR")
          body <- response.as[io.circe.Json]
          _ <- IO.sleep(300.millis)
        } yield (response.status, body)).unsafeRunSync()

        status shouldBe Status.Ok
        body.hcursor.get[BigDecimal]("price").toOption shouldBe Some(stalePrice)
        callCount.get() shouldBe 1
      }
    }

    "the cached rate has expired past hardTtl and One-Frame responds" should {

      "re-fetch synchronously and return the fresh rate" in {
        val oldPrice   = BigDecimal("0.111")
        val freshPrice = BigDecimal("0.999")
        val freshApp   = HttpApp.pure[IO](Response[IO]().withEntity(allPairsJsonWithPrice(freshPrice)))

        val expiredTime = OffsetDateTime.now().minusSeconds(330)

        val (status, body) = (for {
          routes <- routesWithSeededCache(freshApp, allRatesWith(oldPrice), expiredTime)
          response <- requestIO(routes, uri"/rates?from=USD&to=EUR")
          body <- response.as[io.circe.Json]
        } yield (response.status, body)).unsafeRunSync()

        status shouldBe Status.Ok
        body.hcursor.get[BigDecimal]("price").toOption shouldBe Some(freshPrice)
      }
    }

    "One-Frame is unreachable and the cache is within maxStaleOnError (configured to 10 min)" should {

      "serve the stale rate rather than returning an error" in {
        val stalePrice = BigDecimal("0.888")
        val failingApp = HttpApp[IO](_ => IO.raiseError(new RuntimeException("connection refused")))

        val expiredTime = OffsetDateTime.now().minusSeconds(360)

        val (status, price) = (for {
          routes <- routesWithSeededCache(failingApp, allRatesWith(stalePrice), expiredTime)
          response <- requestIO(routes, uri"/rates?from=USD&to=EUR")
          body <- response.as[io.circe.Json]
        } yield (response.status, body.hcursor.get[BigDecimal]("price").toOption)).unsafeRunSync()

        status shouldBe Status.Ok
        price shouldBe Some(stalePrice)
      }
    }

    "One-Frame is unreachable and the cache exceeds maxStaleOnError" should {

      "return 502 Bad Gateway" in {
        val failingApp = HttpApp[IO](_ => IO.raiseError(new RuntimeException("connection refused")))

        val tooOldTime = OffsetDateTime.now().minusMinutes(15)

        val status = (for {
          routes <- routesWithSeededCache(failingApp, allRatesWith(BigDecimal("0.5")), tooOldTime)
          response <- requestIO(routes, uri"/rates?from=USD&to=EUR")
        } yield response.status).unsafeRunSync()

        status shouldBe Status.BadGateway
      }
    }

    "One-Frame is unreachable and the cache is empty" should {

      "return 502 Bad Gateway" in {
        val failingApp = HttpApp[IO](_ => IO.raiseError(new RuntimeException("connection refused")))
        val response   = requestIO(routesUsing(failingApp), uri"/rates?from=USD&to=EUR").unsafeRunSync()
        response.status shouldBe Status.BadGateway
      }
    }

    "more requests than the per-minute limit arrive from the same IP" should {

      "return 429 Too Many Requests once the limit is exceeded" in {
        val limit = 3
        val routes = RateLimiter
          .middleware[IO](limit)
          .map { rl =>
            rl(routesUsing(HttpApp.pure(Response[IO]().withEntity(allPairsJson))))
          }
          .unsafeRunSync()

        val responses = List.fill(limit + 1) {
          requestIO(routes, uri"/rates?from=USD&to=EUR").unsafeRunSync()
        }

        responses.take(limit).foreach(_.status shouldBe Status.Ok)
        responses.last.status shouldBe Status.TooManyRequests
      }
    }
  }
}

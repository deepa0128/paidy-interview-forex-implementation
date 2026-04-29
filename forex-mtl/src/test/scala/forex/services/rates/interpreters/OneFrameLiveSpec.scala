package forex.services.rates.interpreters

import cats.effect.{ ContextShift, IO, Timer }
import cats.effect.concurrent.Ref
import cats.syntax.either._
import ch.qos.logback.classic.{ Level, Logger => LbLogger }
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import forex.clients.oneframe.OneFrameClientAlgebra
import forex.config.{ ApplicationConfig, CacheConfig, CircuitBreakerConfig, HttpConfig, OneFrameConfig, RateLimiterConfig }
import forex.domain.{ Currency, Price, Rate, Timestamp }
import forex.services.rates.cache.InMemoryRatesCache
import forex.services.rates.errors.{ Error => ServiceError }
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory

import java.time.LocalDate
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class OneFrameLiveSpec extends AnyWordSpec with Matchers with EitherValues {

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  private val usdEur = Rate.Pair(Currency.USD, Currency.EUR)

  private val baseConfig = ApplicationConfig(
    http         = HttpConfig("0.0.0.0", 8080, 40.seconds),
    oneFrame     = OneFrameConfig("http://one-frame", "test-token", 10.seconds, maxRetries = 0),
    cache        = CacheConfig(ttl = 5.minutes, softTtl = 4.minutes, maxStaleOnError = 10.minutes),
    rateLimiter  = RateLimiterConfig(maxRequestsPerMinute = 100),
    circuitBreaker = CircuitBreakerConfig(maxFailures = 5, resetTimeout = 60.seconds)
  )

  private def allRates: List[Rate] =
    Rate.Pair.allPairs.map(p => Rate(p, Price(BigDecimal("0.85")), Timestamp.now))

  private def makeService(client: OneFrameClientAlgebra[IO], config: ApplicationConfig) =
    OneFrameLive.make[IO](client, config).unsafeRunSync()

  "OneFrameLive retry" should {

    "retry on OneFrameUnreachable and return success after the third attempt" in {
      val attempts = new AtomicInteger(0)
      val client: OneFrameClientAlgebra[IO] = _ => IO {
        val n = attempts.incrementAndGet()
        if (n <= 2) ServiceError.OneFrameUnreachable(new RuntimeException(s"fail $n")).asLeft
        else allRates.asRight
      }
      val config  = baseConfig.copy(oneFrame = baseConfig.oneFrame.copy(maxRetries = 2))
      val service = makeService(client, config)

      val result = service.get(usdEur).unsafeRunSync()

      result.isRight shouldBe true
      attempts.get() shouldBe 3
    }

    "not retry on quota exhaustion" in {
      val attempts = new AtomicInteger(0)
      val client: OneFrameClientAlgebra[IO] = _ => IO {
        attempts.incrementAndGet()
        ServiceError.OneFrameQuotaExceeded.asLeft[List[Rate]]
      }
      val config  = baseConfig.copy(oneFrame = baseConfig.oneFrame.copy(maxRetries = 3))
      val service = makeService(client, config)

      service.get(usdEur).unsafeRunSync()

      attempts.get() shouldBe 1
    }

    "return the final error after all retries are exhausted" in {
      val client: OneFrameClientAlgebra[IO] = _ => IO.pure(
        ServiceError.OneFrameUnreachable(new RuntimeException("always fails")).asLeft
      )
      val config  = baseConfig.copy(oneFrame = baseConfig.oneFrame.copy(maxRetries = 1))
      val service = makeService(client, config)

      val result = service.get(usdEur).unsafeRunSync()

      result.left.value shouldBe a[ServiceError.OneFrameUnreachable]
    }
  }

  "OneFrameLive quota tracking" should {

    def withLogCapture(f: => Unit): List[ILoggingEvent] = {
      val appender = new ListAppender[ILoggingEvent]()
      appender.start()
      val lbLogger = LoggerFactory
        .getLogger(classOf[OneFrameLive[IO]])
        .asInstanceOf[LbLogger]
      lbLogger.addAppender(appender)
      try f
      finally { val _ = lbLogger.detachAppender(appender) }
      appender.list.asScala.toList
    }

    "emit a WARN quota_alert when daily calls reach 800" in {
      val todayEpoch = LocalDate.now(java.time.ZoneOffset.UTC).toEpochDay
      val counter    = Ref.of[IO, (Long, Long)]((todayEpoch, 799L)).unsafeRunSync()
      val cache      = InMemoryRatesCache.create[IO].unsafeRunSync()
      val client: OneFrameClientAlgebra[IO] = _ => IO.pure(allRates.asRight)

      val service = OneFrameLive.makeWithCacheAndQuota[IO](client, cache, counter, baseConfig).unsafeRunSync()

      val events = withLogCapture {
        val _ = service.get(usdEur).unsafeRunSync()
      }

      val alert = events.find(e => e.getMessage.contains("quota_alert") && e.getMessage.contains("warning"))
      alert shouldBe defined
      alert.get.getLevel shouldBe Level.WARN
    }

    "emit an ERROR quota_alert when daily calls reach 950" in {
      val todayEpoch = LocalDate.now(java.time.ZoneOffset.UTC).toEpochDay
      val counter    = Ref.of[IO, (Long, Long)]((todayEpoch, 949L)).unsafeRunSync()
      val cache      = InMemoryRatesCache.create[IO].unsafeRunSync()
      val client: OneFrameClientAlgebra[IO] = _ => IO.pure(allRates.asRight)

      val service = OneFrameLive.makeWithCacheAndQuota[IO](client, cache, counter, baseConfig).unsafeRunSync()

      val events = withLogCapture {
        val _ = service.get(usdEur).unsafeRunSync()
      }

      val alert = events.find(e => e.getMessage.contains("quota_alert") && e.getMessage.contains("critical"))
      alert shouldBe defined
      alert.get.getLevel shouldBe Level.ERROR
    }

    "not emit a quota_alert below the warning threshold" in {
      val todayEpoch = LocalDate.now(java.time.ZoneOffset.UTC).toEpochDay
      val counter    = Ref.of[IO, (Long, Long)]((todayEpoch, 500L)).unsafeRunSync()
      val cache      = InMemoryRatesCache.create[IO].unsafeRunSync()
      val client: OneFrameClientAlgebra[IO] = _ => IO.pure(allRates.asRight)

      val service = OneFrameLive.makeWithCacheAndQuota[IO](client, cache, counter, baseConfig).unsafeRunSync()

      val events = withLogCapture {
        val _ = service.get(usdEur).unsafeRunSync()
      }

      events.exists(_.getMessage.contains("quota_alert")) shouldBe false
    }
  }
}

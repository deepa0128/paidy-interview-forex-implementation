package forex.services.rates

import cats.effect.{ ContextShift, IO, Timer }
import cats.effect.concurrent.Ref
import cats.syntax.either._
import cats.syntax.flatMap._
import forex.clients.oneframe.OneFrameClientAlgebra
import forex.domain.{ Currency, Price, Rate, Timestamp }
import forex.services.rates.errors.{ Error => ServiceError }
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

class CircuitBreakerSpec extends AnyWordSpec with Matchers with EitherValues {

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  private val pair      = Rate.Pair(Currency.USD, Currency.EUR)
  private val nel       = cats.data.NonEmptyList.one(pair)
  private val dummyRate = Rate(pair, Price(BigDecimal("0.85")), Timestamp.now)
  private val ok        = List(dummyRate).asRight[ServiceError]
  private val unreachable = ServiceError.OneFrameUnreachable(new RuntimeException("network error")).asLeft[List[Rate]]
  private val quota     = ServiceError.OneFrameQuotaExceeded.asLeft[List[Rate]]

  private def fixedClient(result: Either[ServiceError, List[Rate]]): OneFrameClientAlgebra[IO] =
    _ => IO.pure(result)

  private def countingClient(
      result: Ref[IO, Either[ServiceError, List[Rate]]],
      calls: AtomicInteger
  ): OneFrameClientAlgebra[IO] =
    _ => IO(calls.incrementAndGet()) >> result.get

  private def makeCB(
      underlying: OneFrameClientAlgebra[IO],
      maxFailures: Int = 2,
      resetTimeout: FiniteDuration = 100.millis
  ): IO[OneFrameClientAlgebra[IO]] =
    CircuitBreaker.wrap[IO](underlying, maxFailures, resetTimeout)

  "CircuitBreaker in Closed state" should {

    "forward successful calls through to the underlying client" in {
      val cb = makeCB(fixedClient(ok)).unsafeRunSync()
      cb.getRates(nel).unsafeRunSync() shouldBe ok
    }

    "stay Closed when failure count is below maxFailures" in {
      val calls = new AtomicInteger(0)
      val result = Ref.of[IO, Either[ServiceError, List[Rate]]](unreachable).unsafeRunSync()
      val cb = makeCB(countingClient(result, calls), maxFailures = 3).unsafeRunSync()

      cb.getRates(nel).unsafeRunSync()  // failure 1
      cb.getRates(nel).unsafeRunSync()  // failure 2 — still Closed at maxFailures-1

      calls.get() shouldBe 2
    }

    "open after exactly maxFailures consecutive unreachable errors" in {
      val calls = new AtomicInteger(0)
      val result = Ref.of[IO, Either[ServiceError, List[Rate]]](unreachable).unsafeRunSync()
      val cb = makeCB(countingClient(result, calls), maxFailures = 2).unsafeRunSync()

      cb.getRates(nel).unsafeRunSync()  // failure 1
      cb.getRates(nel).unsafeRunSync()  // failure 2 → Open
      cb.getRates(nel).unsafeRunSync()  // rejected by Open CB

      calls.get() shouldBe 2  // 3rd call never reached upstream
    }

    "not count quota errors as circuit-opening failures" in {
      val calls = new AtomicInteger(0)
      val result = Ref.of[IO, Either[ServiceError, List[Rate]]](quota).unsafeRunSync()
      val cb = makeCB(countingClient(result, calls), maxFailures = 2).unsafeRunSync()

      cb.getRates(nel).unsafeRunSync()  // quota error 1 — should not count
      cb.getRates(nel).unsafeRunSync()  // quota error 2 — CB should still be Closed

      calls.get() shouldBe 2  // both calls reached upstream (not rejected)
    }
  }

  "CircuitBreaker in Open state" should {

    "reject calls immediately without reaching the underlying client" in {
      val calls = new AtomicInteger(0)
      val result = Ref.of[IO, Either[ServiceError, List[Rate]]](unreachable).unsafeRunSync()
      val cb = makeCB(countingClient(result, calls), maxFailures = 1).unsafeRunSync()

      cb.getRates(nel).unsafeRunSync()  // failure → Open
      val rejected = cb.getRates(nel).unsafeRunSync()  // rejected

      calls.get() shouldBe 1
      rejected.left.value shouldBe a[ServiceError.OneFrameUnreachable]
    }

    "probe the underlying client after resetTimeout and close on success" in {
      val calls  = new AtomicInteger(0)
      val result = Ref.of[IO, Either[ServiceError, List[Rate]]](unreachable).unsafeRunSync()
      val cb     = makeCB(countingClient(result, calls), maxFailures = 2, resetTimeout = 50.millis).unsafeRunSync()

      cb.getRates(nel).unsafeRunSync()  // failure 1
      cb.getRates(nel).unsafeRunSync()  // failure 2 → Open

      result.set(ok).unsafeRunSync()
      IO.sleep(120.millis).unsafeRunSync()  // past resetTimeout

      cb.getRates(nel).unsafeRunSync() shouldBe ok  // probe → Closed
      calls.get() shouldBe 3

      cb.getRates(nel).unsafeRunSync() shouldBe ok  // Closed: passes through normally
      calls.get() shouldBe 4
    }

    "reopen if the probe call fails" in {
      val calls  = new AtomicInteger(0)
      val result = Ref.of[IO, Either[ServiceError, List[Rate]]](unreachable).unsafeRunSync()
      val cb     = makeCB(countingClient(result, calls), maxFailures = 2, resetTimeout = 50.millis).unsafeRunSync()

      cb.getRates(nel).unsafeRunSync()  // failure 1
      cb.getRates(nel).unsafeRunSync()  // failure 2 → Open

      IO.sleep(120.millis).unsafeRunSync()

      cb.getRates(nel).unsafeRunSync()  // probe → still fails → reOpen
      calls.get() shouldBe 3

      cb.getRates(nel).unsafeRunSync()  // rejected again (Open)
      calls.get() shouldBe 3  // 4th call never reached upstream
    }
  }
}

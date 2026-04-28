package forex.services.rates

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{ Concurrent, Timer }
import cats.syntax.applicative._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import forex.clients.oneframe.OneFrameClientAlgebra
import forex.domain.Rate
import forex.services.rates.errors.{ Error => ServiceError }

import scala.concurrent.duration.{ FiniteDuration, MILLISECONDS }

private sealed trait CBState
private object CBState {
  final case class Closed(failures: Int) extends CBState
  final case class Open(openedAt: Long)  extends CBState
  case object HalfOpen                   extends CBState
}

class CircuitBreaker[F[_]: Concurrent: Timer] private (
    underlying: OneFrameClientAlgebra[F],
    state: Ref[F, CBState],
    maxFailures: Int,
    resetTimeout: FiniteDuration
) extends OneFrameClientAlgebra[F] {

  override def getRates(pairs: NonEmptyList[Rate.Pair]): F[Either[ServiceError, List[Rate]]] =
    now.flatMap { ts =>
      state.get.flatMap {
        case CBState.Open(openedAt) if ts - openedAt >= resetTimeout.toMillis =>
          state.set(CBState.HalfOpen) >> probe(pairs)
        case CBState.Open(_) =>
          (ServiceError.OneFrameUnreachable(new RuntimeException("circuit breaker open")): ServiceError).asLeft[List[Rate]].pure[F]
        case CBState.HalfOpen =>
          probe(pairs)
        case CBState.Closed(_) =>
          call(pairs)
      }
    }

  private def call(pairs: NonEmptyList[Rate.Pair]): F[Either[ServiceError, List[Rate]]] =
    underlying.getRates(pairs).flatTap {
      case Right(_) =>
        state.update { case CBState.Closed(_) => CBState.Closed(0); case s => s }
      case Left(ServiceError.OneFrameUnreachable(_)) =>
        now.flatMap { ts =>
          state.modify {
            case CBState.Closed(n) if n + 1 >= maxFailures => (CBState.Open(ts), ())
            case CBState.Closed(n)                         => (CBState.Closed(n + 1), ())
            case s                                         => (s, ())
          }
        }
      case _ => Concurrent[F].unit
    }

  private def probe(pairs: NonEmptyList[Rate.Pair]): F[Either[ServiceError, List[Rate]]] =
    underlying.getRates(pairs).flatTap {
      case Right(_)                                  => state.set(CBState.Closed(0))
      case Left(ServiceError.OneFrameUnreachable(_)) => now.flatMap(ts => state.set(CBState.Open(ts)))
      case _                                         => Concurrent[F].unit
    }

  private def now: F[Long] = Timer[F].clock.realTime(MILLISECONDS)
}

object CircuitBreaker {
  def wrap[F[_]: Concurrent: Timer](
      underlying: OneFrameClientAlgebra[F],
      maxFailures: Int,
      resetTimeout: FiniteDuration
  ): F[OneFrameClientAlgebra[F]] =
    Ref
      .of[F, CBState](CBState.Closed(0))
      .map(new CircuitBreaker(underlying, _, maxFailures, resetTimeout))
}

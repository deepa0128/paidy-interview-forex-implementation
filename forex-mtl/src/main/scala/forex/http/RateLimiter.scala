package forex.http

import cats.data.OptionT
import cats.effect.concurrent.Ref
import cats.effect.{ Concurrent, Timer }
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.http4s.{ Header, HttpRoutes, Response, Status }
import org.typelevel.ci.CIString

import scala.concurrent.duration.MILLISECONDS

object RateLimiter {

  def middleware[F[_]: Concurrent: Timer](
      maxRequestsPerMinute: Int
  ): F[HttpRoutes[F] => HttpRoutes[F]] =
    Ref.of[F, Map[String, (Int, Long)]](Map.empty).map { counter => routes =>
      HttpRoutes[F] { req =>
        val ip = req.remoteAddr.map(_.toString).getOrElse("unknown")
        OptionT(
          for {
            now    <- Timer[F].clock.realTime(MILLISECONDS)
            window  = now / 60000L
            allowed <- counter.modify { map =>
              // Evict entries from previous windows on every request, bounding map size
              // to the number of unique IPs active within the current minute.
              val current = map.filter { case (_, (_, b)) => b == window }
              val count   = current.get(ip).map(_._1).getOrElse(0)
              if (count >= maxRequestsPerMinute)
                (current, false)
              else
                (current + (ip -> (count + 1, window)), true)
            }
            resp <- if (allowed) routes.run(req).value
                    else {
                      val secondsUntilReset = ((window + 1) * 60000L - now) / 1000L
                      Some(
                        Response[F](Status.TooManyRequests)
                          .putHeaders(Header.Raw(CIString("Retry-After"), secondsUntilReset.toString))
                      ).pure[F]
                    }
          } yield resp
        )
      }
    }
}

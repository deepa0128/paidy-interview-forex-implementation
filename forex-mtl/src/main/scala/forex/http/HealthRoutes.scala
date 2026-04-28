package forex.http

import cats.effect.Sync
import cats.syntax.flatMap._
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl

class HealthRoutes[F[_]: Sync](isReady: F[Boolean]) extends Http4sDsl[F] {

  val routes: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "health" / "live"  => Ok("OK")
    case GET -> Root / "health" / "ready" =>
      isReady.flatMap {
        case true  => Ok("OK")
        case false => ServiceUnavailable("cache cold")
      }
  }
}

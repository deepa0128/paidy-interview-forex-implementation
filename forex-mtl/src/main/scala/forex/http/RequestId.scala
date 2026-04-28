package forex.http

import cats.data.OptionT
import cats.effect.Sync
import cats.syntax.functor._
import org.http4s.{ Header, HttpRoutes }
import org.typelevel.ci.CIString

import java.util.UUID

object RequestId {

  private val headerName = CIString("X-Request-ID")

  def middleware[F[_]: Sync](routes: HttpRoutes[F]): HttpRoutes[F] =
    HttpRoutes[F] { req =>
      val id = req.headers.get(headerName).map(_.head.value).getOrElse(UUID.randomUUID().toString)
      val taggedReq = req.putHeaders(Header.Raw(headerName, id))
      OptionT(
        routes.run(taggedReq).value
          .map(_.map(_.putHeaders(Header.Raw(headerName, id))))
      )
    }
}

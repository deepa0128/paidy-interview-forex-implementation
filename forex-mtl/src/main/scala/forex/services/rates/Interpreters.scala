package forex.services.rates

import cats.Applicative
import cats.effect.{ Concurrent, Resource, Timer }
import forex.config.ApplicationConfig
import interpreters._
import org.http4s.client.Client

object Interpreters {
  def dummy[F[_]: Applicative]: Algebra[F] = new OneFrameDummy[F]()

  def live[F[_]: Concurrent: Timer](
      httpClient: Client[F],
      config: ApplicationConfig
  ): Resource[F, Algebra[F]] =
    OneFrameLive.resource[F](httpClient, config)
}
